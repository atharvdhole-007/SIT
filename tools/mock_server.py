"""Mock FeedSentinel server for dashboard development.

Implements the HTTP + WebSocket contract in docs/API.md with synthetic but plausible data,
so the dashboard can be built and demoed without the real engine.

    .venv/Scripts/python.exe tools/mock_server.py --port 8001
    options: --no-models     send ml/diagnosis as null (no trained models loaded)
             --empty-metrics GET /api/metrics returns {} (evaluation not run yet)
"""
from __future__ import annotations

import argparse
import asyncio
import contextlib
import hashlib
import json
import math
import secrets
import random
import statistics
import time
from collections import deque
from datetime import datetime, timedelta, timezone
from pathlib import Path

import uvicorn
from fastapi import Body, FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles

ROOT = Path(__file__).resolve().parent.parent
DASH = ROOT / "dashboard"
OPTS = argparse.Namespace(no_models=False, empty_metrics=False)

try:  # titles/actions come from the canonical schema when it imports cleanly
    import sys
    sys.path.insert(0, str(ROOT))
    from feedsentinel.schema import CODE_INFO
except Exception:  # pragma: no cover - standalone fallback
    CODE_INFO = {}


def tz_of(name: str):
    try:
        from zoneinfo import ZoneInfo
        return ZoneInfo(name)
    except Exception:
        return timezone(timedelta(hours=-4)) if name == "America/New_York" else timezone.utc


# id, label, group, description, accepts_symbols, code, severity, ttd (market seconds)
SCENARIOS = [
    ("packet_loss", "Packet loss 5%", "fault", "Drops 5% of messages (UDP loss)", False, "GAP", "DEGRADED", 0.6),
    ("delay", "Delay 800 ms", "fault", "Adds 800 ms of latency to every message", False, "DELAY", "DEGRADED", 0.9),
    ("duplicates", "Duplicates", "fault", "Re-sends 10% of messages with the same sequence number", False, "DUPLICATE", "DEGRADED", 0.5),
    ("garbled", "Garbled messages", "fault", "Corrupts 2% of messages (crossed book, negative size)", False, "GARBLED", "DEGRADED", 0.4),
    ("frozen", "Frozen feed", "fault", "Handler stuck: heartbeats and sequence numbers stay perfect, prices stop moving", True, "FROZEN", "CRITICAL", 1.4),
    ("stale", "Stale feed", "fault", "Heartbeats keep arriving but no data messages", False, "STALE", "CRITICAL", 1.1),
    ("disconnect", "Disconnect", "fault", "Feed goes silent: no data, no heartbeats", False, "DISCONNECT", "CRITICAL", 1.0),
    ("decimal_shift", "Decimal shift x10", "fault", "Price(4) scale bug: prices arrive 10x too large", True, "DECIMAL_SHIFT", "CRITICAL", 0.3),
    ("price_spike", "Price spikes", "fault", "Occasional prints 3% away from the market", True, "PRICE_SPIKE", "DEGRADED", 0.7),
    ("drift", "Slow drift", "fault", "Prices creep upward by 0.03% per second", True, "DRIFT", "DEGRADED", 4.5),
    ("clock_skew", "Clock skew +2 s", "fault", "Exchange timestamps run 2 s in the future", False, "CLOCK_SKEW", "DEGRADED", 0.8),
    ("inc_test_data", "Test data in production", "incident", "Replay of a test-data leak: placeholder prices such as $123.47 reach production", True, "TEST_DATA_LEAK", "CRITICAL", 0.2),
    ("inc_sip_outage", "Consolidated-tape outage", "incident", "Replay of a tape outage: the feed stops completely for minutes", False, "DISCONNECT", "CRITICAL", 1.0),
    ("inc_reconnect_storm", "Reconnect storm", "incident", "Feed handler stuck in a reconnect loop, re-sending snapshots", False, "RATE_STORM", "CRITICAL", 0.5),
    ("novel_bitflip", "Bit-flip corruption", "novel", "Random single-bit flips in price fields - never seen in training", True, "ML_ANOMALY", "DEGRADED", 1.8),
    ("novel_jitter", "Latency jitter bursts", "novel", "Bursty 0-400 ms jitter - never seen in training", False, "ML_ANOMALY", "DEGRADED", 2.2),
    ("market_halt", "Trading halt (LULD)", "market", "A real volatility halt: every feed stops quoting the symbol together", True, None, None, None),
    ("market_fast_move", "Fast market move", "market", "A legitimate 1.5% move seen identically on every feed", True, None, None, None),
    ("market_open_burst", "Opening burst", "market", "Message-rate burst at the open, on all feeds", False, None, None, None),
]
SCN = {s[0]: dict(id=s[0], label=s[1], group=s[2], description=s[3], accepts_symbols=s[4],
                  code=s[5], severity=s[6], ttd=s[7]) for s in SCENARIOS}
SEV_RANK = {"HEALTHY": 0, "DEGRADED": 1, "CRITICAL": 2}
TL_ICONS = {"ok": "\U0001F7E2", "warn": "\U0001F7E1", "minor": "\U0001F7E0", "major": "\U0001F534", "incident": "\U0001F6A8",
            "action": "\U0001F504", "info": "\U0001F535"}
CAUSES = {"GAP": ("Packet loss", "Network latency / path"), "DELAY": ("Congested network path", "Feed-handler overload"),
          "FROZEN": ("Feed handler stuck", "Upstream publisher stalled"), "STALE": ("Upstream subscription lost", "Feed handler stuck"),
          "DISCONNECT": ("Session dropped", "Network outage"), "DECIMAL_SHIFT": ("Price-scale bug in decoder", "Reference-data mismatch"),
          "TEST_DATA_LEAK": ("Test data published to production", "Misrouted test session"),
          "RATE_STORM": ("Reconnect loop re-sending snapshots", "Duplicate subscription"),
          "ML_ANOMALY": ("Unknown corruption (novel pattern)", "Bit-level decoder fault")}
COMPANY = {"AAPL": "Apple Inc.", "AMZN": "Amazon.com, Inc.", "GOOG": "Google Inc.", "INTC": "Intel Corporation",
           "MSFT": "Microsoft Corporation", "BTC-USD": "Bitcoin / US dollar", "ETH-USD": "Ether / US dollar", "SOL-USD": "Solana / US dollar"}
USERS = {
    "ops_us": {"username": "ops_us", "display_name": "Olivia Park", "role": "ops_analyst", "regions": ["US"], "home": "/ops"},
    "trader_us": {"username": "trader_us", "display_name": "Tom Reyes", "role": "trader", "regions": ["US"], "home": "/trader"},
    "trader_eu": {"username": "trader_eu", "display_name": "Eva Lindqvist", "role": "trader", "regions": ["EU"], "home": "/trader"},
    "admin": {"username": "admin", "display_name": "Admin", "role": "admin", "regions": ["US", "EU", "GLOBAL"], "home": "/ops"},
}
TOKENS: dict = {}   # access token -> username
REFRESH: dict = {}  # refresh token -> username
AUDIT: list = []


def audit(actor, action, target="", outcome="ok", detail=""):
    prev = AUDIT[-1]["hash"] if AUDIT else "0" * 64
    e = {"seq": len(AUDIT) + 1, "t_ms": int(time.time() * 1000), "actor": actor, "action": action,
         "target": target, "outcome": outcome, "detail": detail}
    e["hash"] = hashlib.sha256((prev + json.dumps(e, sort_keys=True)).encode()).hexdigest()
    AUDIT.append(e)

MODES = {
    "replay": dict(
        tz="America/New_York", source="Nasdaq TotalView-ITCH via LOBSTER · 21 Jun 2012",
        speeds=[1, 2, 5, 10, 20], speed=5, start_ms=1340289600000,  # 10:40:00 EDT (14:40 UTC)
        feeds=[("A", "Direct", "Nasdaq direct feed (fastest)", 412, 3.1, 6.4),
               ("B", "SIP", "Consolidated tape (slower, authoritative)", 409, 21.0, 41.0),
               ("C", "Vendor", "Third-party vendor feed (Chaos Lab target)", 398, 6.1, 14.8)],
        symbols={"AAPL": 585.31, "AMZN": 221.40, "GOOG": 565.20, "INTC": 26.62, "MSFT": 30.71}),
    "live": dict(
        tz="UTC", source="Live crypto venues · Kraken, Gemini, Bitstamp public WebSockets",
        speeds=[1], speed=1, start_ms=None,
        feeds=[("K", "Kraken", "Kraken public WebSocket", 38, 45.0, 120.0),
               ("G", "Gemini", "Gemini market-data v2", 31, 60.0, 150.0),
               ("S", "Bitstamp", "Bitstamp live trades & order book", 24, 70.0, 180.0)],
        symbols={"BTC-USD": 64250.0, "ETH-USD": 3121.5, "SOL-USD": 145.32}),
}

FEATURES = {
    "FROZEN": [("frozen_frac", 1.0, 0.0, 12.5), ("px_change_rate", 0.0, 3.4, -6.1), ("peer_divergence", 0.8, 0.05, 5.2)],
    "GAP": [("gap_rate", 0.05, 0.0, 9.8), ("msg_rate_ratio", 0.95, 1.0, -2.1), ("seq_jump_max", 7.0, 1.0, 6.3)],
    "DELAY": [("lat_p99_ms", 815.0, 14.8, 11.2), ("lat_p50_ms", 806.0, 6.1, 10.7), ("asof_lag_ms", 790.0, 5.0, 9.1)],
    "DECIMAL_SHIFT": [("px_ratio_to_consensus", 10.0, 1.0, 40.0), ("off_tick_frac", 0.0, 0.0, 0.0), ("jump_size_bp", 90000.0, 1.2, 35.0)],
}
DEFAULT_FEATURES = [("msg_rate_ratio", 1.8, 1.0, 4.2), ("px_change_rate", 6.1, 3.4, 3.1), ("lat_p99_ms", 29.0, 14.8, 2.4)]


class Feed:
    def __init__(self, fid, name, role, rate, p50, p99):
        self.id, self.name, self.role = fid, name, role
        self.base_rate, self.p50, self.p99 = rate, p50, p99
        self.health = 97.5
        self.history: deque = deque(maxlen=120)
        self.gaps = self.dups = self.quarantined = 0
        self.ml_score = 0.3
        self.trust = 1.0


class Sim:
    def __init__(self):
        self.hub: set[asyncio.Queue] = set()
        self.demo_task: asyncio.Task | None = None
        self.reset("replay")

    # ------------------------------------------------------------------ setup
    def reset(self, mode: str):
        if self.demo_task:
            self.demo_task.cancel()
        cfg = MODES[mode]
        self.mode, self.tz, self.cfg = mode, cfg["tz"], cfg
        self.tzinfo = tz_of(cfg["tz"])
        self.speed, self.paused = cfg["speed"], False
        self.t_ms = cfg["start_ms"] or int(time.time() * 1000) // 250 * 250
        self.feeds = [Feed(*f) for f in cfg["feeds"]]
        self.fmap = {f.id: f for f in self.feeds}
        self.symbols = list(cfg["symbols"])
        self.symbol = self.symbols[0]
        self.px = dict(cfg["symbols"])
        self.truth = {s: deque([p] * 12, maxlen=12) for s, p in self.px.items()}
        self.buf = {s: deque(maxlen=240) for s in self.symbols}
        self.last = {s: {f.id: p for f in self.feeds} for s, p in self.px.items()}
        self.faults: list[dict] = []
        self.incidents: list[dict] = []
        self.markers: deque = deque(maxlen=40)
        self.stopwatch = None
        self.fault_no = self.inc_no = 0
        self.started, self.processed, self.steps = time.time(), 0, 0
        self.timeline: deque = deque(maxlen=60)
        self.tl_no = 0
        for _ in range(240):  # pre-fill so the chart has history at once
            self.step(prefill=True)
        self.add_tl("info", f"{mode.title()} started: {cfg['source']}")
        self.add_tl("ok", f"All {len(self.feeds)} feeds healthy")

    def add_tl(self, level, text, feed=None, incident_id=None, collapse=None):
        last = self.timeline[-1] if self.timeline else None
        if collapse and last and last.get("_key") == collapse:
            last["count"] += 1
            last["text"] = f"{collapse} x{last['count']}"
            last["t_ms"], last["clock"] = self.t_ms, self.clock(self.t_ms)
            return
        self.tl_no += 1
        fd = self.fmap.get(feed) if feed else None
        self.timeline.append({"id": self.tl_no, "t_ms": self.t_ms, "clock": self.clock(self.t_ms), "feed": feed,
                              "feed_name": fd.name if fd else None, "level": level, "icon": TL_ICONS.get(level, ""),
                              "text": text, "count": 1, "incident_id": incident_id, "_key": collapse})

    def hello(self) -> dict:
        return {
            "type": "hello", "mode": self.mode, "product": "FeedSentinel", "source": self.cfg["source"],
            "tz": self.tz, "speed": self.speed, "speeds": self.cfg["speeds"],
            "feeds": [{"id": f.id, "name": f.name, "role": f.role} for f in self.feeds],
            "symbols": self.symbols,
            "scenarios": [{k: s[k] for k in ("id", "label", "group", "description", "accepts_symbols")}
                          for s in SCN.values()],
            "models": {"anomaly": not OPTS.no_models, "classifier": not OPTS.no_models},
            "llm": {"enabled": False, "provider": "template"},
        }

    def clock(self, t_ms: int) -> str:
        return datetime.fromtimestamp(t_ms / 1000, self.tzinfo).strftime("%H:%M:%S")

    # ------------------------------------------------------------------ simulation
    def feed_faults(self, fid, sym=None):
        return [f for f in self.faults if f["feed"] == fid and (sym is None or not f["symbols"] or sym in f["symbols"])]

    def step(self, prefill=False):
        self.t_ms += 250
        self.steps += 1
        market = [f for f in self.faults if f["group"] == "market"]
        for s in self.symbols:
            p = self.px[s]
            vol = 0.00013
            for f in market:
                if f["symbols"] and s not in f["symbols"]:
                    continue
                if f["scenario"] == "market_fast_move" and f["age_s"] < 6:
                    p *= 1.0006  # ~1.5% over 6 s
                if f["scenario"] == "market_halt":
                    vol = 0.0
            p *= math.exp(random.gauss(0, vol))
            self.px[s] = round(p, 2)
            self.truth[s].append(self.px[s])
        row_t = self.t_ms
        for s in self.symbols:
            tr = self.truth[s]
            vals = {}
            for i, fd in enumerate(self.feeds):
                v = tr[-1 - min(i, 2)]  # A now, B one sample behind, C two behind
                if fd is self.feeds[-1] and random.random() < 0.3:
                    v = tr[-2]
                for f in self.feed_faults(fd.id, s):
                    sc, age = f["scenario"], f["age_s"]
                    if sc == "frozen":
                        v = f.setdefault("frozen_px", {}).setdefault(s, self.last[s][fd.id])
                    elif sc in ("stale", "disconnect", "inc_sip_outage"):
                        v = None
                    elif sc == "packet_loss" and random.random() < 0.05:
                        v = None
                    elif sc == "delay":
                        v = tr[max(0, len(tr) - 4)]
                    elif sc == "decimal_shift" and v is not None:
                        v = round(v * 10, 2)
                    elif sc == "price_spike" and v is not None and random.random() < 0.08:
                        v = round(v * 1.03, 2)
                    elif sc == "drift" and v is not None:
                        v = round(v * (1 + 0.0003 * age), 2)
                    elif sc == "inc_test_data":
                        v = 123.47
                    elif sc == "novel_bitflip" and v is not None and random.random() < 0.1:
                        v = round(v * random.choice((0.996, 1.004)), 2)
                if v is not None:
                    self.last[s][fd.id] = v
                vals[fd.id] = v
            trusted = [vals[f.id] for f in self.feeds if vals[f.id] is not None and self.state(f) != "CRITICAL"]
            cons = round(statistics.median(trusted), 2) if trusted else None
            self.buf[s].append((row_t, cons, vals))
        if not prefill:
            self.advance_faults()
            self.update_health()
            self.processed += random.randint(9000, 10500)

    def advance_faults(self):
        for f in self.faults:
            f["age_s"] = round((self.t_ms - f["started_ms"]) / 1000, 2)
            sc = SCN[f["scenario"]]
            fd = self.fmap.get(f["feed"])
            if fd:
                if f["scenario"] == "packet_loss" and random.random() < 0.3:
                    fd.gaps += 1
                    self.add_tl("minor", "Sequence gap detected", fd.id, collapse=f"Sequence gap on {fd.id}")
                if f["scenario"] in ("duplicates", "inc_reconnect_storm") and random.random() < 0.5:
                    fd.dups += random.randint(1, 4)
                if f["scenario"] in ("garbled", "novel_bitflip", "inc_test_data") and random.random() < 0.4:
                    fd.quarantined += 1
            if sc["code"] and not f.get("incident_id") and f["age_s"] >= sc["ttd"]:
                self.open_incident(f, sc)
        sw = self.stopwatch
        if sw and not sw["detected"] and any(f["id"] == sw["fault_id"] for f in self.faults):
            sw["elapsed_s"] = round((self.t_ms - sw["started_ms"]) / 1000, 1)
            if sw["group"] == "market":
                sw["false_alerts"] = sum(1 for i in self.incidents if i["opened_ms"] >= sw["started_ms"])
        for inc in self.incidents:
            if inc["status"] == "OPEN":
                inc["last_ms"] = self.t_ms
                inc["duration_s"] = round((self.t_ms - inc["opened_ms"]) / 1000, 1)

    def open_incident(self, f, sc):
        fd = self.fmap.get(f["feed"])
        if fd is None:
            return
        self.inc_no += 1
        best = next((x for x in self.feeds if x.id != fd.id and not self.feed_faults(x.id)), self.feeds[0])
        code = sc["code"]
        info = CODE_INFO.get(code, {})
        ph = {"best_feed": f"{best.id} ({best.name})", "gap_first": 1204, "gap_last": 1210, "resume_seq": 88213}
        action = info.get("action", "Investigate the feed").format_map(ph)
        syms = f["symbols"] or list(self.symbols)
        age = max(f["age_s"], sc["ttd"])
        headline = {
            "FROZEN": f"{len(syms)} symbols flat for {age:.1f} s while peers moved {random.randint(9, 30)} times",
            "GAP": f"{fd.gaps + 3} sequence gaps in {age:.1f} s",
            "DELAY": "p99 latency 815 ms vs 15 ms normal",
            "DECIMAL_SHIFT": f"prices 10.0x consensus on {len(syms)} symbols",
            "TEST_DATA_LEAK": "test price $123.47 printed on production symbols",
            "ML_ANOMALY": "feed behaviour outside the learned normal range",
        }.get(code, f"{sc['label']} detected on {fd.name}")
        now = self.t_ms
        inc = {
            "id": f"INC-{self.inc_no:04d}", "feed": fd.id, "feed_name": fd.name, "code": code,
            "title": info.get("title", code.replace("_", " ").title()), "headline": headline,
            "severity": sc["severity"], "status": "OPEN",
            "opened_ms": now, "opened_clock": self.clock(now), "last_ms": now, "closed_ms": None,
            "duration_s": 0.0, "ttd_s": round(age, 1), "action": action, "symbols": syms,
            "explanation_source": "template", "feedback": None,
            # detail-only fields
            "evidence": {"scenario_age_s": round(age, 1), "symbols_affected": len(syms), "peer_feeds_agree": 2,
                         "heartbeat_age_s": 0.4, "msg_rate": round(fd.base_rate * random.uniform(.9, 1.0), 1)},
            "samples": [{"feed": fd.id, "session": "000042", "seq": 88200 + i, "type": "Q", "sym": syms[i % len(syms)],
                         "bid": self.last[syms[i % len(syms)]][fd.id], "bid_sz": 100 * (i + 1),
                         "ask": round(self.last[syms[i % len(syms)]][fd.id] + 0.02, 2), "ask_sz": 200,
                         "exch_ts": now * 1_000_000 + i, "recv_ts": now * 1_000_000 + 6_100_000 + i} for i in range(4)],
            "top_features": [{"name": n, "value": v, "baseline": b, "z": z} for n, v, b, z in FEATURES.get(code, DEFAULT_FEATURES)],
            "diagnosis": None if OPTS.no_models else (
                {"label": "GARBLED", "p": 0.41, "alternatives": [{"label": "PRICE_SPIKE", "p": 0.33}, {"label": "NORMAL", "p": 0.12}]}
                if code == "ML_ANOMALY" else
                {"label": code, "p": 0.93, "alternatives": [{"label": "STALE", "p": 0.04}, {"label": "NORMAL", "p": 0.02}]}),
            "explanation": (f"{fd.name} ({fd.id}) shows {info.get('title', code).lower()} on {len(syms)} symbol(s).\n"
                            f"Evidence: {headline}; peers {', '.join(x.id for x in self.feeds if x is not fd)} kept updating normally.\n"
                            f"Consumers should switch to {best.id} ({best.name}) until the feed recovers."),
            "timeline": [{"t_ms": f["started_ms"], "clock": self.clock(f["started_ms"]), "event": f"fault injected: {sc['label']}"},
                         {"t_ms": now, "clock": self.clock(now), "event": f"opened {sc['severity']}"}],
            "fault": {"scenario": f["scenario"], "label": sc["label"], "started_ms": f["started_ms"]},
            "acked_by": None, "acked_ms": None,
        }
        prim, sec = CAUSES.get(code, (sc["label"], "Unknown / novel behaviour"))
        others = [x for x in self.feeds if x is not fd]
        rca = {"incident_id": inc["id"], "primary": {"cause": prim, "confidence": random.randint(72, 91)},
               "secondary": {"cause": sec, "confidence": random.randint(25, 45)},
               "evidence": [headline, f"Detected {age:.1f} s after onset", f"Other feeds ({', '.join(x.id for x in others)}) unaffected"],
               "scope": f"Other feeds unaffected: feed-path issue on {fd.id} ({fd.name})", "affected": syms,
               "action": f"Failover to Feed {best.id} ({best.name})"}
        rca["text"] = "\n".join(["ROOT CAUSE ANALYSIS", f"Primary: {prim} ({rca['primary']['confidence']}%)",
                                 f"Secondary: {sec} ({rca['secondary']['confidence']}%)", "Evidence:",
                                 *[f"- {e}" for e in rca["evidence"]], f"Scope: {rca['scope']}",
                                 f"Likely affected: {', '.join(syms)}", f"Recommended action: {rca['action']}"])
        inc["rca"] = rca
        self.add_tl("incident", f"{inc['id']} opened: {inc['title']} ({sc['severity']})", fd.id, inc["id"])
        self.incidents.insert(0, inc)
        f["incident_id"] = inc["id"]
        self.markers.append({"t_ms": now, "feed": fd.id, "code": code, "severity": sc["severity"]})
        sw = self.stopwatch
        if sw and sw.get("fault_id") == f["id"]:
            sw.update(detected=True, ttd_s=inc["ttd_s"], incident_id=inc["id"], elapsed_s=inc["ttd_s"])

    def state(self, fd: Feed) -> str:
        sev = [SCN[f["scenario"]]["severity"] for f in self.feed_faults(fd.id) if f.get("incident_id")]
        if sev:
            return max(sev, key=SEV_RANK.get)
        return "HEALTHY" if fd.health >= 80 else "DEGRADED"

    def update_health(self):
        for fd in self.feeds:
            active = self.feed_faults(fd.id)
            detected = [SCN[f["scenario"]]["severity"] for f in active if f.get("incident_id")]
            target = 97.5 + random.gauss(0, 0.5)
            if "CRITICAL" in detected:
                target = 28 + random.gauss(0, 2)
            elif detected:
                target = 63 + random.gauss(0, 2)
            fd.health = max(0.0, min(100.0, fd.health + (target - fd.health) * 0.3))
            fd.trust = max(0.05, min(1.0, fd.trust + ((fd.health / 100) ** 2 - fd.trust) * 0.3))
            faulty = [f for f in active if f["group"] != "market"]
            ml_target = (0.78 if faulty else 0.3) + random.gauss(0, 0.03)
            fd.ml_score += (ml_target - fd.ml_score) * 0.25
            if self.steps % 4 == 0:
                fd.history.append(round(fd.health, 1))
        for inc in self.incidents:
            if inc["status"] == "OPEN" and not any(f.get("incident_id") == inc["id"] for f in self.faults):
                inc.update(status="RESOLVED", closed_ms=self.t_ms)
                self.add_tl("ok", f"{inc['id']} resolved: {inc['feed_name']} back to normal", inc["feed"], inc["id"])
                inc["timeline"].append({"t_ms": self.t_ms, "clock": self.clock(self.t_ms), "event": "resolved: fault cleared"})

    # ------------------------------------------------------------------ payloads
    def feed_payload(self, fd: Feed) -> dict:
        active = [f for f in self.feed_faults(fd.id) if f.get("incident_id")]
        active.sort(key=lambda f: -SEV_RANK[SCN[f["scenario"]]["severity"]])
        codes = [SCN[f["scenario"]]["code"] for f in active]
        incs = {i["id"]: i for i in self.incidents}
        headline = ""
        if active:
            inc = incs.get(active[0]["incident_id"])
            headline = f"{codes[0]}: {inc['headline']}" if inc else codes[0]
        rate = fd.base_rate * random.uniform(0.96, 1.04)
        p50, p99 = fd.p50 * random.uniform(.9, 1.1), fd.p99 * random.uniform(.9, 1.15)
        for f in self.feed_faults(fd.id):
            sc = f["scenario"]
            if sc in ("stale", "disconnect", "inc_sip_outage"):
                rate = 0.0
            elif sc == "inc_reconnect_storm":
                rate *= 8
            elif sc == "packet_loss":
                rate *= 0.95
            elif sc == "delay":
                p50, p99 = p50 + 800, p99 + 800
            elif sc == "novel_jitter":
                p99 += random.uniform(100, 400)
        diag = None
        if not OPTS.no_models:
            label = codes[0] if codes else "NORMAL"
            if label == "ML_ANOMALY":
                label = "GARBLED"
            diag = {"label": label, "p": round(random.uniform(0.86, 0.97) if label != "GARBLED" or not codes else 0.41, 2)}
        score = round(fd.ml_score, 3)
        return {
            "id": fd.id, "name": fd.name, "health": round(fd.health, 1), "state": self.state(fd),
            "trust": round(fd.trust, 2), "msg_rate": round(rate, 1),
            "lat_p50_ms": round(p50, 1), "lat_p99_ms": round(p99, 1),
            "gaps": fd.gaps, "dups": fd.dups, "quarantined": fd.quarantined,
            "headline": headline, "codes": codes,
            "ml": None if OPTS.no_models else {"score": score, "threshold": 0.58, "flag": score > 0.58},
            "diagnosis": diag, "history": list(fd.history),
        }

    def tick(self) -> dict:
        t = datetime.fromtimestamp(self.t_ms / 1000, self.tzinfo)
        hm = t.hour * 60 + t.minute
        session = "OPEN" if self.mode == "live" or 570 <= hm < 960 else ("PRE" if hm < 570 else "CLOSED")
        buf = list(self.buf[self.symbol])
        t0 = buf[0][0] if buf else 0
        feed_ids = [f.id for f in self.feeds]
        chart = {
            "symbol": self.symbol, "t_ms": [r[0] for r in buf],
            "series": {"consensus": [r[1] for r in buf], **{fid: [r[2].get(fid) for r in buf] for fid in feed_ids}},
            "markers": [m for m in self.markers if m["t_ms"] >= t0],
        }
        recommended = {}
        for s in self.symbols:
            ok = [f for f in self.feeds if self.state(f) == "HEALTHY" and not self.feed_faults(f.id, s)]
            recommended[s] = (ok or sorted(self.feeds, key=lambda f: -f.health))[0].id
        opened = [i for i in self.incidents if i["status"] == "OPEN"]
        closed = [i for i in self.incidents if i["status"] != "OPEN"]
        return {
            "type": "tick", "t_ms": self.t_ms, "clock": t.strftime("%H:%M:%S"), "date": t.strftime("%Y-%m-%d"),
            "session": session, "mode": self.mode, "speed": self.speed, "paused": self.paused,
            "feeds": [self.feed_payload(f) for f in self.feeds],
            "recommended": recommended, "chart": chart,
            "incidents": [summary(i) for i in (opened + closed)[:30]],
            "chaos": {"active": [{k: f[k] for k in ("id", "scenario", "label", "group", "feed", "symbols", "started_ms", "age_s")}
                                 for f in self.faults],
                      "stopwatch": ({k: v for k, v in self.stopwatch.items() if k != "fault_id"} if self.stopwatch else None)},
            "self": {"events_per_s": round(random.uniform(36500, 40200)), "lag_ms": round(random.uniform(2.5, 6.0), 1),
                     "processed": self.processed, "uptime_s": round(time.time() - self.started, 1)},
            "timeline": [{k: v for k, v in e.items() if k != "_key"} for e in self.timeline],
        }

    def trader_tick(self, user: dict) -> dict:
        t = datetime.fromtimestamp(self.t_ms / 1000, self.tzinfo)
        out = {"type": "trader_tick", "t_ms": self.t_ms, "clock": t.strftime("%H:%M:%S"), "date": t.strftime("%Y-%m-%d"),
               "session": "OPEN", "tz": self.tz, "watchlist": [], "alerts": []}
        region = "US" if self.mode == "replay" else "GLOBAL"
        if region not in user["regions"] and self.mode == "replay":
            out["notice"] = f"No feeds in your region ({', '.join(user['regions'])}) are streaming right now."
            return out
        opened = [i for i in self.incidents if i["status"] == "OPEN"]
        for s in self.symbols:
            row = self.buf[s][-1] if self.buf[s] else None
            ok = [f for f in self.feeds if self.state(f) == "HEALTHY" and not self.feed_faults(f.id, s)]
            src = ok[0] if ok else sorted(self.feeds, key=lambda f: -f.health)[0]
            bad = [i for i in opened if s in (i["symbols"] or [])]
            if not ok:
                trust, why = "DO NOT USE", "No healthy feed agrees on this price"
            elif bad:
                trust, why = "USE CAUTION", f"{bad[0]['feed_name']} feed unreliable; using {src.name} feed"
            else:
                trust, why = "VERIFIED", f"{len(ok)} feeds agree"
            out["watchlist"].append({"symbol": s, "name": COMPANY.get(s, s), "price": row[1] if row else None, "trust": trust,
                                     "trust_reason": why, "source": src.id, "source_name": src.name,
                                     "updated_s": round(random.uniform(0.1, 0.7), 1)})
        for i in self.incidents[:8]:
            best = next((f for f in self.feeds if f.id != i["feed"]), self.feeds[0])
            if i["status"] == "OPEN":
                lvl = "critical" if i["severity"] == "CRITICAL" else "warn"
                text = f"{', '.join(i['symbols'][:5])} prices from {i['feed_name']} feed unreliable since {i['opened_clock']}; using {best.name} feed"
            else:
                lvl, text = "ok", f"{i['feed_name']} feed back to normal at {self.clock(i['closed_ms'] or self.t_ms)}"
            out["alerts"].append({"t_ms": i["opened_ms"], "clock": i["opened_clock"], "level": lvl, "text": text})
        return out

    def broadcast(self, msg: dict):
        for q in list(self.hub):
            if q.full():
                with contextlib.suppress(asyncio.QueueEmpty):
                    q.get_nowait()
            q.put_nowait(msg)

    # ------------------------------------------------------------------ chaos
    def inject(self, scenario: str, feed: str, symbols, params=None, duration_s=None) -> dict:
        sc = SCN[scenario]
        self.fault_no += 1
        f = {"id": f"F{self.fault_no}", "scenario": scenario, "label": sc["label"], "group": sc["group"],
             "feed": feed, "symbols": symbols if sc["accepts_symbols"] else None,
             "started_ms": self.t_ms, "age_s": 0.0, "params": params or {}, "duration_s": duration_s}
        self.faults.append(f)
        self.stopwatch = {"scenario": scenario, "label": sc["label"], "feed": feed, "group": sc["group"],
                          "started_ms": self.t_ms, "elapsed_s": 0.0, "detected": False, "ttd_s": None,
                          "incident_id": None, "false_alerts": 0, "fault_id": f["id"]}
        return {k: f[k] for k in ("id", "scenario", "label", "group", "feed", "symbols", "started_ms", "age_s")}

    def clear(self, fid=None) -> int:
        before = len(self.faults)
        self.faults = [f for f in self.faults if fid is not None and f["id"] != fid]
        return before - len(self.faults)


SUMMARY_KEYS = ("id", "feed", "feed_name", "code", "title", "headline", "severity", "status", "opened_ms",
                "opened_clock", "last_ms", "closed_ms", "duration_s", "ttd_s", "action", "symbols",
                "explanation_source", "feedback", "acked_by", "acked_ms")


def summary(inc: dict) -> dict:
    return {k: inc.get(k) for k in SUMMARY_KEYS}


sim = Sim()


async def run_loop():
    while True:
        await asyncio.sleep(0.25)
        if not sim.paused:
            for _ in range(int(sim.speed)):
                sim.step()
        sim.broadcast(sim.tick())


async def demo_script():
    plan = [("frozen", 10), ("packet_loss", 8), ("market_fast_move", 8), ("decimal_shift", 8), ("novel_bitflip", 10)]
    try:
        for scenario, secs in plan:
            sim.inject(scenario, sim.feeds[-1].id, None)
            await asyncio.sleep(secs)
            sim.clear()
            await asyncio.sleep(3)
    finally:
        sim.demo_task = None


@contextlib.asynccontextmanager
async def lifespan(_app):
    task = asyncio.create_task(run_loop())
    yield
    task.cancel()


app = FastAPI(title="FeedSentinel mock", lifespan=lifespan)


@app.get("/")
async def root():
    return RedirectResponse("/login")


@app.get("/login")
@app.get("/ops")
@app.get("/trader")
async def spa_page():
    return FileResponse(DASH / "index.html")


OPEN_API = {"/api/auth/login", "/api/auth/refresh", "/api/auth/logout"}
OPS_API = ("/api/chaos", "/api/control", "/api/demo", "/api/incidents", "/api/timeline", "/api/rca", "/api/metrics", "/api/quarantine")


def user_of(request) -> dict | None:
    h = request.headers.get("authorization", "")
    return USERS.get(TOKENS.get(h[7:] if h.lower().startswith("bearer ") else "", ""))


@app.middleware("http")
async def auth_mw(request: Request, call_next):
    path = request.url.path
    if path.startswith("/api/") and path not in OPEN_API:
        u = user_of(request)
        if not u:
            return JSONResponse({"detail": "not authenticated"}, status_code=401)
        if path.startswith("/api/audit") and u["role"] != "admin":
            return JSONResponse({"detail": "admin only"}, status_code=403)
        if u["role"] == "trader" and path.startswith(OPS_API):
            return JSONResponse({"detail": "not permitted for role trader"}, status_code=403)
        request.state.user = u
    return await call_next(request)


def issued(username: str) -> JSONResponse:
    tok, rt = secrets.token_urlsafe(24), secrets.token_urlsafe(24)
    TOKENS[tok], REFRESH[rt] = username, username
    resp = JSONResponse({"access_token": tok, "expires_in": 900, "user": USERS[username]})
    resp.set_cookie("fs_refresh", rt, httponly=True, samesite="lax", path="/api/auth")
    return resp


@app.post("/api/auth/login")
async def auth_login(body: dict = Body(...)):
    u = USERS.get(str(body.get("username", "")))
    if not u or body.get("password") != "demo123":
        audit(str(body.get("username", "?")), "login", outcome="denied")
        return JSONResponse({"detail": "Invalid username or password"}, status_code=401)
    audit(u["username"], "login")
    return issued(u["username"])


@app.post("/api/auth/refresh")
async def auth_refresh(request: Request):
    username = REFRESH.pop(request.cookies.get("fs_refresh", ""), None)
    if not username:
        return JSONResponse({"detail": "refresh token invalid"}, status_code=401)
    return issued(username)


@app.post("/api/auth/logout")
async def auth_logout(request: Request):
    h = request.headers.get("authorization", "")
    u = TOKENS.pop(h[7:], None) if h.lower().startswith("bearer ") else None
    REFRESH.pop(request.cookies.get("fs_refresh", ""), None)
    if u:
        audit(u, "logout")
    resp = JSONResponse({"ok": True})
    resp.delete_cookie("fs_refresh", path="/api/auth")
    return resp


@app.get("/api/auth/me")
async def auth_me(request: Request):
    return request.state.user


def hello_for(u: dict) -> dict:
    view = "trader" if u["role"] == "trader" else "ops"
    h = {**sim.hello(), "user": u, "view": view}
    if view == "trader":
        h["scenarios"] = []
    return h


app.mount("/static", StaticFiles(directory=DASH), name="static")


@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket):
    await ws.accept()
    u = USERS.get(TOKENS.get(ws.query_params.get("token", ""), ""))
    if not u:
        await ws.close(code=4401)
        return
    trader = u["role"] == "trader"
    seq = 0

    def out(m):
        nonlocal seq
        seq += 1
        if m.get("type") == "hello":
            m = hello_for(u)
        elif trader and m.get("type") == "tick":
            m = sim.trader_tick(u)
        return {**m, "seq": seq}

    q: asyncio.Queue = asyncio.Queue(maxsize=8)
    sim.hub.add(q)
    try:
        await ws.send_json(out({"type": "hello"}))
        await ws.send_json(out(sim.tick()))
        while True:
            await ws.send_json(out(await q.get()))
    except (WebSocketDisconnect, RuntimeError):
        pass
    finally:
        sim.hub.discard(q)


@app.get("/api/state")
async def api_state(request: Request):
    u = request.state.user
    return sim.trader_tick(u) if u["role"] == "trader" else sim.tick()


@app.get("/api/hello")
async def api_hello(request: Request):
    return hello_for(request.state.user)


@app.get("/api/timeline")
async def api_timeline(limit: int = 200, feed: str | None = None):
    rows = [{k: v for k, v in e.items() if k != "_key"} for e in sim.timeline if feed is None or e["feed"] == feed]
    return rows[-limit:]


@app.get("/api/rca/{iid}")
async def api_rca(iid: str):
    inc = next((i for i in sim.incidents if i["id"] == iid), None)
    if not inc:
        raise HTTPException(404, f"unknown incident {iid}")
    return inc["rca"]


@app.post("/api/incidents/{iid}/ack")
async def api_ack(iid: str, request: Request):
    inc = next((i for i in sim.incidents if i["id"] == iid), None)
    if not inc:
        raise HTTPException(404, f"unknown incident {iid}")
    u = request.state.user
    if not inc.get("acked_by"):
        inc.update(acked_by=u["display_name"], acked_ms=sim.t_ms)
        inc["timeline"].append({"t_ms": sim.t_ms, "clock": sim.clock(sim.t_ms), "event": f"acknowledged by {u['display_name']}"})
        sim.add_tl("action", f"{iid} acknowledged by {u['display_name']}", inc["feed"], iid)
        audit(u["username"], "incident.ack", iid)
    return {"ok": True}


@app.get("/api/audit")
async def api_audit(limit: int = 100):
    return AUDIT[-limit:][::-1]


@app.get("/api/audit/verify")
async def api_audit_verify():
    prev = "0" * 64
    for e in AUDIT:
        body = {k: v for k, v in e.items() if k != "hash"}
        if hashlib.sha256((prev + json.dumps(body, sort_keys=True)).encode()).hexdigest() != e["hash"]:
            return {"ok": False, "entries": len(AUDIT), "bad_seq": e["seq"]}
        prev = e["hash"]
    return {"ok": True, "entries": len(AUDIT)}


@app.post("/api/chat")
async def api_chat(request: Request, body: dict = Body(...)):
    u = request.state.user
    m = str(body.get("message", "")).lower()
    trader = u["role"] == "trader"
    t = sim.tick()
    if trader and any(w in m for w in ("chaos", "inject", "audit", "raw", "sample", "admin")):
        return {"answer": "I can only answer questions about prices and feed trust for your regions "
                          f"({', '.join(u['regions'])}). Operations tooling is not available to the trader role.",
                "sources": [], "tools": [], "refused": True}
    if "region" in m:
        return {"answer": f"Your regions: {', '.join(u['regions'])}.\nYou only see prices for symbols in those regions.",
                "sources": ["user profile"], "tools": ["get_user"], "refused": False}
    sym = next((s for s in sim.symbols if s.lower() in m), None)
    if sym:
        wl = next((w for w in sim.trader_tick(u)["watchlist"] if w["symbol"] == sym), None)
        if wl:
            return {"answer": f"{sym}: {wl['trust']}. {wl['trust_reason']}.\nUse the {wl['source_name']} feed "
                              f"(feed {wl['source']}); last price {wl['price']} at {t['clock']}.",
                    "sources": [f"consensus {sym} @ {t['clock']}", f"feed {wl['source']} health"],
                    "tools": ["get_trust", "get_recommended_source"], "refused": False}
    fd = next((f for f in t["feeds"] if f"feed {f['id'].lower()}" in m), None)
    if not trader and (fd or "red" in m):
        fd = fd or min(t["feeds"], key=lambda f: f["health"])
        inc = next((i for i in sim.incidents if i["feed"] == fd["id"] and i["status"] == "OPEN"), None)
        lines = [f"Feed {fd['id']} ({fd['name']}) is {fd['state']} with health {fd['health']:.0f}/100."]
        if inc:
            lines += [f"Open incident {inc['id']}: {inc['title']} - {inc['headline']}.",
                      f"Likely cause: {inc['rca']['primary']['cause']} ({inc['rca']['primary']['confidence']}%).",
                      f"Action: {inc['rca']['action']}."]
        else:
            lines.append("There is no open incident on this feed.")
        return {"answer": "\n".join(lines), "sources": ([inc["id"]] if inc else []) + [f"feed {fd['id']} health @ {t['clock']}"],
                "tools": ["get_feed_health", "get_rca"], "refused": False}
    recent = [e for e in sim.timeline if e["t_ms"] >= sim.t_ms - 600_000][-8:]
    lines = [f"Last 10 minutes ({len(recent)} events):"] + [f"{e['clock']}  {e['text']}" for e in recent]
    return {"answer": "\n".join(lines), "sources": ["timeline"], "tools": ["get_timeline"], "refused": False}


@app.get("/api/incidents")
async def api_incidents(limit: int = 100):
    return [summary(i) for i in sim.incidents[:limit]]


@app.get("/api/incidents/{iid}")
async def api_incident(iid: str):
    inc = next((i for i in sim.incidents if i["id"] == iid), None)
    if not inc:
        raise HTTPException(404, f"unknown incident {iid}")
    return inc


@app.post("/api/incidents/{iid}/feedback")
async def api_feedback(iid: str, body: dict = Body(...)):
    inc = next((i for i in sim.incidents if i["id"] == iid), None)
    if not inc:
        raise HTTPException(404, f"unknown incident {iid}")
    if body.get("label") not in ("confirmed", "false_positive"):
        raise HTTPException(400, "label must be 'confirmed' or 'false_positive'")
    inc["feedback"] = body["label"]
    inc["timeline"].append({"t_ms": sim.t_ms, "clock": sim.clock(sim.t_ms), "event": f"operator feedback: {body['label']}"})
    return {"ok": True}


@app.post("/api/chaos")
async def api_chaos(request: Request, body: dict = Body(...)):
    if body.get("scenario") not in SCN:
        raise HTTPException(400, f"unknown scenario {body.get('scenario')!r}")
    if body.get("feed") not in sim.fmap:
        raise HTTPException(400, f"unknown feed {body.get('feed')!r}")
    syms = body.get("symbols")
    if syms is not None and (not isinstance(syms, list) or any(s not in sim.symbols for s in syms)):
        raise HTTPException(400, "symbols must be null or a list of known symbols")
    sim.add_tl("action", f"Chaos Lab: {SCN[body['scenario']]['label']} injected on {body['feed']}", body["feed"])
    audit(request.state.user["username"], "chaos.inject", f"{body['scenario']}@{body['feed']}")
    return sim.inject(body["scenario"], body["feed"], syms, body.get("params"), body.get("duration_s"))


@app.delete("/api/chaos/{fid}")
async def api_chaos_delete(fid: str):
    if not sim.clear(fid):
        raise HTTPException(404, f"no active fault {fid}")
    return {"ok": True}


@app.post("/api/chaos/clear")
async def api_chaos_clear():
    return {"ok": True, "cleared": sim.clear()}


@app.post("/api/control")
async def api_control(body: dict = Body(...)):
    if "mode" in body:
        if body["mode"] not in MODES:
            raise HTTPException(400, "mode must be 'live' or 'replay'")
        sim.reset(body["mode"])
        sim.broadcast(sim.hello())
    if body.get("restart"):
        sim.reset(sim.mode)
        sim.broadcast(sim.hello())
    if "speed" in body:
        if body["speed"] not in sim.cfg["speeds"]:
            raise HTTPException(400, f"speed must be one of {sim.cfg['speeds']}")
        sim.speed = body["speed"]
    if "paused" in body:
        sim.paused = bool(body["paused"])
    if "symbol" in body:
        if body["symbol"] not in sim.symbols:
            raise HTTPException(400, f"unknown symbol {body['symbol']!r}")
        sim.symbol = body["symbol"]
    return {"ok": True}


@app.post("/api/demo")
async def api_demo(body: dict = Body(...)):
    action = body.get("action")
    if action == "start":
        if sim.demo_task is None:
            sim.demo_task = asyncio.get_running_loop().create_task(demo_script())
    elif action == "stop":
        if sim.demo_task:
            sim.demo_task.cancel()
        sim.clear()
    else:
        raise HTTPException(400, "action must be 'start' or 'stop'")
    return {"ok": True}


@app.get("/api/quarantine")
async def api_quarantine(feed: str = "C", limit: int = 50):
    fd = sim.fmap.get(feed)
    n = min(limit, fd.quarantined if fd else 0)
    return [{"reason": random.choice(["OFF_TICK", "CROSSED_BOOK", "NEG_SIZE", "BAD_TYPE"]), "t_ms": sim.t_ms - i * 700,
             "msg": {"feed": feed, "seq": 88000 + i, "type": "Q", "sym": sim.symbols[i % len(sim.symbols)], "bid": -1.0}}
            for i in range(n)]


@app.get("/api/metrics")
async def api_metrics():
    if OPTS.empty_metrics:
        return {}
    labels = ["NORMAL", "GAP", "DELAY", "FROZEN", "STALE", "DECIMAL_SHIFT", "GARBLED"]
    conf = [[480, 2, 1, 0, 0, 0, 3], [1, 58, 0, 0, 1, 0, 0], [3, 0, 55, 0, 2, 0, 0], [0, 0, 0, 60, 0, 0, 0],
            [0, 1, 2, 4, 53, 0, 0], [0, 0, 0, 0, 0, 60, 0], [2, 0, 0, 0, 0, 1, 57]]
    per = []
    for i, lab in enumerate(labels[1:], 1):
        tp, col = conf[i][i], sum(r[i] for r in conf)
        p, r = tp / col, tp / sum(conf[i])
        per.append({"label": lab, "precision": round(p, 3), "recall": round(r, 3), "f1": round(2 * p * r / (p + r), 3), "support": sum(conf[i])})
    faults = [("packet_loss", "Packet loss", "fault", 12, 12, 1.0, 0.6, 1.1), ("delay", "Delay", "fault", 12, 12, 1.0, 0.9, 1.6),
              ("frozen", "Frozen feed", "fault", 12, 12, 1.0, 1.4, 2.2), ("stale", "Stale feed", "fault", 12, 12, 0.92, 1.1, 1.9),
              ("decimal_shift", "Decimal shift", "fault", 12, 12, 1.0, 0.3, 0.5), ("drift", "Slow drift", "fault", 12, 10, 0.9, 4.5, 9.8),
              ("novel_bitflip", "Bit-flip corruption", "novel", 8, 7, 0.0, 1.8, 3.9)]
    return {
        "generated_at": "2026-09-26T15:00:00Z",
        "data": {"source": "LOBSTER AAPL/AMZN/GOOG/INTC/MSFT 2012-06-21", "train": "09:30-12:30",
                 "validation": "12:30-13:30", "test": "13:30-16:00"},
        "clean": {"market_hours": 2.5, "false_alarms": 0, "false_alarms_per_hour": 0.0,
                  "by_feed": {"A": 0, "B": 0, "C": 0}, "market_events": 12, "market_event_alerts": 0},
        "faults": [{"fault": a, "label": b, "group": c, "episodes": d, "detected": e, "detection_rate": round(e / d, 3),
                    "correct_code_rate": f, "ttd_p50_s": g, "ttd_p95_s": h} for a, b, c, d, e, f, g, h in faults],
        "incidents": {"total": 150, "matched": 148, "precision": 0.987, "collateral_on_healthy_feeds": 0},
        "classifier": {"labels": labels, "confusion": conf, "per_class": per,
                       "macro_f1": round(sum(x["f1"] for x in per) / len(per), 3), "accuracy": 0.975},
        "anomaly": {"threshold": 0.58, "percentile": 99.5, "clean_windows": 50000, "flag_rate_clean": 0.005},
        "ablation": {"configs": ["rules", "rules+consensus", "rules+consensus+ml"],
                     "rows": [{"fault": "frozen", "label": "Frozen feed", "rates": [0.0, 1.0, 1.0]},
                              {"fault": "packet_loss", "label": "Packet loss", "rates": [1.0, 1.0, 1.0]},
                              {"fault": "drift", "label": "Slow drift", "rates": [0.0, 0.75, 0.83]},
                              {"fault": "novel_bitflip", "label": "Bit-flip (novel)", "rates": [0.25, 0.38, 0.88]}],
                     "overall": [0.6, 0.9, 0.95], "false_alarms_per_hour": [0.0, 0.0, 0.4]},
        "throughput": {"events_per_s": 40000},
    }


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=8001)
    ap.add_argument("--no-models", action="store_true")
    ap.add_argument("--empty-metrics", action="store_true")
    args = ap.parse_args()
    OPTS.no_models, OPTS.empty_metrics = args.no_models, args.empty_metrics
    uvicorn.run(app, host=args.host, port=args.port, log_level="warning")


if __name__ == "__main__":
    main()
