"""The detection engine: L0-L7 wired together.

    per message   L0 context -> L1 header -> L2 sequence & timing -> L1 payload -> state/consensus input
    per window    L5 consensus summary + L3 features -> L4 AI -> findings -> L6 health -> L7 incidents

All time is market time (int ns): the engine only ever uses message timestamps and the times it is
advanced to, so the same code runs in real time behind the server and faster than real time in
evaluation.
"""
from __future__ import annotations

import logging
import statistics
import threading
from collections import Counter, deque

import numpy as np

from .. import schema as S
from ..config import MS, NS, Config
from ..timeutil import NEW_YORK
from . import features as FX
from .consensus import Consensus
from .context import MarketContext
from .gatekeeper import (CONFLICT, GARBLE_REASONS, PRICE_BAND, REASON_TEXT, TEST_ISSUE, Gatekeeper,
                         check_header)
from .health import FeedHealth, Finding, absorb
from .incidents import IncidentManager
from .sequencer import CONFLICT as SEQ_CONFLICT
from .sequencer import DUP, RESET, Sequencer, content_hash

log = logging.getLogger("feedsentinel.engine")

ML_WARMUP_WINDOWS = 30


def _jsonable(msg: dict) -> dict:
    out = {}
    for k, v in msg.items():
        out[k] = v if isinstance(v, (int, float, str, bool)) or v is None else repr(v)
    return out


class FeedRuntime:
    """Per-feed state: transport, window counters, baselines, health."""

    def __init__(self, info: dict, cfg: Config):
        self.id = info["id"]
        self.name = info.get("name", self.id)
        self.role = info.get("role", "")
        self.seq = Sequencer(cfg)
        self.health = FeedHealth(cfg)
        self.last_recv = 0
        self.last_data_recv = 0
        self.last_hb_recv = 0
        self.quotes: dict[str, tuple] = {}
        self.quarantine: deque = deque(maxlen=200)
        self.lat_p99_hist: deque = deque(maxlen=600)
        self.lat_p50_hist: deque = deque(maxlen=600)
        self.base_p99 = None
        self.base_p50 = None
        self.ewma_msgs = None
        self.delay_run = 0
        self.prev = {"garbled": 0, "dups": 0, "reorders": 0}
        self.ml_flags: deque = deque(maxlen=3)
        self.ml_view: dict | None = None
        self.raw_skew: deque = deque(maxlen=300)
        self.msgs_hist: deque = deque(maxlen=3)      # messages per window, last 3 windows
        self.total_msgs = 0
        self.total_data = 0
        self.total_quarantined = 0
        self.bad_symbols: set = set()
        self.last_raw: dict = {}
        self.reset_window()

    def reset_window(self) -> None:
        self.w_msgs = 0
        self.w_hb = 0
        self.w_data = 0
        self.w_garbled = 0
        self.w_reasons: Counter = Counter()
        self.w_test = 0
        self.w_test_syms: set = set()
        self.w_band: list = []
        self.w_unknown = 0
        self.w_future = 0
        self.w_max_future_ms = 0.0
        self.w_lat: list = []
        self.w_prices: dict = {}
        self.w_samples: dict = {}
        self.w_last_quote: dict = {}

    def sample(self, code: str, msg: dict, reason: str | None = None) -> None:
        lst = self.w_samples.setdefault(code, [])
        if len(lst) < 5:
            m = _jsonable(msg)
            if reason:
                m["_reason"] = reason
            lst.append(m)


class Engine:
    def __init__(self, cfg: Config, feeds: list[dict], symbols, refdata, models=None, explainer=None,
                 tz=NEW_YORK, open_ns: int | None = None, close_ns: int | None = None, annotate=None,
                 record_windows: bool = False, chart: bool = True):
        self.cfg = cfg
        self.layers = cfg.layers
        self.tz = tz
        self.feed_ids = [f["id"] for f in feeds]
        self.feed_set = set(self.feed_ids)
        self.feeds = {f["id"]: FeedRuntime(f, cfg) for f in feeds}
        self.symbols = list(symbols)
        self.refdata = refdata
        self.ctx = MarketContext(cfg, self.feed_ids, self.symbols, open_ns, close_ns,
                                 always_open=cfg.mode == "live")
        self.gate = Gatekeeper(cfg, refdata, self.ctx)
        self.cons = Consensus(cfg, self.feed_ids, self.symbols)
        self.models = models if "L4" in self.layers else None
        self.online = None
        if "L4" in self.layers and cfg.mode == "live":
            self.online = OnlineAnomaly(cfg)
        self.incidents = IncidentManager(cfg, {f: r.name for f, r in self.feeds.items()}, tz,
                                         explainer=explainer, annotate=annotate)
        self.window_ns = int(cfg.window_s * NS)
        self.future_ns = int(cfg.future_tol_ms * MS)
        self.hb_timeout = cfg.hb_timeout_s
        self.now = 0
        self.window_end: int | None = None
        self.windows = 0
        self.processed = 0
        self.clock_offset_ns = 0
        self.record_windows = record_windows
        self.window_log: list = []
        self.chart_on = chart
        self.chart_step = int(cfg.chart_step_ms * MS)
        self.next_chart: int | None = None
        self.chart_t: deque = deque(maxlen=cfg.chart_points)
        self.chart_v = {s: {k: deque(maxlen=cfg.chart_points) for k in ["consensus"] + self.feed_ids}
                        for s in self.symbols}
        self.markers: deque = deque(maxlen=300)
        self.recommended = {s: self.feed_ids[0] for s in self.symbols}
        self.on_window = None          # optional callback(engine, end_ns)

    # ------------------------------------------------------------------ time
    def start(self, t: int) -> None:
        self.now = t
        self.window_end = (t // self.window_ns + 1) * self.window_ns
        self.next_chart = (t // self.chart_step + 1) * self.chart_step
        self.ctx.init_time(t)
        for fr in self.feeds.values():
            fr.last_recv = fr.last_data_recv = fr.last_hb_recv = t

    def advance(self, t: int) -> None:
        """Advance market time to t: close finished windows, evaluate consensus, sample the chart."""
        if self.window_end is None:
            self.start(t)
        while self.window_end <= t:
            self._close_window(self.window_end)
            self.window_end += self.window_ns
        if t > self.now:
            self.now = t
        if "L5" in self.layers:
            self.cons.advance(t)
        if self.chart_on:
            while self.next_chart <= t:
                self._sample_chart(self.next_chart)
                self.next_chart += self.chart_step

    # ------------------------------------------------------------------ per message
    def on_message(self, msg: dict) -> None:
        recv = msg.get("recv_ts")
        if not isinstance(recv, int):
            recv = self.now
        if self.window_end is None:
            self.start(recv)
        if recv >= self.window_end:
            self.advance(recv)
        if recv > self.now:
            self.now = recv
        self.processed += 1
        fr = self.feeds.get(msg.get("feed")) if isinstance(msg, dict) else None
        if fr is None:
            return
        fr.last_recv = recv
        fr.total_msgs += 1
        reason = check_header(msg, self.feed_set)
        if reason is not None:
            fr.w_msgs += 1
            self._quarantine(fr, reason, msg, recv)
            return
        typ = msg.get("type")
        exch = msg.get("exch_ts")
        exch_ok = isinstance(exch, int) and not isinstance(exch, bool)
        if exch_ok:
            lat_ns = recv - exch + self.clock_offset_ns
            if -3600 * NS < lat_ns < 3600 * NS:
                if lat_ns < -self.future_ns:
                    fr.w_future += 1
                    v = -lat_ns / MS
                    if v > fr.w_max_future_ms:
                        fr.w_max_future_ms = v
                    fr.sample(S.CLOCK_SKEW, msg)
                if "L5" in self.layers:
                    self.cons.on_receive(fr.id, exch - self.clock_offset_ns, recv)
            else:
                exch_ok = False
        if typ == S.HEARTBEAT:
            fr.w_hb += 1
            fr.last_hb_recv = recv
            fr.seq.on_heartbeat(msg["session"], msg["seq"], recv)
            return
        fr.w_msgs += 1
        st = fr.seq.on_message(msg["session"], msg["seq"], content_hash(msg), recv)
        if st == DUP:
            fr.sample(S.DUPLICATE, msg)
            return
        if st == SEQ_CONFLICT:
            fr.sample(S.CONFLICTING_DUPLICATE, msg)
            self._quarantine(fr, CONFLICT, msg, recv)
            return
        if st == RESET:
            fr.sample(S.SEQ_RESET, msg)
        reason, detail = self.gate.check(msg, recv)
        if reason is None or reason == PRICE_BAND or reason == TEST_ISSUE:
            self._track_price(fr, msg, typ, detail)
        if reason is not None:
            if reason == PRICE_BAND:
                detail["t"] = exch
                fr.w_band.append(detail)
                fr.sample(S.DECIMAL_SHIFT if detail["power"] else S.PRICE_SPIKE, msg, reason)
            elif reason == TEST_ISSUE:
                fr.w_test += 1
                fr.w_test_syms.add(msg.get("sym"))
                fr.sample(S.TEST_DATA_LEAK, msg, reason)
            else:
                fr.w_garbled += 1
                if reason == "UNKNOWN_SYMBOL":
                    fr.w_unknown += 1
                    fr.sample(S.RATE_STORM, msg, reason)
                fr.sample(S.GARBLED, msg, reason)
            self._quarantine(fr, reason, msg, recv)
            return
        if typ == S.QUOTE or typ == S.TRADE:
            fr.w_data += 1
            fr.total_data += 1
            fr.last_data_recv = recv
            if exch_ok:
                lat_ms = (recv - exch + self.clock_offset_ns) / MS
                if lat_ms >= -self.cfg.future_tol_ms:
                    fr.w_lat.append(lat_ms)
                if self.cfg.mode == "live":
                    fr.raw_skew.append(exch - recv)
            sym = msg["sym"]
            if typ == S.QUOTE:
                q = (msg["bid"], msg["ask"], msg["bid_sz"], msg["ask_sz"], exch)
                fr.quotes[sym] = q
                if "L5" in self.layers:
                    self.cons.on_quote(fr.id, sym, exch - self.clock_offset_ns, q[0], q[1], q[2], q[3])
                fr.w_last_quote[sym] = msg
        elif typ == S.SYSTEM:
            self.ctx.on_system(fr.id, msg["event"])
        elif typ == S.TRADING_ACTION:
            self.ctx.on_trading_action(fr.id, msg["sym"], msg["state"])

    def _track_price(self, fr: FeedRuntime, msg: dict, typ: str, detail) -> None:
        if detail is not None:
            price = detail["price"]
        elif typ == S.QUOTE:
            b, a = msg.get("bid"), msg.get("ask")
            if not isinstance(b, (int, float)) or not isinstance(a, (int, float)):
                return
            price = (b + a) / 2.0
        elif typ == S.TRADE:
            price = msg.get("px")
            if not isinstance(price, (int, float)):
                return
        else:
            return
        key = round(float(price), 4)
        syms = fr.w_prices.get(key)
        if syms is None:
            fr.w_prices[key] = {msg.get("sym")}
        else:
            syms.add(msg.get("sym"))

    def _quarantine(self, fr: FeedRuntime, reason: str, msg, t: int) -> None:
        fr.total_quarantined += 1
        fr.w_reasons[reason] += 1
        fr.quarantine.append({"reason": reason, "text": REASON_TEXT.get(reason, reason),
                              "t_ms": t // MS, "msg": _jsonable(msg) if isinstance(msg, dict) else repr(msg)})

    # ------------------------------------------------------------------ per window
    def _close_window(self, end: int) -> None:
        cfg = self.cfg
        self.windows += 1
        if "L5" in self.layers:
            self.cons.advance(end)
        for fr in self.feeds.values():
            fr.seq.confirm(end)
        if cfg.mode == "live":
            self._estimate_clock_offset()
        self._market_moves()
        halted = set(self.ctx.halted_symbols())
        for fr in self.feeds.values():
            fr.msgs_hist.append(fr.w_msgs)
        raws = {f: self._window_raw(fr, end, halted) for f, fr in self.feeds.items()}
        X = np.vstack([FX.build(raws[f]) for f in self.feed_ids])
        self._ml(X)
        for i, f in enumerate(self.feed_ids):
            fr = self.feeds[f]
            findings = self._findings(fr, raws[f], end)
            findings = absorb(findings, self.incidents.open_codes(f))
            fr.health.update(findings)
            fr.bad_symbols = {s for fd in findings for s in fd.symbols}
            fr.last_raw = raws[f]
            if "L5" in self.layers:
                self.cons.trust[f] = fr.health.trust
            before = len(self.incidents.incidents)
            self.incidents.process(f, findings, end, self._best_feed_name(exclude=f), fr.ml_view)
            for inc in self.incidents.incidents[before:]:
                self.markers.append({"t_ms": inc.opened_ns // MS, "feed": inc.feed, "code": inc.code,
                                     "severity": inc.severity, "symbols": list(inc.symbols)})
            if self.record_windows:
                self.window_log.append((end, f, X[i].copy(), fr.health.state, [fd.code for fd in findings]))
            self._update_baselines(fr, raws[f])
        self.incidents.expire(end)
        self._recommend()
        if "L5" in self.layers:
            for s in self.symbols:
                c = self.cons.cons[s]
                if c is not None and not self.ctx.is_halted(s):
                    self.ctx.update_ref(s, c)
            self.cons.reset_window()
            self.cons.roll_latency()
        else:
            for s in self.symbols:                # rules-only: reference from the first healthy feed
                for f in self.feed_ids:
                    q = self.feeds[f].quotes.get(s)
                    if q is not None and self.feeds[f].health.state == S.HEALTHY:
                        self.ctx.update_ref(s, (q[0] + q[1]) / 2.0)
                        break
        for fr in self.feeds.values():
            fr.prev = {"garbled": fr.w_garbled, "dups": fr.seq.w_dups, "reorders": fr.seq.w_reorders}
            fr.reset_window()
            fr.seq.reset_window()
        if self.on_window is not None:
            self.on_window(self, end)

    def _market_moves(self) -> None:
        """If most feeds break the band for the same symbol together, the market moved (L0)."""
        by_sym: dict[str, dict] = {}
        for f, fr in self.feeds.items():
            for ev in fr.w_band:
                if not ev["power"]:
                    by_sym.setdefault(ev["sym"], {})[f] = ev["price"]
        for sym, prices in by_sym.items():
            if len(prices) >= self.ctx.majority:
                vals = sorted(prices.values())
                med = vals[len(vals) // 2]
                if all(abs(v / med - 1) < 0.005 for v in vals):
                    self.ctx.reanchor(sym, med)
                    for f in prices:
                        fr = self.feeds[f]
                        fr.w_band = [e for e in fr.w_band if e["sym"] != sym]

    def _window_raw(self, fr: FeedRuntime, end: int, halted: set) -> dict:
        lat = fr.w_lat
        if lat:
            lat.sort()
            n = len(lat)
            p50, p99 = lat[n // 2], lat[min(n - 1, int(0.99 * n))]
        else:
            p50 = p99 = None
        peers = [self.feeds[f].w_msgs for f in self.feed_ids if f != fr.id]
        peer_msgs = float(statistics.median(peers)) if peers else None
        # 3-second sums for the storm rule: a latency shift moves a burst between windows, it does
        # not multiply it
        peers3 = [sum(self.feeds[f].msgs_hist) for f in self.feed_ids if f != fr.id]
        peer_msgs3 = float(statistics.median(peers3)) if peers3 else None
        peer_data = sum(self.feeds[f].w_data for f in self.feed_ids if f != fr.id)
        identical, ident_px, ident_syms = 0, None, []
        for px, syms in fr.w_prices.items():
            if len(syms) > identical:
                identical, ident_px, ident_syms = len(syms), px, syms
        cs = self.cons.feed_summary(fr.id, halted, end) if "L5" in self.layers else None
        seq = fr.seq
        return {
            "msgs": fr.w_msgs, "data": fr.w_data, "garbled": fr.w_garbled, "peer_msgs": peer_msgs,
            "msgs3": sum(fr.msgs_hist), "peer_msgs3": peer_msgs3, "windows3": len(fr.msgs_hist),
            "peer_data": peer_data, "ewma_msgs": fr.ewma_msgs,
            "lat_p50": p50, "lat_p99": p99, "base_p50": fr.base_p50, "base_p99": fr.base_p99,
            "missing": seq.w_missing, "gap_first": seq.w_gap_first, "gap_last": seq.w_gap_last,
            "dups": seq.w_dups, "conflicts": seq.w_conflicts, "conflict_seq": seq.w_conflict_seq,
            "reorders": seq.w_reorders, "late": seq.w_late, "resets": seq.w_resets,
            "reset_from": seq.w_reset_from, "reset_to": seq.w_reset_to,
            "dev_max": cs["dev_max"] if cs else 0.0, "dev_mean": cs["dev_mean"] if cs else 0.0,
            "frozen_frac": cs["frozen_frac"] if cs else 0.0, "stale_frac": cs["stale_frac"] if cs else 0.0,
            "size_mismatch_frac": cs["size_mismatch_frac"] if cs else 0.0,
            "cusum_norm": cs["cusum_norm"] if cs else 0.0, "cons": cs,
            "hb_age_s": (end - fr.last_recv) / NS, "silent_s": (end - fr.last_data_recv) / NS,
            "future": fr.w_future, "max_future_ms": fr.w_max_future_ms,
            "identical_syms": identical, "identical_px": ident_px, "identical_list": sorted(ident_syms),
            "unknown_syms": fr.w_unknown + fr.w_test, "band_viol": len(fr.w_band),
            "test": fr.w_test, "test_syms": sorted(s for s in fr.w_test_syms if isinstance(s, str)),
        }

    def _update_baselines(self, fr: FeedRuntime, raw: dict) -> None:
        healthy = fr.health.state == S.HEALTHY
        if healthy and raw["lat_p99"] is not None and raw["data"] >= 5:
            fr.lat_p99_hist.append(raw["lat_p99"])
            fr.lat_p50_hist.append(raw["lat_p50"])
            if len(fr.lat_p99_hist) >= self.cfg.lat_baseline_min_windows:
                fr.base_p99 = statistics.median(fr.lat_p99_hist)
                fr.base_p50 = statistics.median(fr.lat_p50_hist)
        if healthy:
            fr.ewma_msgs = raw["msgs"] if fr.ewma_msgs is None else 0.95 * fr.ewma_msgs + 0.05 * raw["msgs"]

    def _estimate_clock_offset(self) -> None:
        """Live mode: a skew every venue shares is our clock, not theirs (common mode)."""
        meds = [statistics.median(fr.raw_skew) for fr in self.feeds.values() if len(fr.raw_skew) >= 20]
        if len(meds) >= 2:
            self.clock_offset_ns = int(statistics.median(meds))

    # ------------------------------------------------------------------ AI
    def _ml(self, X: np.ndarray) -> None:
        models = self.models
        ready = self.windows > ML_WARMUP_WINDOWS
        if self.online is not None:
            self.online.observe(X, [self.feeds[f].health.state for f in self.feed_ids])
        for i, f in enumerate(self.feed_ids):
            self.feeds[f].ml_view = None
        if not ready:
            return
        if models is not None and models.has_anomaly:
            scores = models.anomaly_scores(X)
            thr = models.threshold
            diags = models.classify(X) if models.has_classifier else [None] * len(self.feed_ids)
            zsrc = models
        elif self.online is not None and self.online.ready:
            scores = self.online.scores(X)
            thr = self.online.threshold
            diags = [None] * len(self.feed_ids)
            zsrc = self.online
        else:
            return
        for i, f in enumerate(self.feed_ids):
            fr = self.feeds[f]
            s = float(scores[i])
            flag = s > thr
            fr.ml_flags.append(flag)
            fr.ml_view = {"score": round(s, 3), "threshold": round(thr, 3), "flag": flag,
                          "diagnosis": diags[i], "top_features": zsrc.top_features(X[i])}

    # ------------------------------------------------------------------ findings
    def _findings(self, fr: FeedRuntime, r: dict, end: int) -> list[Finding]:
        cfg = self.cfg
        out: list[Finding] = []
        add = out.append
        is_open = self.ctx.is_open()
        data = r["data"]

        # ---- L2 transport
        if r["hb_age_s"] > self.hb_timeout and (is_open or cfg.mode == "live"):
            exp = fr.seq.expected
            add(Finding(S.DISCONNECT, S.CRITICAL, 1.0,
                        f"no heartbeat or data for {r['hb_age_s']:.1f} s",
                        {"heartbeat_age_s": round(r["hb_age_s"], 1),
                         "last_seq": None if exp is None else exp - 1, "resume_seq": exp}))
            return out
        if r["missing"] > 0:
            lr = r["missing"] / (r["missing"] + max(1, r["msgs"]))
            add(Finding(S.GAP, S.CRITICAL if lr >= 0.2 else S.DEGRADED, min(1.0, 0.4 + 3 * lr),
                        f"{r['missing']} messages lost (seq {r['gap_first']}-{r['gap_last']})",
                        {"missing": r["missing"], "gap_first": r["gap_first"], "gap_last": r["gap_last"],
                         "loss_rate_pct": round(100 * lr, 1)}))
        if r["conflicts"] > 0:
            add(Finding(S.CONFLICTING_DUPLICATE, S.CRITICAL, 1.0,
                        f"{r['conflicts']} sequence numbers re-sent with different content "
                        f"(e.g. seq {r['conflict_seq']})",
                        {"conflicts": r["conflicts"], "example_seq": r["conflict_seq"]},
                        samples=fr.w_samples.get(S.CONFLICTING_DUPLICATE, [])))
        d = r["dups"]
        if d >= 3 or (d >= 1 and fr.prev["dups"] >= 1):
            rate = d / max(1, r["msgs"])
            add(Finding(S.DUPLICATE, S.DEGRADED, min(1.0, 0.3 + 3 * rate),
                        f"{d} duplicate messages ({100 * rate:.0f}% of traffic)",
                        {"duplicates": d, "dup_rate_pct": round(100 * rate, 1)},
                        samples=fr.w_samples.get(S.DUPLICATE, [])))
        ro = r["reorders"]
        if ro >= 2 or (ro >= 1 and fr.prev["reorders"] >= 1):
            add(Finding(S.OUT_OF_ORDER, S.DEGRADED, min(1.0, 0.3 + ro / 50),
                        f"{ro} messages arrived out of sequence order", {"reordered": ro}))
        if r["resets"] > 0:
            add(Finding(S.SEQ_RESET, S.DEGRADED, 0.8,
                        f"sequence restarted at {r['reset_to']} (was at {r['reset_from']})",
                        {"from_seq": r["reset_from"], "to_seq": r["reset_to"]},
                        samples=fr.w_samples.get(S.SEQ_RESET, [])))
        if fr.base_p99 is not None and r["lat_p99"] is not None:
            thr = max(cfg.lat_mult * fr.base_p99, fr.base_p99 + cfg.lat_abs_ms)
            over = r["lat_p99"] > thr
            fr.delay_run = fr.delay_run + 1 if over else 0
            if fr.delay_run >= cfg.lat_persist or (over and r["lat_p99"] > 4 * thr):
                ratio = r["lat_p99"] / max(fr.base_p99, 0.01)
                add(Finding(S.DELAY, S.CRITICAL if r["lat_p99"] >= 2000 else S.DEGRADED,
                            min(1.0, 0.4 + r["lat_p99"] / (4 * thr)),
                            f"p99 latency {r['lat_p99']:.0f} ms vs normal {fr.base_p99:.1f} ms",
                            {"lat_p99_ms": round(r["lat_p99"], 1), "baseline_p99_ms": round(fr.base_p99, 1),
                             "lat_ratio": round(ratio, 1)}))
        if r["future"] >= cfg.future_min and r["future"] >= 0.2 * max(1, data):
            add(Finding(S.CLOCK_SKEW, S.DEGRADED, 0.6,
                        f"timestamps up to {r['max_future_ms']:.0f} ms in the future",
                        {"skew_ms": round(r["max_future_ms"], 1), "future_msgs": r["future"]},
                        samples=fr.w_samples.get(S.CLOCK_SKEW, [])))

        # ---- L1 validation
        g = r["garbled"]
        if g >= cfg.garbled_min or (g >= 1 and fr.prev["garbled"] >= 1):
            reasons = Counter({k: v for k, v in fr.w_reasons.items() if k in GARBLE_REASONS})
            top = reasons.most_common(1)[0][0] if reasons else "?"
            rate = g / max(1, data + g)
            add(Finding(S.GARBLED, S.CRITICAL if rate >= 0.2 else S.DEGRADED, min(1.0, 0.4 + 3 * rate),
                        f"{g} malformed messages quarantined (top: {REASON_TEXT.get(top, top)})",
                        {"quarantined": g, "quarantine_rate_pct": round(100 * rate, 1), "top_reason": top,
                         "reasons": ", ".join(f"{k} {v}" for k, v in reasons.most_common(4))},
                        samples=fr.w_samples.get(S.GARBLED, [])))
        dec = [e for e in fr.w_band if e["power"]]
        if dec:
            e = dec[-1]
            add(Finding(S.DECIMAL_SHIFT, S.CRITICAL, 1.0,
                        f"{e['sym']} at {e['price']:g} vs reference {e['ref']:.2f} "
                        f"(x10^{e['power']} price scale)",
                        {"ratio": round(e["ratio"], 4), "power": e["power"], "symbol": e["sym"],
                         "price": round(e["price"], 4), "consensus": round(e["ref"], 4),
                         "violations": len(dec)},
                        symbols=sorted({x["sym"] for x in dec}),
                        samples=fr.w_samples.get(S.DECIMAL_SHIFT, [])))
        spikes = [e for e in fr.w_band if not e["power"]]
        # Test issues (ZVZZT...) legitimately appear on production feeds and are filtered by L1, so
        # one of them is not an incident; identical prints across unrelated symbols, or a burst of
        # test-issue traffic, is the 2017 signature.
        if r["identical_syms"] >= cfg.identical_min_symbols or r["test"] >= 3:
            head = (f"{r['identical_syms']} symbols printing the identical price {r['identical_px']:g}"
                    if r["identical_syms"] >= cfg.identical_min_symbols else "test symbols in production")
            if r["test"]:
                head += f" + test issues {', '.join(r['test_syms'])}"
            add(Finding(S.TEST_DATA_LEAK, S.CRITICAL, 1.0, head,
                        {"identical_price": r["identical_px"], "symbols_at_price": r["identical_syms"],
                         "test_symbols": ", ".join(r["test_syms"]) or "none"},
                        symbols=[s for s in r["identical_list"] if isinstance(s, str)],
                        samples=fr.w_samples.get(S.TEST_DATA_LEAK, []) or fr.w_samples.get(S.PRICE_SPIKE, [])))
        cs = r["cons"]
        spike_ev = None
        if spikes:
            e = max(spikes, key=lambda x: abs(x["ratio"] - 1))
            spike_ev = {"symbol": e["sym"], "price": round(e["price"], 4), "consensus": round(e["ref"], 4),
                        "dev_bps": round((e["ratio"] - 1) * 1e4, 1)}
        if cs and cs["spikes"] and (spike_ev is None or abs(cs["spike_ev"]["dev_bps"]) > abs(spike_ev["dev_bps"])):
            spike_ev = cs["spike_ev"]
        n_spikes = len(spikes) + (cs["spikes"] if cs else 0)
        if spike_ev is not None and is_open:
            add(Finding(S.PRICE_SPIKE, S.DEGRADED, 0.6,
                        f"{spike_ev['symbol']} printed {spike_ev['price']:g} vs consensus "
                        f"{spike_ev['consensus']:g} ({spike_ev['dev_bps']:+.0f} bps)",
                        {"max_dev_bps": spike_ev["dev_bps"], "symbol": spike_ev["symbol"],
                         "price": spike_ev["price"], "consensus": spike_ev["consensus"], "spikes": n_spikes},
                        symbols=[spike_ev["symbol"]], samples=fr.w_samples.get(S.PRICE_SPIKE, [])))

        # ---- L3 rate rules
        n = r["msgs"]
        if "L5" in self.layers and r["peer_msgs3"] is not None:
            ratio = (r["msgs3"] + 1.0) / (r["peer_msgs3"] + 1.0)
            storm = ratio >= cfg.storm_ratio
        else:
            ratio = (r["msgs3"] + 1.0) / (r["windows3"] * (fr.ewma_msgs or n) + 1.0)
            storm = ratio >= cfg.storm_ratio_self
        # the 2013 signature: excess traffic made of replays, unknown symbols or malformed messages
        corroborated = (r["dups"] + r["unknown_syms"] + r["garbled"]) >= 0.05 * max(1, n)
        if storm and corroborated and n >= cfg.storm_min_msgs:
            add(Finding(S.RATE_STORM, S.CRITICAL, 1.0,
                        f"{n} msgs in 1 s = {ratio:.1f}x normal ({r['dups']} replays, "
                        f"{r['unknown_syms']} unknown symbols)",
                        {"rate_ratio": round(ratio, 1), "msg_rate": n,
                         "peer_rate": None if r["peer_msgs"] is None else round(r["peer_msgs"], 1),
                         "unknown_symbols": r["unknown_syms"],
                         "dup_rate_pct": round(100 * r["dups"] / max(1, n), 1)},
                        samples=fr.w_samples.get(S.RATE_STORM, []) or fr.w_samples.get(S.DUPLICATE, [])))

        # ---- staleness / consensus
        if is_open:
            if cs is not None:
                if cs["stale"]:
                    syms = cs["stale"]
                    all_stale = len(syms) >= max(1, min(3, cs["active_symbols"]))
                    add(Finding(S.STALE, S.CRITICAL if all_stale else S.DEGRADED, 1.0 if all_stale else 0.6,
                                f"no updates for {', '.join(syms)} for {cs['stale_for']:.1f} s while other feeds updated",
                                {"silent_for_s": round(cs["stale_for"], 1), "stale_symbols": len(syms),
                                 "symbols": ", ".join(syms), "peer_msgs": r["peer_data"]},
                                symbols=syms))
                if cs["frozen"]:
                    syms = cs["frozen"]
                    crit = len(syms) >= 2 or cs["frozen_for"] >= 3.0
                    add(Finding(S.FROZEN, S.CRITICAL if crit else S.DEGRADED, 1.0 if crit else 0.7,
                                f"{len(syms)} symbol(s) flat for {cs['frozen_for']:.1f} s while other feeds "
                                f"moved {cs['frozen_changes']} times",
                                {"frozen_symbols": len(syms), "symbols": ", ".join(syms),
                                 "frozen_for_s": round(cs["frozen_for"], 1), "peer_changes": cs["frozen_changes"],
                                 "heartbeat_age_s": round(r["hb_age_s"], 1)},
                                symbols=syms, samples=[_jsonable(fr.w_last_quote[x]) for x in syms
                                                       if x in fr.w_last_quote][:3]))
                if cs["drift"]:
                    s_, bias, cus = max(cs["drift"], key=lambda x: abs(x[1]))
                    add(Finding(S.DRIFT, S.CRITICAL if abs(bias) >= 50 else S.DEGRADED,
                                min(1.0, 0.4 + abs(bias) / 100),
                                f"{s_} biased {bias:+.0f} bps vs consensus and persisting",
                                {"bias_bps": bias, "symbol": s_, "cusum": cus,
                                 "drift_symbols": len(cs["drift"])},
                                symbols=[x[0] for x in cs["drift"]]))
            elif r["silent_s"] >= cfg.stale_self_s:
                add(Finding(S.STALE, S.CRITICAL, 1.0,
                            f"no data for {r['silent_s']:.1f} s while heartbeats continue",
                            {"silent_for_s": round(r["silent_s"], 1), "peer_msgs": r["peer_data"]}))

        # ---- L4 AI (soft evidence: can only make the feed DEGRADED)
        mv = fr.ml_view
        if mv is not None and sum(fr.ml_flags) >= cfg.ml_persist and mv["flag"]:
            top = mv["top_features"][0]["label"] if mv["top_features"] else "combination of features"
            diag = mv["diagnosis"]
            add(Finding(S.ML_ANOMALY, S.DEGRADED, min(1.0, 0.4 + (mv["score"] - mv["threshold"]) * 4),
                        f"behaviour outside the learned normal range (score {mv['score']:.2f} > "
                        f"{mv['threshold']:.2f}); top signal: {top}",
                        {"score": mv["score"], "threshold": mv["threshold"], "top_feature": top,
                         "diagnosis": f"{diag['label']} ({diag['p']:.0%})" if diag else "n/a"},
                        hard=False))
        return out

    # ------------------------------------------------------------------ outputs
    def _best_feed_name(self, exclude: str | None = None) -> str:
        best = None
        for f in self.feed_ids:
            if f == exclude:
                continue
            fr = self.feeds[f]
            key = (S.STATE_RANK[fr.health.state], -round(fr.health.trust, 1),
                   fr.base_p50 if fr.base_p50 is not None else 1e9)
            if best is None or key < best[0]:
                best = (key, fr)
        if best is None:
            return "another feed"
        return f"{best[1].id} ({best[1].name})"

    def _recommend(self) -> None:
        for s in self.symbols:
            best = None
            for f in self.feed_ids:
                fr = self.feeds[f]
                if fr.health.state == S.CRITICAL or s in fr.bad_symbols or fr.health.trust < 0.5:
                    continue
                key = (S.STATE_RANK[fr.health.state], fr.base_p50 if fr.base_p50 is not None else 1e9)
                if best is None or key < best[0]:
                    best = (key, f)
            if best is None:
                best = (None, max(self.feed_ids, key=lambda f: self.feeds[f].health.trust))
            self.recommended[s] = best[1]

    def _sample_chart(self, t: int) -> None:
        self.chart_t.append(t // MS)
        for s in self.symbols:
            v = self.chart_v[s]
            c = self.cons.cons.get(s) if "L5" in self.layers else None
            v["consensus"].append(None if c is None else round(c, 4))
            for f in self.feed_ids:
                q = self.feeds[f].quotes.get(s)
                v[f].append(None if q is None else round((q[0] + q[1]) / 2.0, 4))

    def chart_view(self, sym: str) -> dict:
        if sym not in self.chart_v:
            sym = self.symbols[0]
        t = list(self.chart_t)
        t0 = t[0] if t else 0
        v = self.chart_v[sym]
        return {"symbol": sym, "t_ms": t,
                "series": {k: list(dq)[-len(t):] if t else [] for k, dq in v.items()},
                "markers": [m for m in self.markers if m["t_ms"] >= t0]}

    def feed_views(self) -> list[dict]:
        out = []
        for f in self.feed_ids:
            fr = self.feeds[f]
            h = fr.health
            r = fr.last_raw or {}
            mv = fr.ml_view
            out.append({
                "id": f, "name": fr.name, "role": fr.role, "health": round(h.health, 1), "state": h.state,
                "trust": round(h.trust, 2), "msg_rate": float(r.get("data", 0)),
                "lat_p50_ms": None if r.get("lat_p50") is None else round(r["lat_p50"], 2),
                "lat_p99_ms": None if r.get("lat_p99") is None else round(r["lat_p99"], 2),
                "gaps": fr.seq.total_missing, "dups": fr.seq.total_dups,
                "quarantined": fr.total_quarantined, "headline": h.headline(), "codes": h.codes(),
                "ml": None if mv is None else {"score": mv["score"], "threshold": mv["threshold"],
                                               "flag": bool(mv["flag"])},
                "diagnosis": None if mv is None or mv["diagnosis"] is None else
                {"label": mv["diagnosis"]["label"], "p": mv["diagnosis"]["p"]},
                "history": list(h.history),
            })
        return out

    def quarantine_view(self, feed: str | None = None, limit: int = 50) -> list[dict]:
        rows = []
        for f in self.feed_ids:
            if feed and f != feed:
                continue
            rows.extend(self.feeds[f].quarantine)
        rows.sort(key=lambda r: r["t_ms"], reverse=True)
        return rows[:limit]


class OnlineAnomaly:
    """Live mode: learn this session's normal behaviour from the first healthy windows, then score.

    The replay-trained model describes Nasdaq replay feeds; live crypto venues behave differently,
    so in live mode an IsolationForest is fitted on the venues' own first minutes (in a background
    thread) and its threshold set at the same percentile as the replay model.
    """

    def __init__(self, cfg: Config, warmup: int = 240):
        self.cfg = cfg
        self.warmup = warmup
        self.rows: list = []
        self.model = None
        self.threshold = float("inf")
        self.med = None
        self.scale = None
        self._fitting = False

    @property
    def ready(self) -> bool:
        return self.model is not None

    def observe(self, X: np.ndarray, states: list[str]) -> None:
        if self.model is not None or self._fitting:
            return
        for i, st in enumerate(states):
            if st == S.HEALTHY:
                self.rows.append(X[i].copy())
        if len(self.rows) >= self.warmup:
            self._fitting = True
            threading.Thread(target=self._fit, daemon=True).start()

    def _fit(self) -> None:
        from sklearn.ensemble import IsolationForest

        from .ml import robust_scale
        Xtr = np.vstack(self.rows)
        iso = IsolationForest(n_estimators=100, random_state=0).fit(Xtr)
        sc = -iso.score_samples(Xtr)
        self.threshold = float(np.percentile(sc, self.cfg.ml_percentile))
        self.med, self.scale = robust_scale(Xtr)
        self.model = iso
        log.info("live anomaly model fitted on %d healthy windows (threshold %.3f)", len(Xtr), self.threshold)

    def scores(self, X: np.ndarray) -> np.ndarray:
        return -self.model.score_samples(X)

    def top_features(self, x: np.ndarray, k: int = 3) -> list[dict]:
        z = (x - self.med) / self.scale
        order = np.argsort(-np.abs(z))[:k]
        return [{"name": FX.FEATURES[i], "label": FX.FEATURE_TEXT[FX.FEATURES[i]],
                 "value": round(float(x[i]), 3), "baseline": round(float(self.med[i]), 3),
                 "z": round(float(np.clip(z[i], -99, 99)), 1)} for i in order if abs(z[i]) >= 1.0]
