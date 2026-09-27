"""Chaos Lab: fault injection on real data. It is both the training data and the demo.

Nobody publishes labelled "this feed broke at 10:42:03" data, so FeedSentinel injects faults into
real Nasdaq-derived data. Every injection is recorded as a ground-truth episode, which gives exact
labels, repeatable demos and measurable detection.

A fault acts at one of four stages of a feed's pipeline (see sim/feeds.py):
  source   market events applied before the feed split, so every feed carries them (halt, real move)
  sender   what the feed publishes (frozen values, spikes, drift, decimal shift, test data, silence,
           clock skew, conflation); suppressed messages consume no sequence number
  network  what arrives (loss, delay, reordering, duplicates, disconnects, reconnect storms)
  garble   payload corruption after sequencing (bit errors, decoder bugs)
"""
from __future__ import annotations

import itertools
import random
from collections import deque
from dataclasses import dataclass, field

from .. import schema as S
from ..config import MS, NS


@dataclass(frozen=True)
class Scenario:
    id: str
    label: str
    group: str                 # "fault" | "incident" | "novel" | "market"
    description: str
    expected: tuple = ()       # incident codes that count as a correct detection
    cls: str | None = None     # classifier label for windows under this fault (None = not trained)
    accepts_symbols: bool = False
    defaults: dict = field(default_factory=dict)
    duration_s: float | None = None   # default auto-expiry (None = until cleared)

    def info(self) -> dict:
        return {"id": self.id, "label": self.label, "group": self.group,
                "description": self.description, "accepts_symbols": self.accepts_symbols}


SCENARIOS: dict[str, Scenario] = {s.id: s for s in [
    Scenario("packet_loss", "Packet loss 5%", "fault", "Drops 5% of packets (UDP loss on the multicast path)",
             (S.GAP,), S.GAP, defaults={"p": 0.05}),
    Scenario("delay", "Delay +800 ms", "fault", "Adds 800 ms to every message (congested path / overloaded handler)",
             (S.DELAY,), S.DELAY, defaults={"ms": 800}),
    Scenario("duplicates", "Duplicate storm", "fault", "Re-delivers 20% of messages with the same sequence number (A/B arbitration bug)",
             (S.DUPLICATE,), S.DUPLICATE, defaults={"p": 0.2}),
    Scenario("conflicting_dups", "Conflicting duplicates", "fault", "Same sequence number arrives twice with different content (corruption)",
             (S.CONFLICTING_DUPLICATE,), S.CONFLICTING_DUPLICATE, defaults={"p": 0.03}),
    Scenario("reorder", "Out-of-order", "fault", "10% of messages overtaken by later ones (multi-path delivery)",
             (S.OUT_OF_ORDER,), S.OUT_OF_ORDER, defaults={"p": 0.1, "max_ms": 40}),
    Scenario("garble", "Garbled payloads", "fault", "Corrupts 10% of payloads (8 modes: truncation, bit flips, bad types, crossed quotes, ...)",
             (S.GARBLED,), S.GARBLED, defaults={"p": 0.1}),
    Scenario("frozen", "Frozen feed", "fault", "Handler stuck: heartbeats and sequence numbers stay perfect, prices stop moving",
             (S.FROZEN,), S.FROZEN, accepts_symbols=True),
    Scenario("silence", "Silent feed", "fault", "Data stops while heartbeats continue (session alive, data dead)",
             (S.STALE,), S.STALE, accepts_symbols=True),
    Scenario("disconnect", "Disconnect", "fault", "Session drops for 8 s: no data, no heartbeats; resumes with a sequence gap",
             (S.DISCONNECT,), S.DISCONNECT, duration_s=8.0),
    Scenario("spike", "Price spikes", "fault", "Occasional fat-finger prints 1.5-12% away from the market",
             (S.PRICE_SPIKE,), S.PRICE_SPIKE, accepts_symbols=True, defaults={"p": 0.01, "lo": 0.015, "hi": 0.12}),
    Scenario("decimal_shift", "Decimal shift", "fault", "Price(4) decoded with the wrong scale: prices x10",
             (S.DECIMAL_SHIFT,), S.DECIMAL_SHIFT, accepts_symbols=True, defaults={"power": 1}),
    Scenario("drift", "Slow drift", "fault", "Prices drift away from the market: 0 -> +150 bps over 60 s (bad adjustment factor)",
             (S.DRIFT,), S.DRIFT, accepts_symbols=True, defaults={"bps": 150, "ramp_s": 60}),
    Scenario("clock_skew", "Clock skew", "fault", "Source clock runs 2 s fast (timestamps from the future)",
             (S.CLOCK_SKEW,), S.CLOCK_SKEW, defaults={"offset_ms": 2000}),
    Scenario("seq_reset", "Sequence reset", "fault", "Sequence numbers restart at 1 mid-session (handler restart)",
             (S.SEQ_RESET,), S.SEQ_RESET, duration_s=1.0),
    Scenario("test_leak_2017", "2017 test-data leak", "incident",
             "3 Jul 2017: test data reached production; AAPL, AMZN, MSFT, GOOG all printed $123.47",
             (S.TEST_DATA_LEAK,), S.TEST_DATA_LEAK),
    Scenario("reconnect_storm_2013", "2013 reconnect storm", "incident",
             "22 Aug 2013: connect/disconnect cycles flooded the SIP with replayed updates and quotes for inaccurate symbols",
             (S.RATE_STORM,), S.RATE_STORM, defaults={"interval_ms": 500, "burst": 800}),
    Scenario("throttle", "Silent throttling", "novel",
             "Vendor starts conflating updates (<= 2 per second per symbol): no gaps, no bad values. Not in the AI's training set",
             (S.ML_ANOMALY,), None, accepts_symbols=True, defaults={"interval_ms": 500}),
    Scenario("volume_corrupt", "Volume corruption", "novel",
             "Sizes multiplied by 100 (volume scale bug): prices stay right. Not in the AI's training set",
             (S.ML_ANOMALY,), None, accepts_symbols=True, defaults={"mult": 100}),
    Scenario("halt", "Trading halt", "market",
             "A real trading halt on every feed: quiet is not broken. Should NOT alert",
             (), None, accepts_symbols=True, duration_s=30.0),
    Scenario("market_move", "Real market move +3%", "market",
             "A genuine 3% move on every feed: all feeds agree, so it is the market. Should NOT alert",
             (), None, accepts_symbols=True, defaults={"pct": 3.0, "ramp_s": 2.0}),
]}

FAULT_GROUPS = ("fault", "incident", "novel")
DEFAULT_SYMBOL_FOR = {"halt": 0, "market_move": 0}   # market events default to the first symbol
GARBLE_MODES = ("truncate", "bitflip", "nonpositive", "crossed", "unknown_symbol", "off_tick",
                "bad_timestamp", "unknown_type")


def random_params(scenario: str, rng: random.Random) -> dict:
    """Randomised fault parameters for dataset generation and evaluation."""
    u = rng.uniform
    return {
        "packet_loss": lambda: {"p": u(0.01, 0.15)},
        "delay": lambda: {"ms": u(150, 2000)},
        "duplicates": lambda: {"p": u(0.03, 0.3)},
        "conflicting_dups": lambda: {"p": u(0.005, 0.05)},
        "reorder": lambda: {"p": u(0.03, 0.2), "max_ms": u(15, 60)},
        "garble": lambda: {"p": u(0.01, 0.15)},
        "spike": lambda: {"p": u(0.002, 0.01), "lo": 0.015, "hi": 0.12},
        "decimal_shift": lambda: {"power": rng.choice((1, 2, -1))},
        "drift": lambda: {"bps": u(60, 300) * rng.choice((1, -1)), "ramp_s": u(20, 90)},
        "clock_skew": lambda: {"offset_ms": u(100, 3000)},
        "reconnect_storm_2013": lambda: {"interval_ms": u(300, 800), "burst": int(u(300, 1500))},
        "throttle": lambda: {"interval_ms": u(400, 1000)},
        "volume_corrupt": lambda: {"mult": rng.choice((10, 100, 1000))},
        "market_move": lambda: {"pct": u(1.0, 4.0) * rng.choice((1, -1)), "ramp_s": u(1, 5)},
    }.get(scenario, dict)()


@dataclass
class Fault:
    id: str
    scenario: str
    feed: str | None           # None for market events (all feeds)
    symbols: tuple | None      # None = all symbols
    params: dict
    start_ns: int
    end_ns: int | None = None      # scheduled end
    ended_ns: int | None = None    # actual end
    state: dict = field(default_factory=dict)

    @property
    def spec(self) -> Scenario:
        return SCENARIOS[self.scenario]

    @property
    def active(self) -> bool:
        return self.ended_ns is None

    def applies(self, sym) -> bool:
        return self.symbols is None or sym in self.symbols

    def record(self) -> dict:
        sp = self.spec
        return {"id": self.id, "scenario": self.scenario, "label": sp.label, "group": sp.group,
                "feed": self.feed, "symbols": list(self.symbols) if self.symbols else None,
                "params": self.params, "start_ns": self.start_ns, "end_ns": self.ended_ns,
                "expected": list(sp.expected), "cls": sp.cls}


def _round_cent(x: float) -> float:
    return round(x * 100.0) / 100.0


class ChaosLab:
    def __init__(self, symbols, refdata=None, seed: int = 7):
        self.rng = random.Random(seed)
        self.symbols = tuple(symbols)
        self.faults: list[Fault] = []            # every fault ever injected (ground truth)
        self._ids = itertools.count(1)
        self._active_feed: dict[str, list[Fault]] = {}
        self._active_market: list[Fault] = []
        self._pending_source: list[dict] = []    # payloads to emit on the next source event
        self.pending_resets: set[str] = set()
        self.recent: dict[str, deque] = {}       # per-feed recently sent messages (storm replays)
        known = set(refdata.securities) if refdata is not None else set(self.symbols)
        self.test_symbols = [s for s in ("ZVZZT", "ZXZZT", "ZWZZT", "ZJZZT")
                             if refdata is None or (refdata.get(s) and refdata.get(s).test_issue)]
        self.bad_symbols = self._inaccurate_symbols(known)

    # ------------------------------------------------------------------ control
    def _inaccurate_symbols(self, known: set) -> list[str]:
        rng = random.Random(13)
        out = []
        letters = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        while len(out) < 40:
            base = rng.choice(self.symbols)
            s = list(base)
            s[rng.randrange(len(s))] = rng.choice(letters)
            if rng.random() < 0.3:
                s.append(rng.choice(letters))
            cand = "".join(s)
            if cand not in known and cand not in out:
                out.append(cand)
        return out

    def inject(self, scenario: str, feed: str | None, now_ns: int, symbols=None, params=None,
               duration_s: float | None = None) -> Fault:
        if scenario not in SCENARIOS:
            raise ValueError(f"unknown scenario {scenario!r}")
        sp = SCENARIOS[scenario]
        if sp.group == "market":
            feed = None
            if not symbols:
                symbols = (self.symbols[DEFAULT_SYMBOL_FOR.get(scenario, 0)],)
        elif feed is None:
            raise ValueError("a feed is required for fault scenarios")
        if symbols is not None:
            symbols = tuple(symbols) if sp.accepts_symbols else None
        p = dict(sp.defaults)
        p.update(params or {})
        dur = duration_s if duration_s is not None else sp.duration_s
        f = Fault(id=f"F{next(self._ids)}", scenario=scenario, feed=feed, symbols=symbols, params=p,
                  start_ns=now_ns, end_ns=None if dur is None else now_ns + int(dur * NS))
        self.faults.append(f)
        if feed is None:
            self._active_market.append(f)
            if scenario == "halt":
                for sym in f.symbols:
                    self._pending_source.append({"type": S.TRADING_ACTION, "sym": sym, "state": S.HALTED})
        else:
            self._active_feed.setdefault(feed, []).append(f)
            if scenario == "seq_reset":
                self.pending_resets.add(feed)
        return f

    def clear(self, fault_id: str, now_ns: int) -> bool:
        for f in self.faults:
            if f.id == fault_id and f.active:
                self._end(f, now_ns)
                return True
        return False

    def clear_all(self, now_ns: int) -> int:
        n = 0
        for f in list(self.active()):
            self._end(f, now_ns)
            n += 1
        return n

    def _end(self, f: Fault, now_ns: int) -> None:
        f.ended_ns = now_ns
        if f.feed is None:
            self._active_market = [x for x in self._active_market if x is not f]
            if f.scenario == "halt":
                for sym in f.symbols:
                    self._pending_source.append({"type": S.TRADING_ACTION, "sym": sym, "state": S.TRADING})
        else:
            lst = [x for x in self._active_feed.get(f.feed, []) if x is not f]
            if lst:
                self._active_feed[f.feed] = lst
            else:
                self._active_feed.pop(f.feed, None)

    def active(self) -> list[Fault]:
        return [f for f in self.faults if f.active]

    def tick(self, now_ns: int) -> None:
        for f in self.faults:
            if f.active and f.end_ns is not None and f.end_ns <= now_ns:
                self._end(f, f.end_ns)

    def has_feed_faults(self, feed: str) -> bool:
        return feed in self._active_feed

    # ------------------------------------------------------------------ source stage
    def source(self, payload: dict, t: int) -> list[dict]:
        """Market events, applied once before the split so every feed carries them."""
        out = self._pending_source
        if out:
            self._pending_source = []
        else:
            out = []
        if self._active_market:
            sym = payload.get("sym")
            for f in self._active_market:
                if not f.applies(sym):
                    continue
                if f.scenario == "halt":
                    return out            # the halted symbol does not trade or quote
                if f.scenario == "market_move":
                    ramp = max(1e-9, f.params.get("ramp_s", 2.0))
                    frac = min(1.0, (t - f.start_ns) / (ramp * NS))
                    factor = 1.0 + frac * f.params.get("pct", 3.0) / 100.0
                    payload = self._scale_prices(payload, factor, round_cent=True)
        out.append(payload)
        return out

    # ------------------------------------------------------------------ sender stage
    def sender(self, feed: str, msg: dict, t: int) -> list[dict]:
        faults = self._active_feed.get(feed)
        if not faults:
            return [msg]
        out_extra = []
        typ = msg["type"]
        sym = msg.get("sym")
        for f in faults:
            sc = f.scenario
            if sc == "clock_skew":
                msg["exch_ts"] = msg["exch_ts"] + int(f.params.get("offset_ms", 2000) * MS)
                continue
            if typ not in S.DATA_TYPES or not f.applies(sym):
                continue
            if sc == "silence":
                return []
            if sc == "throttle":
                if typ == S.QUOTE:
                    last = f.state.setdefault("last_pub", {})
                    if t - last.get(sym, -10**18) < f.params.get("interval_ms", 500) * MS:
                        return []
                    last[sym] = t
            elif sc == "frozen":
                snap = f.state.setdefault("snap", {})
                if typ == S.QUOTE:
                    q = snap.setdefault(sym, (msg["bid"], msg["bid_sz"], msg["ask"], msg["ask_sz"]))
                    msg["bid"], msg["bid_sz"], msg["ask"], msg["ask_sz"] = q
                else:
                    px, sz = snap.setdefault(("T", sym), (msg["px"], msg["sz"]))
                    msg["px"], msg["sz"] = px, sz
            elif sc == "spike":
                if self.rng.random() < f.params.get("p", 0.004):
                    mag = self.rng.uniform(f.params.get("lo", 0.015), f.params.get("hi", 0.12))
                    factor = 1.0 + mag * (1 if self.rng.random() < 0.5 else -1)
                    msg = self._scale_prices(msg, factor, round_cent=True)
            elif sc == "decimal_shift":
                msg = self._scale_prices(msg, 10.0 ** f.params.get("power", 1), round_cent=False)
            elif sc == "drift":
                ramp = max(1e-9, f.params.get("ramp_s", 60))
                frac = min(1.0, (t - f.start_ns) / (ramp * NS))
                msg = self._scale_prices(msg, 1.0 + frac * f.params.get("bps", 150) / 1e4, round_cent=True)
            elif sc == "volume_corrupt":
                mult = f.params.get("mult", 100)
                if typ == S.QUOTE:
                    msg["bid_sz"] = msg["bid_sz"] * mult
                    msg["ask_sz"] = msg["ask_sz"] * mult
                else:
                    msg["sz"] = msg["sz"] * mult
            elif sc == "test_leak_2017":
                if typ == S.QUOTE:
                    msg["bid"], msg["ask"] = 123.46, 123.48
                else:
                    msg["px"] = 123.47
                if self.test_symbols and self.rng.random() < 0.03:
                    x = dict(msg)
                    x["sym"] = self.rng.choice(self.test_symbols)
                    out_extra.append(x)
            elif sc == "reconnect_storm_2013":
                if self.rng.random() < 0.05:
                    x = dict(msg)
                    x["sym"] = self.rng.choice(self.bad_symbols)
                    out_extra.append(x)
        return [msg] + out_extra if out_extra else [msg]

    @staticmethod
    def _scale_prices(msg: dict, factor: float, round_cent: bool) -> dict:
        m = dict(msg)
        typ = m.get("type")
        if typ == S.QUOTE:
            b, a = m["bid"] * factor, m["ask"] * factor
            if round_cent:
                b, a = _round_cent(b), _round_cent(a)
                if a <= b:
                    a = round(b + 0.01, 2)
            m["bid"], m["ask"] = b, a
        elif typ == S.TRADE:
            px = m["px"] * factor
            m["px"] = _round_cent(px) if round_cent else px
        return m

    # ------------------------------------------------------------------ network stage
    def network(self, feed: str, msg: dict, t: int) -> list[tuple[dict, int, str]]:
        """Returns deliveries as (message, extra delay ns, mode). Modes:
        "fifo"  normal in-order delivery, extra delay added to the latency (keeps feed order)
        "free"  delivered at send time + latency + extra, may overtake or be overtaken (reordering)
        "after" delivered `extra` after the first delivery of this message (duplicates, replays)"""
        rec = self.recent.get(feed)
        if rec is None:
            rec = self.recent[feed] = deque(maxlen=3000)
        if msg["type"] != S.HEARTBEAT:
            rec.append(msg)
        faults = self._active_feed.get(feed)
        if not faults:
            return [(msg, 0, "fifo")]
        rng = self.rng
        out = [(msg, 0, "fifo")]
        for f in faults:
            sc = f.scenario
            p = f.params
            if sc == "disconnect":
                return []
            if sc == "packet_loss":
                if rng.random() < p.get("p", 0.05):
                    return []
            elif sc == "delay":
                ms = p.get("ms", 800)
                out = [(m, d + int(ms * rng.uniform(0.95, 1.05) * MS), mode) for m, d, mode in out]
            elif msg["type"] == S.HEARTBEAT:
                continue
            elif sc == "reorder":
                if rng.random() < p.get("p", 0.1):
                    out = [(m, d + int(rng.uniform(3, p.get("max_ms", 40)) * MS), "free" if mode == "fifo" else mode)
                           for m, d, mode in out]
            elif sc == "duplicates":
                if rng.random() < p.get("p", 0.2):
                    out.append((dict(msg), int(rng.uniform(0.05, 3.0) * MS), "after"))
            elif sc == "conflicting_dups":
                if rng.random() < p.get("p", 0.03):
                    out.append((self._alter(msg), int(rng.uniform(0.5, 3.0) * MS), "after"))
            elif sc == "reconnect_storm_2013":
                nxt = f.state.get("next_burst", f.start_ns)
                if t >= nxt:
                    f.state["next_burst"] = t + int(p.get("interval_ms", 500) * MS)
                    burst = list(rec)[-int(p.get("burst", 800)):]
                    span = 60.0 / max(1, len(burst))
                    for k, old in enumerate(burst):
                        out.append((dict(old), int((k * span + 0.01) * MS), "after"))
        return out

    def _alter(self, msg: dict) -> dict:
        m = dict(msg)
        if m["type"] == S.QUOTE:
            if self.rng.random() < 0.5:
                m["bid_sz"] = m["bid_sz"] + self.rng.choice((1, 100, 200))
            else:
                m["bid"] = round(m["bid"] - 0.01, 2)
        elif m["type"] == S.TRADE:
            m["px"] = round(m["px"] + 0.01, 2)
        return m

    # ------------------------------------------------------------------ garble stage
    def garble(self, feed: str, msg: dict) -> dict:
        faults = self._active_feed.get(feed)
        if not faults or msg["type"] not in S.DATA_TYPES:
            return msg
        for f in faults:
            if f.scenario == "garble" and self.rng.random() < f.params.get("p", 0.1):
                mode = f.params.get("mode") or self.rng.choice(GARBLE_MODES)
                return self.corrupt(msg, mode)
        return msg

    def corrupt(self, msg: dict, mode: str) -> dict:
        m = dict(msg)
        rng = self.rng
        is_q = m["type"] == S.QUOTE
        if mode in ("crossed", "off_tick") and not is_q:
            mode = "bitflip"
        if mode == "truncate":
            for k in (("ask", "ask_sz") if is_q else ("sz",)):
                m.pop(k, None)
        elif mode == "bitflip":
            key = "bid" if is_q else "px"
            s = f"{m[key]:.2f}"
            i = rng.randrange(len(s))
            m[key] = s[:i] + rng.choice("#?*%@") + s[i + 1:]
        elif mode == "nonpositive":
            m["bid" if is_q else "px"] = rng.choice((0.0, -m["bid" if is_q else "px"]))
        elif mode == "crossed":
            m["bid"], m["ask"] = m["ask"] + 0.01, m["bid"]
        elif mode == "unknown_symbol":
            m["sym"] = rng.choice(self.bad_symbols)
        elif mode == "off_tick":
            m["bid"] = m["bid"] + 0.003
        elif mode == "bad_timestamp":
            m["exch_ts"] = rng.choice((0, -1, m["exch_ts"] * 1000, "2012-06-21T10:00"))
        elif mode == "unknown_type":
            m["type"] = rng.choice(("X", "?", "q"))
        return m

    # ------------------------------------------------------------------ status
    def take_reset(self, feed: str) -> bool:
        if feed in self.pending_resets:
            self.pending_resets.discard(feed)
            return True
        return False

    def active_view(self, now_ns: int) -> list[dict]:
        out = []
        for f in self.active():
            sp = f.spec
            out.append({"id": f.id, "scenario": f.scenario, "label": sp.label, "group": sp.group,
                        "feed": f.feed, "symbols": list(f.symbols) if f.symbols else None,
                        "started_ms": f.start_ns // MS, "age_s": round(max(0, now_ns - f.start_ns) / NS, 1)})
        return out

    def episodes(self) -> list[dict]:
        return [f.record() for f in self.faults]


def scenario_list() -> list[dict]:
    return [s.info() for s in SCENARIOS.values()]


def expected_codes(scenario: str) -> tuple:
    return SCENARIOS[scenario].expected

