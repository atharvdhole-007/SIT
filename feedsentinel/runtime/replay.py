"""Replay session: LOBSTER tape -> three simulated feeds (+ Chaos Lab) -> detection engine.

Market time advances in small steps. The server paces steps against the wall clock (1x-20x); the
evaluator runs them as fast as the CPU allows. The engine code is identical in both.
"""
from __future__ import annotations

import logging
import time

from ..config import NS, REPLAY, Config
from ..data.lobster import SOURCE_LABEL, Tape
from ..sim.chaos import SCENARIOS, ChaosLab
from ..sim.feeds import REPLAY_FEEDS, FeedSimulator
from ..timeutil import NEW_YORK, parse_clock
from ..detect.engine import Engine

log = logging.getLogger("feedsentinel.replay")

MATCH_GRACE_S = 5.0


class ReplaySession:
    mode = "replay"
    source = SOURCE_LABEL
    tz = NEW_YORK

    def __init__(self, tape: Tape, refdata, cfg: Config = REPLAY, models=None, explainer=None,
                 start: str = "10:30", warmup_s: float = 120.0, end: str | None = None, seed: int = 1,
                 chaos_seed: int = 7, record_windows: bool = False, chart: bool = True,
                 session: str | None = None):
        self.tape = tape
        self.cfg = cfg
        self.symbols = list(tape.symbols)
        self.chaos = ChaosLab(tape.symbols, refdata, seed=chaos_seed)
        self.t_start = max(tape.start_ns, parse_clock(tape.day, start))
        self.t0 = max(tape.start_ns - NS // 2, self.t_start - int(warmup_s * NS))
        end_ns = parse_clock(tape.day, end) if end else None
        self.end_ns = end_ns if end_ns is not None else tape.end_ns + NS
        self.sim = FeedSimulator(tape, self.chaos, self.t0, specs=REPLAY_FEEDS, seed=seed,
                                 session=session, end_ns=self.end_ns)
        self.feeds = self.sim.info()
        self.engine = Engine(cfg, self.feeds, self.symbols, refdata, models=models, explainer=explainer,
                             tz=NEW_YORK, open_ns=parse_clock(tape.day, "09:30"),
                             close_ns=parse_clock(tape.day, "16:00"), annotate=self._annotate,
                             record_windows=record_windows, chart=chart)
        self.engine.start(self.t0)
        self.now = self.t0
        self.busy_s = 0.0

    # ------------------------------------------------------------------ running
    def step_to(self, t: int) -> int:
        """Process everything up to market time t. Returns the number of messages processed."""
        if t <= self.now:
            return 0
        c0 = time.perf_counter()
        msgs = self.sim.run_until(t)
        on = self.engine.on_message
        for m in msgs:
            on(m)
        self.engine.advance(t)
        self.now = t
        self.busy_s += time.perf_counter() - c0
        return len(msgs)

    def run_until(self, t: int, step_ms: float = 20.0) -> None:
        step = int(step_ms * 1e6)
        while self.now < t:
            self.step_to(min(t, self.now + step))

    def warmup(self) -> None:
        self.run_until(self.t_start)

    @property
    def finished(self) -> bool:
        return self.now >= self.end_ns

    # ------------------------------------------------------------------ chaos
    def inject(self, scenario: str, feed: str | None, symbols=None, params=None, duration_s=None):
        return self.chaos.inject(scenario, feed, self.now, symbols=symbols, params=params,
                                 duration_s=duration_s)

    def clear(self, fault_id: str) -> bool:
        return self.chaos.clear(fault_id, self.now)

    def clear_all(self) -> int:
        return self.chaos.clear_all(self.now)

    def _annotate(self, inc) -> None:
        """Attach the injected fault an incident corresponds to (ground truth -> time to detect).
        The detectors never see this; it is for display and evaluation only."""
        grace = int(MATCH_GRACE_S * NS)
        best = None
        for f in reversed(self.chaos.faults):
            if f.feed != inc.feed or f.start_ns > inc.opened_ns:
                continue
            if f.ended_ns is not None and f.ended_ns + grace < inc.opened_ns:
                continue
            exact = inc.code in SCENARIOS[f.scenario].expected
            if best is None or (exact and not best[0]):
                best = (exact, f)
            if exact:
                break
        if best is None:
            return
        f = best[1]
        inc.fault = {"id": f.id, "scenario": f.scenario, "label": f.spec.label,
                     "started_ms": f.start_ns // 1_000_000, "expected": list(f.spec.expected),
                     "exact_match": best[0]}
        inc.ttd_s = round((inc.opened_ns - f.start_ns) / NS, 2)
