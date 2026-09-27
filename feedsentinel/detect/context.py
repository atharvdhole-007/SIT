"""L0 market context: quiet is not broken.

A halted symbol, a closed market, or every feed going quiet at once is the market, not a fault.
Session and halt state change only when a majority of feeds agree, so one bad feed cannot switch
the alarms off. The LULD-style reference price per symbol is maintained from cross-feed consensus.
"""
from __future__ import annotations

from collections import deque

from .. import schema as S
from ..config import NS, Config

OPEN, PRE, CLOSED = "OPEN", "PRE", "CLOSED"


class MarketContext:
    def __init__(self, cfg: Config, feed_ids, symbols, open_ns: int | None = None,
                 close_ns: int | None = None, always_open: bool = False):
        self.cfg = cfg
        self.feed_ids = list(feed_ids)
        self.majority = len(self.feed_ids) // 2 + 1
        self.open_ns = open_ns
        self.close_ns = close_ns
        self.always_open = always_open
        self.session = OPEN if always_open else PRE
        self._session_votes: dict[str, set] = {}
        self.trading_state = {s: S.TRADING for s in symbols}
        self._halt_votes: dict[tuple, set] = {}
        self._ref: dict[str, deque] = {s: deque(maxlen=cfg.band_ref_s) for s in symbols}
        self._ref_sum: dict[str, float] = {s: 0.0 for s in symbols}

    # ------------------------------------------------------------------ session
    def init_time(self, t: int) -> None:
        """Joining mid-session (replay starting at 10:30): infer the session from the clock."""
        if self.always_open or self.open_ns is None:
            return
        if self.open_ns <= t < (self.close_ns or 1 << 62):
            self.session = OPEN
        elif self.close_ns is not None and t >= self.close_ns:
            self.session = CLOSED

    def on_system(self, feed: str, event: str) -> None:
        votes = self._session_votes.setdefault(event, set())
        votes.add(feed)
        if len(votes) >= self.majority:
            if event == "Q":
                self.session = OPEN
            elif event in ("M", "E", "C"):
                self.session = CLOSED
            self._session_votes.pop(event, None)

    def on_trading_action(self, feed: str, sym: str, state: str) -> None:
        key = (sym, state)
        votes = self._halt_votes.setdefault(key, set())
        votes.add(feed)
        if len(votes) >= self.majority and sym in self.trading_state:
            self.trading_state[sym] = state
            for k in [k for k in self._halt_votes if k[0] == sym]:
                self._halt_votes.pop(k, None)

    def is_open(self) -> bool:
        return self.session == OPEN

    def is_halted(self, sym: str) -> bool:
        return self.trading_state.get(sym, S.TRADING) != S.TRADING

    def halted_symbols(self) -> list[str]:
        return [s for s, st in self.trading_state.items() if st != S.TRADING]

    # ------------------------------------------------------------------ LULD-style reference
    def band_pct(self, t: int) -> float:
        """Bands double in the first 15 and last 25 minutes of the session, as LULD does."""
        cfg = self.cfg
        if self.open_ns is not None and t < self.open_ns + 15 * 60 * NS:
            return cfg.band_pct_edge
        if self.close_ns is not None and t > self.close_ns - 25 * 60 * NS:
            return cfg.band_pct_edge
        return cfg.band_pct

    def update_ref(self, sym: str, price: float) -> None:
        dq = self._ref.get(sym)
        if dq is None:
            return
        if len(dq) == dq.maxlen:
            self._ref_sum[sym] -= dq[0]
        dq.append(price)
        self._ref_sum[sym] += price

    def ref_price(self, sym: str) -> float | None:
        dq = self._ref.get(sym)
        if not dq or len(dq) < self.cfg.band_min_ref_samples:
            return None
        return self._ref_sum[sym] / len(dq)

    def reanchor(self, sym: str, price: float) -> None:
        """A move every feed agrees on is the market: restart the reference at the new level."""
        dq = self._ref.get(sym)
        if dq is None:
            return
        dq.clear()
        for _ in range(self.cfg.band_min_ref_samples):
            dq.append(price)
        self._ref_sum[sym] = price * len(dq)
