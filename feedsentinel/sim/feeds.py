"""Three redundant feeds from one real source: Direct (fast), SIP (slower, correct), Vendor.

A discrete-event simulation on market time. Every source event is published on every feed with its
own MoldUDP64-style sequence number, delivered after a feed-specific latency (lognormal with rare
queueing microbursts), in FIFO order per feed. Each feed also sends a heartbeat every second whose
`seq` is the next sequence number, so loss is visible even when the market is quiet.
"""
from __future__ import annotations

import heapq
import itertools
from dataclasses import dataclass

import numpy as np

from .. import schema as S
from ..config import MS, NS
from ..data.lobster import KIND_QUOTE, KIND_TRADE, PRICE_SCALE, Tape
from .chaos import ChaosLab

HALT_CODES = {-1: S.HALTED, 0: S.QUOTE_ONLY, 1: S.TRADING}


@dataclass(frozen=True)
class FeedSpec:
    id: str
    name: str
    role: str
    lat_ms: float          # median one-way latency
    lat_sigma: float       # lognormal shape
    burst_every_s: float   # mean time between queueing microbursts
    burst_ms: float        # peak extra delay during a microburst
    burst_len_ms: float

    def info(self) -> dict:
        return {"id": self.id, "name": self.name, "role": self.role}


REPLAY_FEEDS = (
    FeedSpec("A", "Direct", "Nasdaq direct feed (fastest)", 0.35, 0.35, 90.0, 3.0, 40.0),
    FeedSpec("B", "SIP", "Consolidated tape (slower, authoritative)", 22.0, 0.30, 60.0, 60.0, 120.0),
    FeedSpec("C", "Vendor", "Third-party vendor feed (Chaos Lab target)", 6.0, 0.45, 60.0, 25.0, 80.0),
)


class _Latency:
    """Lognormal latency with Poisson microbursts, sampled in blocks for speed."""

    def __init__(self, spec: FeedSpec, rng: np.random.Generator):
        self.spec = spec
        self.rng = rng
        self.buf = np.empty(0)
        self.i = 0
        self.mu = np.log(spec.lat_ms)
        self.burst_start = -1
        self.burst_end = -1
        self.next_burst = None

    def sample_ns(self, t: int) -> int:
        if self.i >= len(self.buf):
            self.buf = self.rng.lognormal(self.mu, self.spec.lat_sigma, 65536) * MS
            self.i = 0
        lat = self.buf[self.i]
        self.i += 1
        sp = self.spec
        if self.next_burst is None:
            self.next_burst = t + int(self.rng.exponential(sp.burst_every_s) * NS)
        if t >= self.next_burst:
            self.burst_start = self.next_burst
            self.burst_end = self.burst_start + int(sp.burst_len_ms * MS)
            self.next_burst = self.burst_end + int(self.rng.exponential(sp.burst_every_s) * NS)
        if self.burst_start <= t < self.burst_end:
            lat += sp.burst_ms * MS * (1.0 - (t - self.burst_start) / (self.burst_end - self.burst_start))
        return int(lat)


class FeedSimulator:
    """Replays `tape` from `start_ns` as the given feeds, pushing each message through `chaos`."""

    def __init__(self, tape: Tape, chaos: ChaosLab, start_ns: int, specs=REPLAY_FEEDS, seed: int = 1,
                 session: str | None = None, end_ns: int | None = None):
        self.tape = tape
        self.chaos = chaos
        self.specs = tuple(specs)
        self.feed_ids = [s.id for s in self.specs]
        self.i = tape.index_at(start_ns)
        self.end_i = tape.index_at(end_ns) if end_ns is not None else len(tape)
        self.end_ns = end_ns if end_ns is not None else tape.end_ns + NS
        self.clock = start_ns
        self.session = session or f"{tape.day:%Y%m%d}A0"
        self.heap: list = []
        self.count = itertools.count()
        rng = np.random.default_rng(seed)
        self.lat = {s.id: _Latency(s, np.random.default_rng(rng.integers(1 << 32))) for s in self.specs}
        self.next_seq = {f: 1 for f in self.feed_ids}
        self.fifo = {f: 0 for f in self.feed_ids}
        self.next_hb = (start_ns // NS + 1) * NS
        self.sent = {f: 0 for f in self.feed_ids}
        self.delivered = 0
        self._market_open_sent = False
        # Pre-convert tape columns to Python lists once: much faster per-event access.
        sl = slice(self.i, self.end_i)
        self._ts = tape.ts[sl].tolist()
        self._sym = [tape.symbols[k] for k in tape.sym[sl].tolist()]
        self._kind = tape.kind[sl].tolist()
        self._bid = (tape.bid[sl] / PRICE_SCALE).tolist()
        self._ask = (tape.ask[sl] / PRICE_SCALE).tolist()
        self._bsz = tape.bid_sz[sl].tolist()
        self._asz = tape.ask_sz[sl].tolist()
        self._px = tape.px[sl].tolist()
        self._sz = tape.sz[sl].tolist()
        self._side = tape.side[sl].tolist()
        self._j = 0
        self._n = self.end_i - self.i

    # ------------------------------------------------------------------ public
    @property
    def done(self) -> bool:
        return self._j >= self._n and not self.heap and self.clock >= self.end_ns

    def run_until(self, t_ns: int) -> list[dict]:
        """All messages with recv_ts <= t_ns, in delivery order."""
        self.chaos.tick(t_ns)
        ts = self._ts
        while True:
            nxt_src = ts[self._j] if self._j < self._n else None
            nxt_hb = self.next_hb if self.next_hb <= self.end_ns else None
            if nxt_src is None and nxt_hb is None:
                break
            if nxt_hb is not None and (nxt_src is None or nxt_hb <= nxt_src):
                if nxt_hb > t_ns:
                    break
                self.chaos.tick(nxt_hb)
                self._heartbeats(nxt_hb)
                self.next_hb += NS
            else:
                if nxt_src > t_ns:
                    break
                self._source_event(self._j)
                self._j += 1
        self.clock = max(self.clock, t_ns)
        out = []
        heap = self.heap
        while heap and heap[0][0] <= t_ns:
            out.append(heapq.heappop(heap)[2])
        self.delivered += len(out)
        return out

    def info(self) -> list[dict]:
        return [s.info() for s in self.specs]

    # ------------------------------------------------------------------ internals
    def _payload(self, j: int) -> dict:
        k = self._kind[j]
        if k == KIND_QUOTE:
            return {"type": S.QUOTE, "sym": self._sym[j], "bid": self._bid[j], "bid_sz": self._bsz[j],
                    "ask": self._ask[j], "ask_sz": self._asz[j]}
        if k == KIND_TRADE:
            return {"type": S.TRADE, "sym": self._sym[j], "px": self._px[j] / PRICE_SCALE,
                    "sz": self._sz[j], "side": "B" if self._side[j] > 0 else "S"}
        return {"type": S.TRADING_ACTION, "sym": self._sym[j], "state": HALT_CODES.get(self._px[j], S.HALTED)}

    def _source_event(self, j: int) -> None:
        t = self._ts[j]
        if not self._market_open_sent:
            self._market_open_sent = True
            for f in self.feed_ids:
                self._publish(f, {"type": S.SYSTEM, "event": "Q"}, t)
        for payload in self.chaos.source(self._payload(j), t):
            for f in self.feed_ids:
                self._publish(f, dict(payload), t)

    def _heartbeats(self, t: int) -> None:
        for f in self.feed_ids:
            msg = {"feed": f, "session": self.session, "type": S.HEARTBEAT, "exch_ts": t}
            for m in self.chaos.sender(f, msg, t):
                m["seq"] = self.next_seq[f]
                self._network(f, m, t)

    def _publish(self, f: str, payload: dict, t: int) -> None:
        payload["feed"] = f
        payload["session"] = self.session
        payload["exch_ts"] = t
        for m in self.chaos.sender(f, payload, t):
            if self.chaos.take_reset(f):
                self.next_seq[f] = 1
            m["seq"] = self.next_seq[f]
            self.next_seq[f] += 1
            self.sent[f] += 1
            self._network(f, m, t)

    def _network(self, f: str, m: dict, t: int) -> None:
        lat = self.lat[f]
        first = None
        for msg, extra, mode in self.chaos.network(f, m, t):
            if mode == "after" and first is not None:
                recv = first + extra
            else:
                recv = t + lat.sample_ns(t) + extra
                if mode == "fifo":
                    if recv < self.fifo[f]:
                        recv = self.fifo[f]
                    self.fifo[f] = recv
                if first is None:
                    first = recv
            msg = self.chaos.garble(f, msg)
            msg["recv_ts"] = recv
            heapq.heappush(self.heap, (recv, next(self.count), msg))

    def close_session(self, t: int) -> list[dict]:
        """Publish end-of-market (System Event M) on every feed."""
        for f in self.feed_ids:
            self._publish(f, {"type": S.SYSTEM, "event": "M"}, t)
        return self.run_until(t + NS)

