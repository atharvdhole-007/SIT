"""L2 sequence & timing, using the same logic as a MoldUDP64 receiver.

Tracks the next expected sequence number per feed session. A missing number opens a gap; if it
arrives within the fill window it was reordering, otherwise it is confirmed lost (a later arrival is
a recovered retransmission). A number seen before is a duplicate: benign if the content matches,
corruption if it does not. An unexplained backwards jump is a sequence reset. Heartbeats carry the
next sequence number, so loss is detected even when no data follows.
"""
from __future__ import annotations

from ..config import MS, NS, Config

OK, DUP, CONFLICT, REORDER, LATE, RESET, NEW_SESSION = "ok", "dup", "conflict", "reorder", "late", "reset", "new"


def content_hash(msg: dict) -> int:
    g = msg.get
    key = (g("type"), g("sym"), g("bid"), g("bid_sz"), g("ask"), g("ask_sz"), g("px"), g("sz"),
           g("exch_ts"), g("event"), g("state"))
    try:
        return hash(key)
    except TypeError:
        return hash(repr(key))


class Sequencer:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.fill_ns = int(cfg.fill_window_ms * MS)
        self.lost_ns = int(cfg.lost_memory_s * NS)
        self.dup_ns = int(cfg.dup_memory_s * NS)
        self.mask = cfg.ring_size - 1
        self.ring_seq = [-1] * cfg.ring_size
        self.ring_hash = [0] * cfg.ring_size
        self.ring_t = [0] * cfg.ring_size
        self.session: str | None = None
        self.expected: int | None = None
        self.pending: list[list[int]] = []   # [first, last, opened_ns]
        self.lost: list[list[int]] = []      # [first, last, confirmed_ns]
        self.sessions_seen = 0
        self.total_missing = 0
        self.total_dups = 0
        self.total_conflicts = 0
        self.reset_window()

    def reset_window(self) -> None:
        self.w_missing = 0
        self.w_gap_first = None
        self.w_gap_last = None
        self.w_dups = 0
        self.w_conflicts = 0
        self.w_reorders = 0
        self.w_late = 0
        self.w_resets = 0
        self.w_reset_from = None
        self.w_reset_to = None
        self.w_conflict_seq = None

    # ------------------------------------------------------------------ helpers
    def _new_session(self, session: str) -> None:
        self.session = session
        self.expected = None
        self.pending.clear()
        self.lost.clear()
        self.sessions_seen += 1

    def _store(self, seq: int, h: int, now: int) -> None:
        i = seq & self.mask
        self.ring_seq[i] = seq
        self.ring_hash[i] = h
        self.ring_t[i] = now

    def _open_gap(self, first: int, last: int, now: int) -> None:
        if last >= first:
            self.pending.append([first, last, now])
            if len(self.pending) > 5000:        # pathological: confirm the oldest at once
                self.confirm(now + self.fill_ns + 1)

    def _fill(self, seq: int, now: int) -> str | None:
        for lst, status in ((self.pending, None), (self.lost, LATE)):
            for k, r in enumerate(lst):
                if r[0] <= seq <= r[1]:
                    first, last, t0 = r
                    parts = []
                    if first <= seq - 1:
                        parts.append([first, seq - 1, t0])
                    if seq + 1 <= last:
                        parts.append([seq + 1, last, t0])
                    lst[k:k + 1] = parts
                    if status is None:
                        return REORDER if now - t0 <= self.fill_ns else LATE
                    return status
        return None

    def _reset(self, from_seq: int, to_seq: int) -> None:
        self.w_resets += 1
        self.w_reset_from = from_seq
        self.w_reset_to = to_seq
        self.pending.clear()
        self.lost.clear()

    # ------------------------------------------------------------------ inputs
    def on_heartbeat(self, session: str, next_seq: int, now: int) -> None:
        if session != self.session:
            self._new_session(session)
        if self.expected is None:
            self.expected = next_seq
        elif next_seq > self.expected:
            self._open_gap(self.expected, next_seq - 1, now)
            self.expected = next_seq
        elif self.expected - next_seq > self.cfg.reset_backjump:
            self._reset(self.expected - 1, next_seq)
            self.expected = next_seq

    def on_message(self, session: str, seq: int, h: int, now: int) -> str:
        if session != self.session:
            self._new_session(session)
            self.expected = seq + 1
            self._store(seq, h, now)
            return NEW_SESSION
        exp = self.expected
        if exp is None:
            self.expected = seq + 1
            self._store(seq, h, now)
            return OK
        if seq == exp:
            self.expected = exp + 1
            self._store(seq, h, now)
            return OK
        if seq > exp:
            self._open_gap(exp, seq - 1, now)
            self.expected = seq + 1
            self._store(seq, h, now)
            return OK
        # seq < expected: duplicate, late fill, or reset
        i = seq & self.mask
        if self.ring_seq[i] == seq and now - self.ring_t[i] <= self.dup_ns:
            if self.ring_hash[i] == h:
                self.w_dups += 1
                self.total_dups += 1
                return DUP
            self.w_conflicts += 1
            self.total_conflicts += 1
            self.w_conflict_seq = seq
            return CONFLICT
        st = self._fill(seq, now)
        if st is not None:
            if st == REORDER:
                self.w_reorders += 1
            else:
                self.w_late += 1
            self._store(seq, h, now)
            return st
        if exp - seq > self.cfg.reset_backjump or seq <= 16:
            self._reset(exp - 1, seq)
            self.expected = seq + 1
            self._store(seq, h, now)
            return RESET
        # an old duplicate we no longer remember
        self.w_dups += 1
        self.total_dups += 1
        return DUP

    def confirm(self, now: int) -> None:
        """Pending gaps older than the fill window become confirmed losses."""
        if self.pending:
            keep = []
            for r in self.pending:
                if now - r[2] > self.fill_ns:
                    n = r[1] - r[0] + 1
                    self.w_missing += n
                    self.total_missing += n
                    self.w_gap_first = r[0] if self.w_gap_first is None else min(self.w_gap_first, r[0])
                    self.w_gap_last = r[1] if self.w_gap_last is None else max(self.w_gap_last, r[1])
                    self.lost.append([r[0], r[1], now])
                else:
                    keep.append(r)
            self.pending = keep
        if self.lost and now - self.lost[0][2] > self.lost_ns:
            self.lost = [r for r in self.lost if now - r[2] <= self.lost_ns]
