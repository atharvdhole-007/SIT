"""Incident Timeline: an ordered, per-feed event log built from the engine's window output.

Entries on: state transitions, first occurrence of each fingerprint (repeats collapse into
"Sequence gap x14 in 8 s"), incident open/close, recommendation changes and operator acks.
"""
from __future__ import annotations

import itertools
from collections import deque

from .. import schema as S
from ..config import MS, NS
from ..timeutil import clock

ICONS = {"ok": "🟢", "warn": "🟡", "minor": "🟠", "major": "🔴", "incident": "🚨", "action": "🔄", "info": "🔵"}
LABELS = {
    S.GAP: "Sequence gap detected", S.DELAY: "Latency increasing", S.DUPLICATE: "Duplicate burst detected",
    S.CONFLICTING_DUPLICATE: "Conflicting duplicates detected", S.OUT_OF_ORDER: "Out-of-order delivery",
    S.SEQ_RESET: "Sequence reset", S.GARBLED: "Garbled messages quarantined", S.FROZEN: "Prices frozen",
    S.STALE: "Feed stopped updating", S.DISCONNECT: "Feed disconnected", S.PRICE_SPIKE: "Price spike vs consensus",
    S.DECIMAL_SHIFT: "Decimal shift detected", S.DRIFT: "Prices drifting from consensus",
    S.TEST_DATA_LEAK: "Test data detected", S.RATE_STORM: "Message-rate storm", S.CLOCK_SKEW: "Clock skew detected",
    S.ML_ANOMALY: "Unusual behaviour (AI)",
}


class Timeline:
    def __init__(self, tz, maxlen: int = 2000):
        self.tz = tz
        self.entries: deque = deque(maxlen=maxlen)
        self._ids = itertools.count(1)
        self._state: dict[str, str] = {}
        self._active: dict[tuple, dict] = {}      # (feed, code) -> entry being collapsed into
        self._seen_incidents = 0
        self._open_ids: set = set()
        self._recommended: dict = {}

    def add(self, t_ns: int, level: str, text: str, feed=None, feed_name=None, incident_id=None) -> dict:
        # collapse an identical line for the same feed within 30 s ("Feed critical x3")
        for prev in reversed(self.entries):
            if prev["feed"] == feed:
                if prev.get("_base") == text and incident_id is None and t_ns - prev["_t_ns"] <= 30 * NS:
                    prev["count"] += 1
                    prev["text"] = f"{text} x{prev['count']}"
                    return prev
                break
        e = {"_base": text, "_t_ns": t_ns, "id": next(self._ids), "t_ms": t_ns // MS, "clock": clock(t_ns, self.tz), "feed": feed,
             "feed_name": feed_name, "level": level, "icon": ICONS[level], "text": text, "count": 1,
             "incident_id": incident_id}
        self.entries.append(e)
        return e

    def on_window(self, engine, end: int) -> None:
        for f in engine.feed_ids:
            fr = engine.feeds[f]
            st = fr.health.state
            prev = self._state.get(f)
            codes = {fd.code: fd for fd in fr.health.findings}
            for code, fd in codes.items():
                key = (f, code)
                act = self._active.get(key)
                if act is not None and end - act["_t0"] <= 30 * NS:
                    act["count"] += 1
                    secs = max(1, int((end - act["_t0"]) / NS))
                    act["text"] = f"{LABELS.get(code, code)} x{act['count']} in {secs} s"
                    continue
                level = "warn" if code == S.DELAY else ("major" if fd.severity == S.CRITICAL else "minor")
                e = self.add(end, level, LABELS.get(code, code), f, fr.name)
                e["_t0"] = end
                self._active[key] = e
            if prev is not None and st != prev:
                if st == S.HEALTHY:
                    self.add(end, "ok", "Feed recovered", f, fr.name)
                    for k in [k for k in self._active if k[0] == f]:
                        self._active.pop(k)
                elif st == S.CRITICAL:
                    self.add(end, "major", "Feed critical", f, fr.name)
                elif prev == S.HEALTHY:
                    self.add(end, "warn", "Feed degraded", f, fr.name)
            elif prev is None:
                self.add(end, "ok", "Feed healthy", f, fr.name)
            self._state[f] = st
        incs = engine.incidents.incidents
        for inc in incs[self._seen_incidents:]:
            self.add(inc.opened_ns, "incident", f"Incident created: {inc.id} {inc.code} ({inc.severity})",
                     inc.feed, inc.feed_name, inc.id)
            self._open_ids.add(inc.id)
        self._seen_incidents = len(incs)
        for iid in list(self._open_ids):
            inc = engine.incidents.get(iid)
            if inc is not None and inc.status != "OPEN":
                self._open_ids.discard(iid)
                txt = f"Incident {iid} merged into {inc.merged_into}" if inc.merged_into else f"Incident {iid} resolved"
                self.add(end, "info", txt, inc.feed, inc.feed_name, iid)
        rec = dict(engine.recommended)
        if self._recommended:
            changed: dict[str, list] = {}
            for s, f in rec.items():
                if self._recommended.get(s) != f:
                    changed.setdefault(f, []).append(s)
            for f, syms in changed.items():
                name = engine.feeds[f].name
                self.add(end, "action", f"Alternate feed recommended: {f} ({name}) for {', '.join(syms)}", f, name)
        self._recommended = rec

    def on_ack(self, t_ns: int, inc, user: str) -> None:
        self.add(t_ns, "info", f"{inc.id} acknowledged by {user}", inc.feed, inc.feed_name, inc.id)

    def view(self, limit: int = 60, feed: str | None = None, feeds_allowed=None) -> list[dict]:
        out = []
        for e in reversed(self.entries):
            if feed and e["feed"] != feed:
                continue
            if feeds_allowed is not None and e["feed"] is not None and e["feed"] not in feeds_allowed:
                continue
            out.append({k: v for k, v in e.items() if not k.startswith("_")})
            if len(out) >= limit:
                break
        out.reverse()
        return out
