"""L7 incidents: one card per feed and fingerprint, not 500 alerts.

An incident opens when a finding first appears on a feed, accumulates evidence while the finding
keeps recurring, escalates if the severity rises, and resolves after `incident_close_s` without new
evidence. Each carries the evidence, sample bad messages, the recommended runbook action, a timeline,
and a three-line explanation (template now, grounded LLM text later if enabled).
"""
from __future__ import annotations

import itertools
import queue
import string
from dataclasses import dataclass, field

from .. import schema as S
from ..config import MS, NS, Config
from ..timeutil import clock
from .health import Finding


class _SafeDict(dict):
    def __missing__(self, key):
        return "?"


def format_action(code: str, evidence: dict, best_feed: str) -> str:
    tmpl = S.CODE_INFO[code]["action"]
    vals = _SafeDict({k: v for k, v in evidence.items()})
    vals["best_feed"] = best_feed
    try:
        return string.Formatter().vformat(tmpl, (), vals)
    except (ValueError, IndexError, KeyError):
        return tmpl


@dataclass
class Incident:
    id: str
    feed: str
    feed_name: str
    code: str
    severity: str
    opened_ns: int
    last_ns: int
    headline: str
    evidence: dict
    symbols: list
    action: str
    status: str = "OPEN"
    closed_ns: int | None = None
    samples: list = field(default_factory=list)
    top_features: list = field(default_factory=list)
    diagnosis: dict | None = None
    explanation: str = ""
    explanation_source: str = "template"
    timeline: list = field(default_factory=list)
    fault: dict | None = None
    ttd_s: float | None = None
    feedback: str | None = None
    feedback_note: str = ""
    count: int = 1
    merged_into: str | None = None

    @property
    def title(self) -> str:
        return S.CODE_INFO[self.code]["title"]

    def summary(self, tz) -> dict:
        end = self.closed_ns if self.closed_ns is not None else self.last_ns
        return {
            "id": self.id, "feed": self.feed, "feed_name": self.feed_name, "code": self.code,
            "title": self.title, "headline": self.headline, "severity": self.severity,
            "status": self.status, "opened_ms": self.opened_ns // MS,
            "opened_clock": clock(self.opened_ns, tz), "last_ms": self.last_ns // MS,
            "closed_ms": None if self.closed_ns is None else self.closed_ns // MS,
            "duration_s": round((end - self.opened_ns) / NS, 1), "ttd_s": self.ttd_s,
            "action": self.action, "symbols": list(self.symbols),
            "explanation_source": self.explanation_source, "feedback": self.feedback,
        }

    def detail(self, tz) -> dict:
        d = self.summary(tz)
        d.update({
            "evidence": dict(self.evidence), "samples": list(self.samples),
            "top_features": list(self.top_features), "diagnosis": self.diagnosis,
            "explanation": self.explanation, "timeline": list(self.timeline), "fault": self.fault,
            "count": self.count, "feedback_note": self.feedback_note, "merged_into": self.merged_into,
        })
        return d

    def _event(self, t: int, text: str, tz) -> None:
        self.timeline.append({"t_ms": t // MS, "clock": clock(t, tz), "event": text})
        if len(self.timeline) > 40:
            del self.timeline[1:2]


class IncidentManager:
    def __init__(self, cfg: Config, feed_names: dict, tz, explainer=None, annotate=None):
        self.cfg = cfg
        self.feed_names = feed_names
        self.tz = tz
        self.explainer = explainer
        self.annotate = annotate                # callback(incident) -> None, e.g. ground-truth TTD
        self.incidents: list[Incident] = []
        self.by_id: dict[str, Incident] = {}
        self.open: dict[tuple, Incident] = {}
        self._ids = itertools.count(1)
        self._llm_results: queue.SimpleQueue = queue.SimpleQueue()
        self.close_ns = int(cfg.incident_close_s * NS)
        self.opened_log: list[Incident] = []    # opened since last drain (for callers)

    def open_codes(self, feed: str) -> set:
        return {code for (f, code) in self.open if f == feed}

    def process(self, feed: str, findings: list[Finding], now: int, best_feed: str,
                ml_view: dict | None) -> None:
        for fd in findings:
            key = (feed, fd.code)
            inc = self.open.get(key)
            action = format_action(fd.code, fd.evidence, best_feed)
            if inc is None:
                inc = Incident(
                    id=f"INC-{next(self._ids):04d}", feed=feed, feed_name=self.feed_names.get(feed, feed),
                    code=fd.code, severity=fd.severity, opened_ns=now, last_ns=now, headline=fd.headline,
                    evidence=dict(fd.evidence), symbols=list(fd.symbols), action=action,
                    samples=list(fd.samples)[: self.cfg.max_samples])
                if ml_view:
                    inc.top_features = ml_view.get("top_features") or []
                    inc.diagnosis = ml_view.get("diagnosis")
                inc._event(now, f"opened {fd.severity}: {fd.headline}", self.tz)
                self.open[key] = inc
                self.incidents.append(inc)
                self.by_id[inc.id] = inc
                self._merge_children(inc, now)
                self.opened_log.append(inc)
                if self.annotate is not None:
                    self.annotate(inc)
                self._explain(inc)
                continue
            inc.last_ns = now
            inc.count += 1
            inc.headline = fd.headline
            inc.action = action
            for k, v in fd.evidence.items():
                old = inc.evidence.get(k)
                if isinstance(v, (int, float)) and isinstance(old, (int, float)) and k.endswith(
                        ("_count", "missing", "duplicates", "conflicts", "reordered", "quarantined")):
                    inc.evidence[k] = old + v          # cumulative counters
                else:
                    inc.evidence[k] = v
            for s in fd.symbols:
                if s not in inc.symbols:
                    inc.symbols.append(s)
            for smp in fd.samples:
                if len(inc.samples) < self.cfg.max_samples:
                    inc.samples.append(smp)
            if ml_view and ml_view.get("top_features"):
                inc.top_features = ml_view["top_features"]
                inc.diagnosis = ml_view.get("diagnosis")
            if S.STATE_RANK[fd.severity] > S.STATE_RANK[inc.severity]:
                inc.severity = fd.severity
                inc._event(now, f"escalated to {fd.severity}: {fd.headline}", self.tz)
                self._explain(inc)

    def _merge_children(self, parent: Incident, now: int) -> None:
        """A parent fingerprint explains open child incidents on the same feed: fold them in."""
        for code in S.ABSORBS.get(parent.code, ()):
            child = self.open.pop((parent.feed, code), None)
            if child is None:
                continue
            child.status = "RESOLVED"
            child.closed_ns = now
            child.merged_into = parent.id
            child._event(now, f"merged into {parent.id} ({parent.code})", self.tz)
            parent._event(now, f"absorbed {child.id} ({child.code})", self.tz)

    def expire(self, now: int) -> None:
        for key, inc in list(self.open.items()):
            if now - inc.last_ns >= self.close_ns:
                inc.status = "RESOLVED"
                inc.closed_ns = inc.last_ns
                inc._event(now, f"resolved: no new evidence for {self.cfg.incident_close_s:g} s", self.tz)
                del self.open[key]
        self._drain_llm()

    # ------------------------------------------------------------------ explanations
    def _explain(self, inc: Incident) -> None:
        if self.explainer is None:
            inc.explanation = f"{inc.feed_name} ({inc.feed}): {inc.title}. {inc.headline}\n" \
                              f"Evidence: {inc.evidence}\nAction: {inc.action}"
            return
        detail = inc.detail(self.tz)
        try:
            inc.explanation = self.explainer.template(detail)
        except Exception:  # never let the explainer break detection
            inc.explanation = f"{inc.title}: {inc.headline}\nEvidence available in the drawer.\nAction: {inc.action}"
        inc.explanation_source = "template"
        if getattr(self.explainer, "enabled", False):
            try:
                self.explainer.submit(detail, self._on_llm)
            except Exception:
                pass

    def _on_llm(self, incident_id, text, source) -> None:
        # Runs on the explainer's worker thread: hand over through a thread-safe queue.
        self._llm_results.put((incident_id, text, source))

    def _drain_llm(self) -> None:
        while True:
            try:
                iid, text, source = self._llm_results.get_nowait()
            except queue.Empty:
                return
            inc = self.by_id.get(iid)
            if inc is not None and text:
                inc.explanation = text
                inc.explanation_source = source

    # ------------------------------------------------------------------ queries
    def get(self, iid: str) -> Incident | None:
        return self.by_id.get(iid)

    def summaries(self, limit: int = 30) -> list[dict]:
        opened = [i for i in self.open.values()]
        opened.sort(key=lambda i: (-S.STATE_RANK[i.severity], -i.opened_ns))
        rest = [i for i in reversed(self.incidents) if i.status != "OPEN"]
        return [i.summary(self.tz) for i in (opened + rest)[:limit]]

    def all(self) -> list[Incident]:
        return list(self.incidents)

    def drain_opened(self) -> list[Incident]:
        out, self.opened_log = self.opened_log, []
        return out
