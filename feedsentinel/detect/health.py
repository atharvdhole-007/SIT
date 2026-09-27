"""L6 health score, state machine and trust.

Health = 100 x prod(1 - w_i * p_i) over the window's findings (w: how bad the fault type is,
p: how strongly it is present). The state escalates immediately and recovers only after several
calm windows (hysteresis).

Corroboration rule (the false-positive control): the ML alone can only make a feed DEGRADED.
CRITICAL needs hard evidence from a deterministic rule.
"""
from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field

from .. import schema as S
from ..config import Config

WEIGHT = {
    S.GAP: 0.5, S.DELAY: 0.5, S.DUPLICATE: 0.3, S.CONFLICTING_DUPLICATE: 0.8, S.OUT_OF_ORDER: 0.3,
    S.SEQ_RESET: 0.5, S.GARBLED: 0.6, S.FROZEN: 0.85, S.STALE: 0.85, S.DISCONNECT: 0.95,
    S.PRICE_SPIKE: 0.5, S.DECIMAL_SHIFT: 0.85, S.DRIFT: 0.6, S.TEST_DATA_LEAK: 0.95,
    S.RATE_STORM: 0.8, S.CLOCK_SKEW: 0.4, S.ML_ANOMALY: 0.35,
}


@dataclass
class Finding:
    code: str
    severity: str                       # DEGRADED | CRITICAL
    p: float                            # intensity 0..1
    headline: str
    evidence: dict = field(default_factory=dict)
    symbols: list = field(default_factory=list)
    samples: list = field(default_factory=list)
    hard: bool = True                   # produced by a deterministic rule (not the ML)


def absorb(findings: list[Finding], open_codes: set) -> list[Finding]:
    """Drop findings explained by a parent fingerprint found now or already open on this feed."""
    present = {f.code for f in findings} | set(open_codes)
    hidden = set()
    for parent, children in S.ABSORBS.items():
        if parent in present:
            hidden |= children
    if any(f.hard for f in findings) or (open_codes - {S.ML_ANOMALY}):
        hidden.add(S.ML_ANOMALY)
    return [f for f in findings if f.code not in hidden]


class FeedHealth:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.health = 100.0
        self.state = S.HEALTHY
        self.calm = 0
        self.trust = 1.0
        self.history: deque = deque(maxlen=cfg.history_points)
        self.findings: list[Finding] = []

    def update(self, findings: list[Finding]) -> None:
        cfg = self.cfg
        self.findings = findings
        inst = 100.0
        for f in findings:
            inst *= 1.0 - WEIGHT.get(f.code, 0.5) * max(0.0, min(1.0, f.p))
        if inst < self.health:
            self.health = inst
        else:
            self.health = min(inst, self.health + cfg.health_recovery_per_s * cfg.window_s)
        target = S.HEALTHY
        for f in findings:
            sev = f.severity
            if sev == S.CRITICAL and not f.hard:
                sev = S.DEGRADED               # corroboration rule
            if S.STATE_RANK[sev] > S.STATE_RANK[target]:
                target = sev
        if S.STATE_RANK[target] > S.STATE_RANK[self.state]:
            self.state = target
            self.calm = 0
        elif S.STATE_RANK[target] < S.STATE_RANK[self.state]:
            self.calm += 1
            if self.calm >= cfg.recover_windows:
                self.state = target
                self.calm = 0
        else:
            self.calm = 0
        q = self.health / 100.0
        if self.state == S.CRITICAL:
            q = min(q, 0.2)
        elif self.state == S.DEGRADED:
            q = min(q, 0.7)
        a = cfg.trust_down if q < self.trust else cfg.trust_up
        self.trust = max(cfg.trust_floor, self.trust + a * (q - self.trust))
        self.history.append(round(self.health, 1))

    def headline(self) -> str:
        if not self.findings:
            return ""
        top = max(self.findings, key=lambda f: (S.STATE_RANK[f.severity], WEIGHT.get(f.code, 0)))
        return f"{top.code}: {top.headline}"

    def codes(self) -> list[str]:
        fs = sorted(self.findings, key=lambda f: (-S.STATE_RANK[f.severity], -WEIGHT.get(f.code, 0)))
        out = []
        for f in fs:
            if f.code not in out:
                out.append(f.code)
        return out
