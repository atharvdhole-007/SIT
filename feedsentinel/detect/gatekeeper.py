"""L1 gatekeeper: per-message validation. Quarantine, never drop: the garbled message is the evidence.

check_header runs before sequencing (a message without a usable sequence number cannot be tracked).
check_message runs after sequencing, on the payload.
"""
from __future__ import annotations

import math

from .. import schema as S
from ..config import NS, Config

# Reasons that mean "this message is malformed" (-> GARBLED)
GARBLE_REASONS = frozenset({
    "BAD_HEADER", "UNKNOWN_TYPE", "MISSING_FIELD", "BAD_TYPE", "BAD_VALUE", "BAD_TIMESTAMP",
    "NONPOSITIVE_PRICE", "BAD_SIZE", "CROSSED_QUOTE", "OFF_TICK", "UNKNOWN_SYMBOL",
})
# Reasons with their own fingerprint
PRICE_BAND = "PRICE_BAND"
TEST_ISSUE = "TEST_ISSUE"
CONFLICT = "CONFLICTING_DUPLICATE"

REASON_TEXT = {
    "BAD_HEADER": "header unusable (feed/seq)",
    "UNKNOWN_TYPE": "unknown message type",
    "MISSING_FIELD": "required field missing",
    "BAD_TYPE": "field has the wrong type",
    "BAD_VALUE": "field value not allowed",
    "BAD_TIMESTAMP": "timestamp is garbage",
    "NONPOSITIVE_PRICE": "price <= 0",
    "BAD_SIZE": "size <= 0",
    "CROSSED_QUOTE": "bid >= ask",
    "OFF_TICK": "quote off the $0.01 tick grid",
    "UNKNOWN_SYMBOL": "symbol not in the Nasdaq Symbol Directory",
    TEST_ISSUE: "test issue (Test Issue = Y) in production",
    PRICE_BAND: "price outside the LULD-style band",
    CONFLICT: "same sequence number, different content",
}


def _num(x) -> bool:
    return isinstance(x, (int, float)) and not isinstance(x, bool) and math.isfinite(x)


def _int(x) -> bool:
    return isinstance(x, int) and not isinstance(x, bool)


def check_header(msg, feed_ids) -> str | None:
    if not isinstance(msg, dict):
        return "BAD_HEADER"
    if msg.get("feed") not in feed_ids:
        return "BAD_HEADER"
    seq = msg.get("seq")
    if not _int(seq) or seq < 0:
        return "BAD_HEADER"
    if not isinstance(msg.get("session"), str):
        return "BAD_HEADER"
    return None


class Gatekeeper:
    def __init__(self, cfg: Config, refdata, ctx):
        self.cfg = cfg
        self.refdata = refdata
        self.ctx = ctx
        self.max_ts_err = int(cfg.max_ts_error_s * NS)
        self.tick_mult = 1.0 / cfg.tick_size

    def check(self, msg: dict, recv_ts: int):
        """Returns (reason | None, detail | None). detail carries band info for PRICE_BAND."""
        typ = msg.get("type")
        if typ not in S.MSG_TYPES:
            return "UNKNOWN_TYPE", None
        ts = msg.get("exch_ts")
        if not _int(ts) or abs(ts - recv_ts) > self.max_ts_err:
            return "BAD_TIMESTAMP", None
        for f in S.PAYLOAD_FIELDS[typ]:
            if f not in msg:
                return "MISSING_FIELD", None
        if typ == S.SYSTEM:
            return (None, None) if msg["event"] in S.SYSTEM_EVENTS else ("BAD_VALUE", None)
        sym = msg["sym"]
        if not isinstance(sym, str):
            return "BAD_TYPE", None
        sec = self.refdata.get(sym)
        if sec is None:
            return "UNKNOWN_SYMBOL", None
        if sec.test_issue:
            return TEST_ISSUE, None
        if typ == S.TRADING_ACTION:
            return (None, None) if msg["state"] in S.TRADING_STATES else ("BAD_VALUE", None)
        if typ == S.QUOTE:
            bid, ask, bsz, asz = msg["bid"], msg["ask"], msg["bid_sz"], msg["ask_sz"]
            if not (_num(bid) and _num(ask) and _num(bsz) and _num(asz)):
                return "BAD_TYPE", None
            if bid <= 0 or ask <= 0:
                return "NONPOSITIVE_PRICE", None
            if bsz <= 0 or asz <= 0:
                return "BAD_SIZE", None
            band = self._band(sym, (bid + ask) / 2.0, ts)
            if band is not None:
                return PRICE_BAND, band
            if bid >= ask:
                return "CROSSED_QUOTE", None
            if self.cfg.check_tick:
                m = self.tick_mult
                for p in (bid, ask):
                    if p >= self.cfg.tick_min_price and abs(p * m - round(p * m)) > 1e-6:
                        return "OFF_TICK", None
            return None, None
        # trade: sub-penny prices are legal (midpoint / hidden executions)
        px, sz = msg["px"], msg["sz"]
        if not (_num(px) and _num(sz)):
            return "BAD_TYPE", None
        if px <= 0:
            return "NONPOSITIVE_PRICE", None
        if sz <= 0:
            return "BAD_SIZE", None
        band = self._band(sym, px, ts)
        if band is not None:
            return PRICE_BAND, band
        return None, None

    def _band(self, sym: str, price: float, t: int):
        ref = self.ctx.ref_price(sym)
        if ref is None or ref <= 0:
            return None
        ratio = price / ref
        if abs(ratio - 1.0) <= self.ctx.band_pct(t):
            return None
        lr = math.log10(ratio) if ratio > 0 else 0.0
        k = round(lr)
        power = k if (k != 0 and abs(lr - k) < self.cfg.decimal_tol) else 0
        return {"sym": sym, "price": price, "ref": ref, "ratio": ratio, "power": power}
