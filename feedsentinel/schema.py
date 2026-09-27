"""Canonical message schema shared by the simulator, live adapters and detectors.

Every message that travels on the bus is a plain, JSON-serialisable dict. Field
names follow Nasdaq conventions where one exists (ITCH message types, MoldUDP64
sequencing, ITCH System Event and Trading Action codes).

Header (all messages)
    feed     str   feed id, e.g. "A"
    session  str   MoldUDP64-style session id; a new session restarts sequencing
    seq      int   sequence number. For data messages it is the message's own
                   number. For heartbeats (type "H") it is the *next* sequence
                   number the sender will use (MoldUDP64 heartbeat semantics), so a
                   receiver can detect loss even when the market is quiet.
    type     str   one of MSG_TYPES
    exch_ts  int   exchange/source timestamp, integer nanoseconds since the UNIX epoch
    recv_ts  int   receive timestamp (ns since epoch), stamped by the feed handler /
                   receiver, never by the sender

Payload by type
    "Q" quote            sym, bid, bid_sz, ask, ask_sz        (prices are float dollars)
    "T" trade            sym, px, sz, [side "B"|"S" aggressor]
    "H" heartbeat        (no payload; seq = next sequence number)
    "S" system event     event: one of SYSTEM_EVENTS keys
    "R" trading action   sym, state: one of TRADING_STATES keys

All times are int nanoseconds since the UNIX epoch. Prices are floats in the
instrument's currency (the source uses ITCH Price(4) integers; the feed handler
normalises them by 10**4).
"""
from __future__ import annotations

# --------------------------------------------------------------------------- types
QUOTE = "Q"
TRADE = "T"
HEARTBEAT = "H"
SYSTEM = "S"
TRADING_ACTION = "R"

MSG_TYPES = {
    QUOTE: "quote",
    TRADE: "trade",
    HEARTBEAT: "heartbeat",
    SYSTEM: "system event",
    TRADING_ACTION: "trading action",
}

HEADER_FIELDS = ("feed", "session", "seq", "type", "exch_ts")
PAYLOAD_FIELDS = {
    QUOTE: ("sym", "bid", "bid_sz", "ask", "ask_sz"),
    TRADE: ("sym", "px", "sz"),
    HEARTBEAT: (),
    SYSTEM: ("event",),
    TRADING_ACTION: ("sym", "state"),
}
# Messages that carry market data and consume a sequence number.
DATA_TYPES = (QUOTE, TRADE)
SEQUENCED_TYPES = (QUOTE, TRADE, SYSTEM, TRADING_ACTION)

# ITCH 5.0 System Event codes
SYSTEM_EVENTS = {
    "O": "start of messages",
    "S": "start of system hours",
    "Q": "start of market hours",
    "M": "end of market hours",
    "E": "end of system hours",
    "C": "end of messages",
}
# ITCH 5.0 Stock Trading Action states
HALTED, PAUSED, QUOTE_ONLY, TRADING = "H", "P", "Q", "T"
TRADING_STATES = {
    HALTED: "halted",
    PAUSED: "paused",
    QUOTE_ONLY: "quotation only",
    TRADING: "trading",
}

# --------------------------------------------------------------------------- health
HEALTHY = "HEALTHY"
DEGRADED = "DEGRADED"
CRITICAL = "CRITICAL"
STATE_RANK = {HEALTHY: 0, DEGRADED: 1, CRITICAL: 2}

# --------------------------------------------------------------------------- finding codes
GAP = "GAP"
DELAY = "DELAY"
DUPLICATE = "DUPLICATE"
CONFLICTING_DUPLICATE = "CONFLICTING_DUPLICATE"
OUT_OF_ORDER = "OUT_OF_ORDER"
SEQ_RESET = "SEQ_RESET"
GARBLED = "GARBLED"
FROZEN = "FROZEN"
STALE = "STALE"
DISCONNECT = "DISCONNECT"
PRICE_SPIKE = "PRICE_SPIKE"
DECIMAL_SHIFT = "DECIMAL_SHIFT"
DRIFT = "DRIFT"
TEST_DATA_LEAK = "TEST_DATA_LEAK"
RATE_STORM = "RATE_STORM"
CLOCK_SKEW = "CLOCK_SKEW"
ML_ANOMALY = "ML_ANOMALY"

# code -> title, the layer that produces it, and the runbook action shown to operators.
# `action` may contain {placeholders} that the incident manager fills from evidence.
CODE_INFO = {
    GAP: dict(
        title="Sequence gap (packet loss)", layer="L2",
        action="Request retransmission of seq {gap_first}-{gap_last} (MoldUDP64 re-request / "
               "SoupBinTCP re-login); fail consumers over to {best_feed} if loss persists"),
    DELAY: dict(
        title="Feed delayed", layer="L2",
        action="Route latency-sensitive consumers to {best_feed}; check the network path and "
               "feed-handler load"),
    DUPLICATE: dict(
        title="Duplicate messages", layer="L2",
        action="Check A/B line arbitration; de-duplicate by sequence number"),
    CONFLICTING_DUPLICATE: dict(
        title="Conflicting duplicates (same seq, different content)", layer="L2",
        action="Treat as corruption: quarantine the feed, fail over to {best_feed}, open a vendor ticket"),
    OUT_OF_ORDER: dict(
        title="Out-of-order delivery", layer="L2",
        action="Resequence by sequence number before publishing downstream; check multi-path routing"),
    SEQ_RESET: dict(
        title="Sequence reset mid-session", layer="L2",
        action="Confirm the session with the source; the feed handler probably restarted. "
               "Re-sync state and verify no messages were lost"),
    GARBLED: dict(
        title="Garbled messages quarantined", layer="L1",
        action="Check the decoder / schema version for this feed; bad messages are quarantined, "
               "not dropped"),
    FROZEN: dict(
        title="Frozen values (feed alive, prices not moving)", layer="L5",
        action="Restart the feed handler; fail consumers over to {best_feed}"),
    STALE: dict(
        title="Stale feed (heartbeats alive, no data)", layer="L5",
        action="Fail over to {best_feed}; check the upstream session and subscriptions"),
    DISCONNECT: dict(
        title="Disconnected (no heartbeats)", layer="L2",
        action="Fail over to {best_feed}; re-establish the session and re-login from seq {resume_seq}"),
    PRICE_SPIKE: dict(
        title="Price spike vs consensus", layer="L1/L5",
        action="Block the outlier prices from downstream; confirm against {best_feed}"),
    DECIMAL_SHIFT: dict(
        title="Decimal shift (price-scale bug)", layer="L1/L5",
        action="Block prices from this feed now; check the Price(4) scale mapping in the decoder"),
    DRIFT: dict(
        title="Slow drift / bias vs consensus", layer="L5",
        action="Check the scaling / adjustment factor against reference data; fail over to {best_feed}"),
    TEST_DATA_LEAK: dict(
        title="Test data in production", layer="L1/L5",
        action="Block propagation NOW: stop redistributing this feed and purge the test prints "
               "downstream"),
    RATE_STORM: dict(
        title="Message-rate storm (reconnect loop)", layer="L3",
        action="Rate-limit or isolate the source; protect downstream consumers; fail over to {best_feed}"),
    CLOCK_SKEW: dict(
        title="Clock skew (timestamps from the future)", layer="L2",
        action="Check PTP/NTP synchronisation at the source; do not trust this feed's timestamps"),
    ML_ANOMALY: dict(
        title="Unusual feed behaviour (AI)", layer="L4",
        action="Investigate: behaviour is outside the learned normal range (see top features)"),
}

FAULT_CODES = tuple(CODE_INFO)

# Fingerprints that absorb findings they explain, so operators get one card, not many.
ABSORBS = {
    TEST_DATA_LEAK: {PRICE_SPIKE, DECIMAL_SHIFT, GARBLED, DRIFT, FROZEN, STALE, ML_ANOMALY},
    RATE_STORM: {DUPLICATE, GARBLED, OUT_OF_ORDER, ML_ANOMALY},
    DISCONNECT: {STALE, GAP, FROZEN, ML_ANOMALY},
    STALE: {FROZEN, ML_ANOMALY},
    FROZEN: {DRIFT, PRICE_SPIKE, ML_ANOMALY},
    DECIMAL_SHIFT: {PRICE_SPIKE, DRIFT, STALE, FROZEN, ML_ANOMALY},
    CONFLICTING_DUPLICATE: {DUPLICATE, ML_ANOMALY},
    SEQ_RESET: {GAP, DUPLICATE, ML_ANOMALY},
    DELAY: {STALE, FROZEN, DRIFT, ML_ANOMALY},
    CLOCK_SKEW: {STALE, FROZEN, DRIFT, PRICE_SPIKE, ML_ANOMALY},
    GARBLED: {STALE, ML_ANOMALY},
}


def required_fields(msg_type: str) -> tuple[str, ...]:
    """Header plus payload fields a message of this type must carry."""
    return HEADER_FIELDS + PAYLOAD_FIELDS.get(msg_type, ())
