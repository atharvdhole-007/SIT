"""Automatic root-cause analysis. Deterministic: a cause-evidence scoring table, a scope test and a
blast radius. An LLM may only rephrase the result, never change it.

Reads engine output only (incidents, feed health); it does not change detection.
"""
from __future__ import annotations

from .. import schema as S

CAUSES = {
    "network": "Network latency / path",
    "packet_loss": "Packet loss",
    "handler_freeze": "Feed-handler freeze",
    "decoder": "Decoder / schema change",
    "auth": "Source authentication failure",
    "upstream": "Upstream / common-mode source",
    "clock": "Clock skew",
    "test_leak": "Test-data leak",
    "storm": "Reconnect storm",
    "market": "Genuine market event (halt/auction)",
}

# finding code -> {cause: evidence weight}
SCORES = {
    S.GAP: {"packet_loss": 0.85, "network": 0.3},
    S.DELAY: {"network": 0.85, "packet_loss": 0.1},
    S.DUPLICATE: {"network": 0.5, "storm": 0.35},
    S.CONFLICTING_DUPLICATE: {"decoder": 0.7, "network": 0.2},
    S.OUT_OF_ORDER: {"network": 0.8},
    S.SEQ_RESET: {"handler_freeze": 0.55, "upstream": 0.25},
    S.GARBLED: {"decoder": 0.85, "network": 0.1},
    S.FROZEN: {"handler_freeze": 0.9},
    S.STALE: {"handler_freeze": 0.6, "network": 0.3},
    S.DISCONNECT: {"network": 0.7, "handler_freeze": 0.3},
    S.PRICE_SPIKE: {"decoder": 0.45, "upstream": 0.3},
    S.DECIMAL_SHIFT: {"decoder": 0.95},
    S.DRIFT: {"decoder": 0.55, "upstream": 0.3},
    S.TEST_DATA_LEAK: {"test_leak": 0.95},
    S.RATE_STORM: {"storm": 0.95},
    S.CLOCK_SKEW: {"clock": 0.95},
    S.ML_ANOMALY: {"network": 0.3, "decoder": 0.3},
}

FEED_PATH = {"network", "packet_loss", "handler_freeze", "decoder", "auth", "clock", "test_leak", "storm"}

ACTIONS = {
    "network": "Failover to Feed {target}; check the network path and handler load on {feed}",
    "packet_loss": "Request retransmission of the missing range; failover to Feed {target} if loss persists",
    "handler_freeze": "Restart the {feed} feed handler; failover to Feed {target}",
    "decoder": "Block prices from {feed}; check the decoder / schema version; failover to Feed {target}",
    "auth": "Quarantine the source until its credentials are rotated; failover to Feed {target}",
    "upstream": "All feeds affected: escalate to the source exchange and hold downstream publication",
    "clock": "Distrust {feed} timestamps and check PTP/NTP; failover to Feed {target} for time-sensitive consumers",
    "test_leak": "Block propagation now: stop redistributing {feed}; failover to Feed {target}",
    "storm": "Isolate / rate-limit {feed}; failover to Feed {target}",
    "market": "No action needed: this is the market, not a feed fault",
}


def _evidence_lines(inc) -> list[str]:
    ev = inc.evidence
    c = inc.code
    if c == S.DELAY:
        return [f"Latency (p99) {ev.get('baseline_p99_ms')} ms -> {ev.get('lat_p99_ms')} ms"]
    if c == S.GAP:
        return [f"Sequence gaps: {ev.get('missing')} messages lost (latest seq {ev.get('gap_first')}-{ev.get('gap_last')})"]
    if c == S.DUPLICATE:
        return [f"Duplicates: {ev.get('duplicates')} ({ev.get('dup_rate_pct')}% of traffic)"]
    if c == S.FROZEN:
        return [f"{ev.get('frozen_symbols')} symbol(s) flat for {ev.get('frozen_for_s')} s while peers moved "
                f"{ev.get('peer_changes')} times", "Heartbeats and sequence numbers normal"]
    if c == S.DISCONNECT:
        return [f"No heartbeat for {ev.get('heartbeat_age_s')} s (resume from seq {ev.get('resume_seq')})"]
    if c == S.GARBLED:
        return [f"{ev.get('quarantined')} malformed messages quarantined (top: {ev.get('top_reason')})"]
    if c == S.TEST_DATA_LEAK:
        return [f"{ev.get('symbols_at_price')} symbols printing the identical price {ev.get('identical_price')}"]
    if c == S.RATE_STORM:
        return [f"Message rate {ev.get('rate_ratio')}x the other feeds ({ev.get('unknown_symbols')} unknown symbols)"]
    if c == S.DECIMAL_SHIFT:
        return [f"{ev.get('symbol')} at {ev.get('price')} vs reference {ev.get('consensus')} (x10^{ev.get('power')})"]
    if c == S.CLOCK_SKEW:
        return [f"Timestamps up to {ev.get('skew_ms')} ms in the future"]
    if c == S.DRIFT:
        return [f"{ev.get('symbol')} biased {ev.get('bias_bps')} bps vs consensus"]
    return [inc.headline]


def analyse(inc, engine, user_regions=None, feed_region=None) -> dict:
    """RCA for one incident, using the engine's current view of every feed."""
    feed = inc.feed
    fr = engine.feeds.get(feed)
    feed_name = fr.name if fr else feed
    # signals: this incident (full weight) + other open incidents on the same feed
    scores = {k: 0.0 for k in CAUSES}
    for cause, w in SCORES.get(inc.code, {}).items():
        scores[cause] += w
    related = [i for i in engine.incidents.open.values() if i.feed == feed and i.id != inc.id]
    for other in related:
        for cause, w in SCORES.get(other.code, {}).items():
            scores[cause] += 0.6 * w
    # scope test
    others = [f for f in engine.feed_ids if f != feed]
    affected_others = [f for f in others if engine.feeds[f].health.state != S.HEALTHY]
    halted = engine.ctx.halted_symbols()
    if not affected_others:
        scope = f"Other feeds unaffected: feed-path issue on {feed} ({feed_name})"
        scores["upstream"] *= 0.2
        scores["market"] = 0.0
    elif len(affected_others) == len(others):
        scope = "All feeds affected: upstream, common-mode or market event"
        scores["upstream"] += 0.8
        for k in FEED_PATH:
            scores[k] *= 0.5
    else:
        scope = f"Feeds {', '.join(affected_others)} also degraded: check shared infrastructure"
        scores["network"] += 0.2
    if halted and set(inc.symbols) & set(halted):
        scores["market"] += 0.9
    ranked = sorted(((v, k) for k, v in scores.items() if v > 0), reverse=True)
    total = sum(v for v, _ in ranked) or 1.0

    def conf(v):
        return int(round(min(95.0, 100.0 * v / (total + 0.25))))

    primary = ranked[0][1] if ranked else "network"
    secondary = ranked[1] if len(ranked) > 1 else None
    # failover target: highest-trust healthy feed (in the user's regions)
    target = None
    for f in sorted(others, key=lambda x: -engine.feeds[x].health.trust):
        if engine.feeds[f].health.state == S.HEALTHY:
            target = f
            break
    target_txt = f"{target} ({engine.feeds[target].name})" if target else "the healthiest available feed"
    evidence = _evidence_lines(inc)
    for other in related[:3]:
        evidence.extend(_evidence_lines(other))
    evidence.append("Other feeds unaffected" if not affected_others else scope)
    affected = list(inc.symbols) or list(engine.symbols)
    action = ACTIONS[primary].format(target=target_txt, feed=f"{feed} ({feed_name})")
    out = {
        "incident_id": inc.id,
        "primary": {"cause": CAUSES[primary], "confidence": conf(ranked[0][0]) if ranked else 0},
        "secondary": None if secondary is None else {"cause": CAUSES[secondary[1]], "confidence": conf(secondary[0])},
        "evidence": evidence,
        "scope": scope,
        "affected": affected,
        "action": action,
        "failover": target,
    }
    lines = ["ROOT CAUSE ANALYSIS",
             f"Primary: {out['primary']['cause']} ({out['primary']['confidence']}%)"]
    if out["secondary"]:
        lines.append(f"Secondary: {out['secondary']['cause']} ({out['secondary']['confidence']}%)")
    lines.append("Evidence: " + " ".join(f"• {e}" for e in evidence))
    lines.append(f"Likely affected: {', '.join(affected)}")
    lines.append(f"Recommended action: {action}")
    out["text"] = "\n".join(lines)
    return out
