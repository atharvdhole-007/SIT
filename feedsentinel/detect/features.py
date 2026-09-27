"""L3 feature windows: every second, each feed becomes a vector describing its *behaviour*.

The ML never sees raw prices. Ratios are taken against the feed's own learned baseline or its peers,
so one model serves a fast direct feed and a slower SIP alike.
"""
from __future__ import annotations

import math

import numpy as np

FEATURES = (
    "log_msgs",            # log1p(messages received in the window)
    "rate_ratio_peer",     # log((n+1)/(median peer n+1))
    "rate_ratio_self",     # log((n+1)/(own recent average+1))
    "lat_p50_ratio",       # log(p50 / learned p50)
    "lat_p99_ratio",       # log(p99 / learned p99)
    "log_missing",         # confirmed lost sequence numbers
    "log_dups",
    "log_conflicts",
    "log_reorders",
    "resets",
    "quarantine_rate",     # malformed / all data messages
    "log_dev_max",         # max |deviation from consensus| (bps)
    "log_dev_mean",
    "frozen_frac",         # active symbols whose value has not moved while consensus did
    "stale_frac",          # active symbols not published while consensus moved
    "hb_age_s",            # seconds since the last message of any kind
    "future_frac",         # messages stamped in the future
    "identical_syms",      # most symbols sharing one identical price
    "unknown_sym_frac",    # unknown or test symbols / data messages
    "band_viol_frac",      # LULD-style band violations / data messages
    "size_mismatch_frac",  # sizes disagreeing with the majority of feeds
    "cusum_norm",          # max drift CUSUM / threshold
)
N_FEATURES = len(FEATURES)

FEATURE_TEXT = {
    "log_msgs": "message count",
    "rate_ratio_peer": "message rate vs other feeds",
    "rate_ratio_self": "message rate vs own baseline",
    "lat_p50_ratio": "median latency vs baseline",
    "lat_p99_ratio": "p99 latency vs baseline",
    "log_missing": "lost sequence numbers",
    "log_dups": "duplicates",
    "log_conflicts": "conflicting duplicates",
    "log_reorders": "reordered messages",
    "resets": "sequence resets",
    "quarantine_rate": "quarantined (malformed) share",
    "log_dev_max": "max deviation from consensus",
    "log_dev_mean": "mean deviation from consensus",
    "frozen_frac": "share of symbols not moving",
    "stale_frac": "share of symbols not updating",
    "hb_age_s": "seconds since last message",
    "future_frac": "share of future timestamps",
    "identical_syms": "symbols sharing one price",
    "unknown_sym_frac": "unknown/test symbol share",
    "band_viol_frac": "price-band violation share",
    "size_mismatch_frac": "sizes disagreeing with other feeds",
    "cusum_norm": "drift statistic",
}


def _lg(x: float) -> float:
    return math.log1p(max(0.0, x))


def _ratio(a, b) -> float:
    """log(a/b); 0 when either side is unknown (no data yet / no baseline) or non-positive."""
    if a is None or b is None or a <= 0 or b <= 0:
        return 0.0
    return math.log(a / b)


def build(w: dict) -> np.ndarray:
    """w: raw window measurements from the engine (see Engine._window_raw)."""
    n = w["msgs"]
    data = max(1, w["data"])
    x = (
        _lg(n),
        math.log((n + 1.0) / (w["peer_msgs"] + 1.0)) if w["peer_msgs"] is not None else 0.0,
        math.log((n + 1.0) / (w["ewma_msgs"] + 1.0)) if w["ewma_msgs"] is not None else 0.0,
        _ratio(w["lat_p50"], w["base_p50"]),
        _ratio(w["lat_p99"], w["base_p99"]),
        _lg(w["missing"]),
        _lg(w["dups"]),
        _lg(w["conflicts"]),
        _lg(w["reorders"]),
        float(w["resets"]),
        w["garbled"] / data,
        _lg(w["dev_max"]),
        _lg(w["dev_mean"]),
        w["frozen_frac"],
        w["stale_frac"],
        min(w["hb_age_s"], 30.0),
        w["future"] / data,
        float(w["identical_syms"]),
        w["unknown_syms"] / data,
        w["band_viol"] / data,
        w["size_mismatch_frac"],
        min(w["cusum_norm"], 4.0),
    )
    return np.asarray(x, dtype=np.float64)
