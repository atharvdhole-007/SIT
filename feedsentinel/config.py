"""Every detection threshold in one place, with presets for replay and live data."""
from __future__ import annotations

from dataclasses import dataclass, field, replace
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "data"
RAW_DIR = DATA_DIR / "raw" / "lobster"
PROCESSED_DIR = DATA_DIR / "processed"
REFERENCE_FILE = DATA_DIR / "reference" / "nasdaqlisted.txt"
MODELS_DIR = ROOT / "models"
REPORTS_DIR = ROOT / "reports"
DASHBOARD_DIR = ROOT / "dashboard"

NS = 1_000_000_000
MS = 1_000_000

# Engine layer sets used by the ablation study.
LAYERS_RULES = frozenset({"L1", "L2", "L3"})
LAYERS_CONSENSUS = LAYERS_RULES | {"L5"}
LAYERS_FULL = LAYERS_CONSENSUS | {"L4"}
ABLATION_CONFIGS = {
    "rules": LAYERS_RULES,
    "rules+consensus": LAYERS_CONSENSUS,
    "rules+consensus+ml": LAYERS_FULL,
}


@dataclass(frozen=True)
class Config:
    mode: str = "replay"                 # "replay" | "live"
    window_s: float = 1.0                # L3 feature window and health update period

    # ---- L1 gatekeeper
    check_tick: bool = True              # quotes must sit on the $0.01 grid (Reg NMS 612); trades may not
    tick_size: float = 0.01
    tick_min_price: float = 1.0
    band_pct: float = 0.05               # LULD-style band around the 5-minute reference price
    band_pct_edge: float = 0.10          # doubled in the first 15 and last 25 minutes of the session
    band_ref_s: int = 300
    band_min_ref_samples: int = 5
    decimal_tol: float = 0.03            # |log10(px/ref) - k| below this -> decimal shift by 10**k
    max_ts_error_s: float = 3600.0       # exch_ts further than this from recv_ts is garbage
    future_tol_ms: float = 5.0           # exch_ts ahead of recv_ts by more than this = clock skew evidence

    # ---- L2 sequence & timing
    fill_window_ms: float = 100.0        # a missing seq filled within this window is reordering, not loss
    lost_memory_s: float = 30.0          # keep confirmed gaps this long to recognise late retransmissions
    ring_size: int = 1 << 17             # recent (seq -> content hash) memory for duplicate checks
    dup_memory_s: float = 60.0
    reset_backjump: int = 1000           # an unexplained backwards seq jump larger than this = reset
    hb_timeout_s: float = 1.6            # no heartbeat or data for this long = disconnected (1 s heartbeats)
    lat_mult: float = 3.0                # DELAY if p99 > max(mult * baseline, baseline + abs)
    lat_abs_ms: float = 50.0
    lat_persist: int = 2                 # consecutive windows
    lat_baseline_min_windows: int = 20
    garbled_min: int = 2                 # quarantined messages per window (or 1 in two consecutive windows)
    future_min: int = 3

    # ---- L3 rate rules
    storm_ratio: float = 4.0             # msg rate vs peers (or vs own baseline in rules-only mode)
    storm_ratio_self: float = 6.0
    storm_min_msgs: int = 200
    stale_self_s: float = 5.0            # rules-only: no data for this long while heartbeats continue
    identical_min_symbols: int = 3       # >= this many symbols printing the same price = test data

    # ---- L5 consensus
    watermark_max_wait_ms: float = 500.0
    live_feed_s: float = 1.0
    hist_keep_s: float = 5.0
    spike_bps: float = 75.0
    frozen_min_changes: int = 3
    frozen_min_s: float = 1.0
    stale_min_changes: int = 3
    stale_min_s: float = 1.5
    stale_peer_msgs: int = 10
    drift_k_bps: float = 5.0             # CUSUM slack
    drift_h_bps: float = 150.0           # CUSUM decision threshold
    drift_clip_bps: float = 50.0         # one outlier cannot trip the drift detector
    drift_min_bias_bps: float = 5.0
    basis_tau_s: float = 0.0             # >0 learns a per-(feed, symbol) basis (live venues differ slightly)

    # ---- L4 AI
    ml_persist: int = 2                  # anomalous windows out of the last 3
    ml_percentile: float = 99.5

    # ---- L6 health
    recover_windows: int = 5
    health_recovery_per_s: float = 8.0
    trust_down: float = 0.5
    trust_up: float = 0.05
    trust_floor: float = 0.05

    # ---- L7 incidents
    incident_close_s: float = 10.0
    max_samples: int = 5

    # ---- misc
    chart_step_ms: int = 250
    chart_points: int = 480
    history_points: int = 120
    layers: frozenset = field(default_factory=lambda: LAYERS_FULL)

    def with_layers(self, layers) -> "Config":
        return replace(self, layers=frozenset(layers))


REPLAY = Config()

# Live crypto venues: prices differ slightly between venues, clocks are not PTP-synced, heartbeats
# are produced by our own handlers over the public internet, and tick sizes differ per venue.
LIVE = replace(
    REPLAY,
    mode="live",
    check_tick=False,
    band_pct=0.10,
    band_pct_edge=0.10,
    future_tol_ms=1000.0,
    hb_timeout_s=4.0,
    lat_abs_ms=400.0,
    lat_mult=4.0,
    spike_bps=150.0,
    frozen_min_changes=8,
    frozen_min_s=6.0,
    stale_min_changes=8,
    stale_min_s=6.0,
    stale_peer_msgs=5,
    drift_k_bps=15.0,
    drift_h_bps=600.0,
    drift_clip_bps=100.0,
    drift_min_bias_bps=20.0,
    basis_tau_s=600.0,
    watermark_max_wait_ms=1500.0,
    live_feed_s=3.0,
    fill_window_ms=500.0,
)
