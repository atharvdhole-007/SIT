"""L5 cross-feed consensus, as of the same exchange timestamp, with trust weights.

Each accepted quote is recorded per (feed, symbol). An update is evaluated only once every live
feed's watermark (latest exchange/heartbeat timestamp received) has passed its exchange time, so a
slow-but-correct feed (the SIP) is compared on equal terms and never penalised for latency; delay is
L2's job. At each evaluation every feed's as-of value is compared with the trust-weighted median.

From this one mechanism come the faults no single-feed rule can see:
  FROZEN  the feed keeps publishing (fresh seq, fresh timestamps) but its value does not change
          while the consensus changes
  STALE   the feed stops publishing a symbol while the others keep updating it
  spikes  a value far from consensus
  DRIFT   a small, persistent bias (two-sided CUSUM on the signed deviation)
"""
from __future__ import annotations

import heapq
from bisect import bisect_left, bisect_right, insort

from ..config import MS, NS, Config


class _Hist:
    __slots__ = ("ts", "mid", "bsz", "asz")

    def __init__(self):
        self.ts: list[int] = []
        self.mid: list[float] = []
        self.bsz: list[float] = []
        self.asz: list[float] = []


def weighted_median(pairs: list[tuple[float, float]]) -> float:
    """pairs of (value, weight). An exact 50/50 split averages the two middle values."""
    pairs.sort()
    total = 0.0
    for _, w in pairs:
        total += w
    half = total / 2.0
    acc = 0.0
    for i, (v, w) in enumerate(pairs):
        acc += w
        if acc > half + 1e-12:
            return v
        if abs(acc - half) <= 1e-12 and i + 1 < len(pairs):
            return (v + pairs[i + 1][0]) / 2.0
    return pairs[-1][0]


class Consensus:
    def __init__(self, cfg: Config, feed_ids, symbols):
        self.cfg = cfg
        self.feeds = list(feed_ids)
        self.symbols = list(symbols)
        self.hist = {(f, s): _Hist() for f in self.feeds for s in self.symbols}
        self.heap: list[tuple[int, str]] = []
        self.last_eval_t = {s: -1 for s in self.symbols}
        self.cons = {s: None for s in self.symbols}          # latest consensus mid
        self.cons_t = {s: 0 for s in self.symbols}
        self.cons_change_t = {s: 0 for s in self.symbols}
        self.trust = {f: 1.0 for f in self.feeds}
        self.feed_last_exch = {f: 0 for f in self.feeds}
        self.feed_last_recv = {f: 0 for f in self.feeds}
        self.lat_cur = {f: 0 for f in self.feeds}     # max observed latency, this window (ns)
        self.lat_prev = {f: 0 for f in self.feeds}    # ... and the previous window
        self.judged: set = set(self.feeds)            # feeds caught up enough to be compared
        self.watermark = 0
        keys = [(f, s) for f in self.feeds for s in self.symbols]
        self.last_val = {k: None for k in keys}
        self.val_change_t = {k: 0 for k in keys}
        self.n_since_val = {k: 0 for k in keys}
        self.last_entry = {k: None for k in keys}
        self.n_since_msg = {k: 0 for k in keys}
        self.cusum_pos = {k: 0.0 for k in keys}
        self.cusum_neg = {k: 0.0 for k in keys}
        self.bias = {k: 0.0 for k in keys}
        self.basis = {k: 0.0 for k in keys}
        self.basis_t = {k: 0 for k in keys}
        self.keep_ns = int(cfg.hist_keep_s * NS)
        self.max_wait_ns = int(cfg.watermark_max_wait_ms * MS)
        self.live_ns = int(cfg.live_feed_s * NS)
        self.evaluations = 0
        self.reset_window()

    def reset_window(self) -> None:
        self.w_dev_max = {f: 0.0 for f in self.feeds}
        self.w_dev_sum = {f: 0.0 for f in self.feeds}
        self.w_dev_n = {f: 0 for f in self.feeds}
        self.w_spikes = {f: 0 for f in self.feeds}
        self.w_spike_ev = {f: None for f in self.feeds}
        self.w_size_mis = {f: 0 for f in self.feeds}
        self.w_size_n = {f: 0 for f in self.feeds}

    # ------------------------------------------------------------------ inputs
    def on_receive(self, feed: str, exch_ts: int, recv_ts: int) -> None:
        """Any message (data or heartbeat) advances the feed's watermark: feeds are FIFO."""
        if exch_ts > self.feed_last_exch[feed]:
            self.feed_last_exch[feed] = exch_ts
        self.feed_last_recv[feed] = recv_ts
        lat = recv_ts - exch_ts
        if lat > self.lat_cur[feed]:
            self.lat_cur[feed] = lat

    def roll_latency(self) -> None:
        for f in self.feeds:
            self.lat_prev[f] = self.lat_cur[f]
            self.lat_cur[f] = 0

    def feed_watermark(self, f: str, now: int) -> int:
        """Exchange time up to which feed f has delivered everything. A FIFO feed with current
        latency L has delivered all it sent before now - L, even if it had nothing to send."""
        # Conservative: a latency burst still in flight can exceed what we have observed so far.
        lat = 2 * max(self.lat_cur[f], self.lat_prev[f]) + 10 * MS
        return min(now, max(self.feed_last_exch[f], now - lat))

    def on_quote(self, feed: str, sym: str, t: int, bid: float, ask: float, bsz, asz) -> None:
        h = self.hist.get((feed, sym))
        if h is None:
            return
        mid = (bid + ask) / 2.0
        ts = h.ts
        if not ts or t >= ts[-1]:
            ts.append(t)
            h.mid.append(mid)
            h.bsz.append(bsz)
            h.asz.append(asz)
        else:                                   # late (reordered) message: keep time order
            i = bisect_right(ts, t)
            ts.insert(i, t)
            h.mid.insert(i, mid)
            h.bsz.insert(i, bsz)
            h.asz.insert(i, asz)
        if len(ts) > 64 and ts[0] < t - 2 * self.keep_ns:
            k = bisect_left(ts, t - self.keep_ns) - 1
            if k > 0:
                del ts[:k], h.mid[:k], h.bsz[:k], h.asz[:k]
        heapq.heappush(self.heap, (t, sym))

    # ------------------------------------------------------------------ evaluation
    def advance(self, now: int) -> None:
        # Feeds lagging more than max_wait sit out of the comparison (their lag is DELAY, an L2
        # finding); everyone else is compared only at times they have all fully reported.
        judged, marks = set(), []
        for f in self.feeds:
            if now - self.feed_last_recv[f] > self.live_ns:
                continue
            wm = self.feed_watermark(f, now)
            if now - wm <= self.max_wait_ns:
                judged.add(f)
                marks.append(wm)
        self.judged = judged
        if not marks:
            return
        w = min(marks)
        if w < self.watermark:
            w = self.watermark
        self.watermark = w
        heap = self.heap
        while heap and heap[0][0] <= w:
            t, s = heapq.heappop(heap)
            if t > self.last_eval_t[s]:
                self.last_eval_t[s] = t
                self._evaluate(s, t)

    def _evaluate(self, s: str, t: int) -> None:
        vals = []
        judged = self.judged
        for f in self.feeds:
            if f not in judged:
                continue
            h = self.hist[(f, s)]
            if not h.ts:
                continue
            i = bisect_right(h.ts, t) - 1
            if i < 0:
                continue
            vals.append((f, h.mid[i], h.bsz[i], h.asz[i], h.ts[i]))
        if len(vals) < 2:
            return
        self.evaluations += 1
        cons = weighted_median([(v[1], max(self.trust[v[0]], 1e-6)) for v in vals])
        if cons <= 0:
            return
        prev = self.cons[s]
        changed = prev is not None and cons != prev
        if prev is None or changed:
            self.cons_change_t[s] = t
        self.cons[s] = cons
        self.cons_t[s] = t
        # sizes: majority vote over feeds
        size_votes: dict[tuple, int] = {}
        for v in vals:
            key = (v[2], v[3])
            size_votes[key] = size_votes.get(key, 0) + 1
        size_major, size_n = max(size_votes.items(), key=lambda kv: kv[1])
        cfg = self.cfg
        k_slack, clip, cap = cfg.drift_k_bps, cfg.drift_clip_bps, 4 * cfg.drift_h_bps
        for f, mid, bsz, asz, et in vals:
            key = (f, s)
            dev = (mid - cons) / cons * 1e4
            if cfg.basis_tau_s > 0:
                dt = t - self.basis_t[key] if self.basis_t[key] else 0
                a = min(1.0, dt / (cfg.basis_tau_s * NS))
                self.basis[key] += a * (dev - self.basis[key])
                self.basis_t[key] = t
                dev -= self.basis[key]
            ad = dev if dev >= 0 else -dev
            if ad > self.w_dev_max[f]:
                self.w_dev_max[f] = ad
            self.w_dev_sum[f] += ad
            self.w_dev_n[f] += 1
            # a spike is a price the feed itself published at t; a lagging as-of value is only lag
            if ad > cfg.spike_bps and et == t:
                self.w_spikes[f] += 1
                ev = self.w_spike_ev[f]
                if ev is None or ad > abs(ev["dev_bps"]):
                    self.w_spike_ev[f] = {"symbol": s, "price": round(mid, 4), "consensus": round(cons, 4),
                                          "dev_bps": round(dev, 1)}
            x = clip if dev > clip else (-clip if dev < -clip else dev)
            pos = self.cusum_pos[key] + x - k_slack
            neg = self.cusum_neg[key] - x - k_slack
            self.cusum_pos[key] = 0.0 if pos < 0 else (cap if pos > cap else pos)
            self.cusum_neg[key] = 0.0 if neg < 0 else (cap if neg > cap else neg)
            self.bias[key] += 0.2 * (dev - self.bias[key])
            if size_n >= 2:
                self.w_size_n[f] += 1
                if (bsz, asz) != size_major:
                    self.w_size_mis[f] += 1
            # frozen / stale bookkeeping
            if mid != self.last_val[key]:
                self.last_val[key] = mid
                self.val_change_t[key] = et
                self.n_since_val[key] = 0
            elif changed:
                # never convict a feed whose value currently agrees with the consensus
                self.n_since_val[key] = 0 if mid == cons else self.n_since_val[key] + 1
            if et != self.last_entry[key]:
                self.last_entry[key] = et
                self.n_since_msg[key] = 0
            elif changed:
                self.n_since_msg[key] += 1

    # ------------------------------------------------------------------ window summary
    def feed_summary(self, f: str, halted: set, now: int) -> dict:
        """Per-feed consensus evidence for the window that just closed."""
        cfg = self.cfg
        w = self.watermark
        frozen, frozen_for, frozen_changes = [], 0.0, 0
        stale, stale_for, soft_frozen, soft_stale, active = [], 0.0, 0, 0, 0
        drift = []
        cusum_max = 0.0
        fmin = int(cfg.frozen_min_s * NS)
        smin = int(cfg.stale_min_s * NS)
        judged = f in self.judged
        for s in self.symbols:
            key = (f, s)
            c = max(self.cusum_pos[key], self.cusum_neg[key])
            if c > cusum_max:
                cusum_max = c
            if s in halted or self.cons[s] is None:
                continue
            recently_active = w - self.cons_change_t[s] <= 5 * NS
            if recently_active:
                active += 1
            last_entry = self.last_entry[key]
            nv, nm = self.n_since_val[key], self.n_since_msg[key]
            if recently_active and nv >= 2:
                soft_frozen += 1
            if recently_active and nm >= 2:
                soft_stale += 1
            if last_entry is None:
                continue
            vt = self.val_change_t[key]
            # Stale: the feed stopped publishing the symbol while the consensus kept changing.
            # Frozen: the feed keeps publishing it (fresh messages) but the value never changes.
            if not judged:
                continue
            h = self.hist[key]
            latest_t = h.ts[-1] if h.ts else last_entry     # newest entry received, even after evaluation
            if nm >= cfg.stale_min_changes and w - latest_t >= smin:
                stale.append(s)
                stale_for = max(stale_for, (w - latest_t) / NS)
            elif (nv >= cfg.frozen_min_changes and w - vt >= fmin and last_entry - vt >= fmin
                  and h.mid and h.mid[-1] == self.last_val[key]):
                frozen.append(s)
                frozen_for = max(frozen_for, (w - vt) / NS)
                frozen_changes = max(frozen_changes, nv)
            if c > cfg.drift_h_bps and abs(self.bias[key]) >= cfg.drift_min_bias_bps:
                drift.append((s, round(self.bias[key], 1), round(c, 1)))
        n = self.w_dev_n[f]
        return {
            "dev_max": self.w_dev_max[f],
            "dev_mean": self.w_dev_sum[f] / n if n else 0.0,
            "evals": n,
            "spikes": self.w_spikes[f],
            "spike_ev": self.w_spike_ev[f],
            "size_mismatch_frac": self.w_size_mis[f] / self.w_size_n[f] if self.w_size_n[f] else 0.0,
            "frozen": frozen, "frozen_for": frozen_for, "frozen_changes": frozen_changes,
            "stale": stale, "stale_for": stale_for,
            "frozen_frac": soft_frozen / active if active else 0.0,
            "stale_frac": soft_stale / active if active else 0.0,
            "active_symbols": active,
            "drift": drift,
            "cusum_norm": cusum_max / cfg.drift_h_bps,
        }

    def asof_mid(self, f: str, s: str, t: int):
        h = self.hist.get((f, s))
        if not h or not h.ts:
            return None
        i = bisect_right(h.ts, t) - 1
        return h.mid[i] if i >= 0 else None
