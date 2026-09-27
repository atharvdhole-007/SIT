"""Train the AI layer and evaluate the whole detector on real data, split by time blocks.

    train   09:45-12:30  clean windows -> IsolationForest; Chaos Lab windows -> RandomForest classifier
    calib   12:30-13:15  clean windows -> anomaly threshold at the 99.5th percentile (false-alarm budget)
    test    13:30-16:00  clean run -> false alarms/hour; Chaos Lab run -> detection rate, time-to-detect,
                         correct fingerprint, collateral on healthy feeds; classifier confusion matrix;
                         ablation (rules / + consensus / + ML) on the identical fault schedule

Usage: python -m feedsentinel.ml.evaluate
"""
from __future__ import annotations

import json
import logging
import random
import time
from datetime import datetime, timezone

import joblib
import numpy as np

from ..config import ABLATION_CONFIGS, MODELS_DIR, NS, REPLAY, REPORTS_DIR
from ..data.lobster import SOURCE_LABEL, load_tape
from ..detect.features import FEATURES
from ..detect.ml import MODEL_FILE, Models, robust_scale
from ..refdata import SymbolDirectory
from ..runtime.replay import ReplaySession
from ..sim.chaos import SCENARIOS, random_params
from ..timeutil import parse_clock

log = logging.getLogger("feedsentinel.eval")
SLOT_S, GAP_S = 30, 20


def schedule(names, rounds, seed):
    rng = random.Random(seed)
    out = []
    for _ in range(rounds):
        order = list(names)
        rng.shuffle(order)
        for sc in order:
            sp = SCENARIOS[sc]
            feed = None if sp.group == "market" else rng.choice(("C", "C", "A", "B"))
            out.append((sc, feed, random_params(sc, rng)))
    return out


def run(tape, ref, start, end, sched, cfg=REPLAY, models=None, seed=1):
    s = ReplaySession(tape, ref, cfg, models=models, start=start, warmup_s=90, end=end, seed=seed,
                      chaos_seed=seed + 100, record_windows=True, chart=False)
    s.warmup()
    t_end = parse_clock(tape.day, end)
    for sc, feed, params in sched:
        if s.now + (SLOT_S + GAP_S) * NS > t_end:
            break
        f = s.inject(sc, feed, params=params, duration_s=SLOT_S if SCENARIOS[sc].duration_s is None else None)
        s.run_until(f.start_ns + SLOT_S * NS)
        if f.active:
            s.clear(f.id)
        s.run_until(s.now + GAP_S * NS)
    if not sched or cfg is REPLAY and all(SCENARIOS[x[0]].group == "market" for x in sched):
        s.run_until(t_end)
    return s


def labelled_windows(s):
    eps = [f for f in s.chaos.faults if f.feed is not None]
    X, y = [], []
    for end, feed, x, _state, _codes in s.engine.window_log:
        lab = "NORMAL"
        for f in eps:
            fe = f.ended_ns if f.ended_ns is not None else s.now
            if f.feed == feed and f.start_ns <= end - NS // 2 and fe >= end - NS // 2:
                lab = f.spec.cls or "NOVEL"
                break
        X.append(x)
        y.append(lab)
    return np.array(X), np.array(y)


def score_run(s):
    """Incident-level results for one Chaos Lab run."""
    incs = s.engine.incidents.all()
    grace = 5 * NS
    rows = {}
    matched_ids = set()
    market_alerts = market_events = collateral = 0
    for f in s.chaos.faults:
        sp = f.spec
        fe = f.ended_ns if f.ended_ns is not None else s.now
        if sp.group == "market":
            market_events += 1
            hits = [i for i in incs if f.start_ns <= i.opened_ns <= fe + grace]
            market_alerts += len(hits)
            matched_ids.update(i.id for i in hits if False)
            continue
        on_feed = [i for i in incs if i.feed == f.feed and f.start_ns <= i.opened_ns <= fe + grace]
        matched_ids.update(i.id for i in on_feed)
        collateral += sum(1 for i in incs if i.feed != f.feed and f.start_ns <= i.opened_ns <= fe)
        correct = [i for i in on_feed if i.code in sp.expected]
        r = rows.setdefault(f.scenario, {"fault": f.scenario, "label": sp.label, "group": sp.group,
                                         "episodes": 0, "detected": 0, "correct": 0, "ttd": []})
        r["episodes"] += 1
        if on_feed:
            r["detected"] += 1
            first = min(correct or on_feed, key=lambda i: i.opened_ns)
            r["ttd"].append((first.opened_ns - f.start_ns) / NS)
        if correct:
            r["correct"] += 1
    return rows, incs, matched_ids, market_events, market_alerts, collateral


def finish_rows(rows):
    out = []
    for r in rows.values():
        t = sorted(r.pop("ttd"))
        c = r.pop("correct")
        r["detection_rate"] = round(r["detected"] / r["episodes"], 3)
        r["correct_code_rate"] = round(c / r["episodes"], 3)
        r["ttd_p50_s"] = round(float(np.percentile(t, 50)), 2) if t else None
        r["ttd_p95_s"] = round(float(np.percentile(t, 95)), 2) if t else None
        out.append(r)
    order = list(SCENARIOS)
    return sorted(out, key=lambda r: order.index(r["fault"]))


def main():
    logging.basicConfig(level=logging.WARNING)
    t0 = time.time()
    tape, ref = load_tape(), SymbolDirectory.load()
    faults = [k for k, v in SCENARIOS.items() if v.group in ("fault", "incident")]
    everything = [k for k, v in SCENARIOS.items()]
    # ---------------- train
    clean_tr = run(tape, ref, "09:45", "11:00", [])
    chaos_tr = run(tape, ref, "11:00", "12:30", schedule(faults + ["halt", "market_move"], 2, 11), seed=2)
    Xc, _ = labelled_windows(clean_tr)
    Xl, yl = labelled_windows(chaos_tr)
    calib = run(tape, ref, "12:30", "13:15", [], seed=3)
    Xv, _ = labelled_windows(calib)
    from sklearn.ensemble import IsolationForest, RandomForestClassifier
    iso = IsolationForest(n_estimators=150, random_state=0).fit(Xc)
    thr = float(np.percentile(-iso.score_samples(Xv), REPLAY.ml_percentile))
    keep = yl != "NOVEL"
    clf = RandomForestClassifier(n_estimators=120, max_depth=14, class_weight="balanced", random_state=0,
                                 n_jobs=-1).fit(np.vstack([Xl[keep], Xc]), np.concatenate([yl[keep], ["NORMAL"] * len(Xc)]))
    med, scale = robust_scale(np.vstack([Xc, Xv]))
    bundle = {"features": FEATURES, "iso": iso, "threshold": thr, "percentile": REPLAY.ml_percentile, "clf": clf,
              "labels": list(clf.classes_), "med": med, "scale": scale,
              "meta": {"trained_at": datetime.now(timezone.utc).isoformat(), "clean_windows": len(Xc),
                       "labelled_windows": int(keep.sum())}}
    MODELS_DIR.mkdir(exist_ok=True)
    joblib.dump(bundle, MODEL_FILE)
    models = Models(bundle)
    print(f"trained: {len(Xc)} clean windows, {int(keep.sum())} labelled; threshold {thr:.3f} ({time.time()-t0:.0f}s)")
    # ---------------- test
    clean_te = run(tape, ref, "13:30", "14:30", schedule(["halt", "market_move"], 3, 5), models=models, seed=4)
    hours = (clean_te.now - clean_te.t_start) / NS / 3600
    _, incs_c, _, mev, malerts, _ = score_run(clean_te)
    by_feed = {f: sum(1 for i in incs_c if i.feed == f) for f in clean_te.engine.feed_ids}
    sched = schedule(everything, 2, 21)
    results, ablation_rates, ablation_fa = {}, [], []
    for name, layers in ABLATION_CONFIGS.items():
        cfg = REPLAY.with_layers(layers)
        s = run(tape, ref, "14:30", "16:00", sched, cfg=cfg, models=models if "L4" in layers else None, seed=6)
        rows, incs, matched, _, m_al, coll = score_run(s)
        results[name] = (s, rows, incs, matched, coll)
        tot = sum(r["episodes"] for r in rows.values())
        ablation_rates.append(round(sum(r["correct"] for r in rows.values()) / max(1, tot), 3))
        ablation_fa.append(len([i for i in incs if i.id not in matched]))
        print(f"{name}: correct-code detection {ablation_rates[-1]:.0%} ({time.time()-t0:.0f}s)")
    s, rows, incs, matched, coll = results["rules+consensus+ml"]
    abl_rows = []
    for sc in SCENARIOS:
        if SCENARIOS[sc].group == "market":
            continue
        rates = []
        for name in ABLATION_CONFIGS:
            r = results[name][1].get(sc)
            rates.append(round(r["correct"] / r["episodes"], 2) if r else None)
        abl_rows.append({"fault": sc, "label": SCENARIOS[sc].label, "rates": rates})
    Xt, yt = labelled_windows(s)
    kt = yt != "NOVEL"
    pred = models.clf.predict(Xt[kt])
    labels = list(models.labels)
    from sklearn.metrics import confusion_matrix, precision_recall_fscore_support
    cm = confusion_matrix(yt[kt], pred, labels=labels)
    p, r_, f1, sup = precision_recall_fscore_support(yt[kt], pred, labels=labels, zero_division=0)
    fault_rows = finish_rows(rows)
    scores_clean = -models.iso.score_samples(labelled_windows(clean_te)[0])
    metrics = {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "data": {"source": SOURCE_LABEL, "train": "09:45-12:30", "validation": "12:30-13:15",
                 "test": "13:30-16:00"},
        "clean": {"market_hours": round(hours, 2), "false_alarms": len(incs_c),
                  "false_alarms_per_hour": round(len(incs_c) / hours, 2), "by_feed": by_feed,
                  "market_events": mev, "market_event_alerts": malerts},
        "faults": fault_rows,
        "incidents": {"total": len(incs), "matched": len(matched & {i.id for i in incs}),
                      "precision": round(len(matched & {i.id for i in incs}) / max(1, len(incs)), 3),
                      "collateral_on_healthy_feeds": coll},
        "classifier": {"labels": labels, "confusion": cm.tolist(),
                       "per_class": [{"label": l, "precision": round(float(a), 3), "recall": round(float(b), 3),
                                      "f1": round(float(c), 3), "support": int(d)}
                                     for l, a, b, c, d in zip(labels, p, r_, f1, sup)],
                       "macro_f1": round(float(np.mean(f1[sup > 0])), 3),
                       "accuracy": round(float((pred == yt[kt]).mean()), 3)},
        "anomaly": {"threshold": round(thr, 4), "percentile": REPLAY.ml_percentile, "clean_windows": len(Xc),
                    "flag_rate_clean": round(float((scores_clean > thr).mean()), 4)},
        "ablation": {"configs": list(ABLATION_CONFIGS), "rows": abl_rows, "overall": ablation_rates,
                     "false_alarms_per_hour": [round(x / 1.5, 2) for x in ablation_fa]},
        "throughput": {"events_per_s": round(s.engine.processed / max(1e-9, s.busy_s))},
    }
    REPORTS_DIR.mkdir(exist_ok=True)
    (REPORTS_DIR / "metrics.json").write_text(json.dumps(metrics, indent=2), encoding="utf-8")
    print(json.dumps({k: metrics[k] for k in ("clean", "incidents", "anomaly", "throughput")}, indent=1))
    for r in fault_rows:
        print(f"  {r['fault']:22s} det {r['detection_rate']:.0%} code {r['correct_code_rate']:.0%} "
              f"ttd p50 {r['ttd_p50_s']} p95 {r['ttd_p95_s']} (n={r['episodes']})")
    print(f"classifier accuracy {metrics['classifier']['accuracy']}, macro F1 {metrics['classifier']['macro_f1']}; "
          f"total {time.time()-t0:.0f}s")


if __name__ == "__main__":
    main()
