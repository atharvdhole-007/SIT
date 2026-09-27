"""L4 AI: an anomaly model trained only on healthy behaviour, and a fault-type classifier.

- IsolationForest on clean windows. Its alarm threshold is calibrated to a false-alarm budget: the
  99.5th percentile of scores on held-out clean windows, not "flag 5% of everything".
- RandomForest trained on windows labelled by fault injection, to name the failure so the operator
  knows the action (retransmit for a gap, fail over for a freeze, block for a test leak).
- Explanations: robust z-scores of each feature against clean data, top 3 reported.
"""
from __future__ import annotations

import logging
from pathlib import Path

import numpy as np

from ..config import MODELS_DIR
from .features import FEATURE_TEXT, FEATURES

log = logging.getLogger("feedsentinel.ml")

MODEL_FILE = MODELS_DIR / "feedsentinel_models.joblib"


class Models:
    def __init__(self, bundle: dict):
        if tuple(bundle.get("features", ())) != FEATURES:
            raise ValueError("model was trained on a different feature set; retrain it")
        self.bundle = bundle
        self.iso = bundle.get("iso")
        self.threshold = float(bundle.get("threshold", np.inf))
        self.percentile = bundle.get("percentile")
        self.clf = bundle.get("clf")
        self.labels = list(bundle.get("labels", []))
        self.med = np.asarray(bundle["med"], dtype=np.float64)
        self.scale = np.asarray(bundle["scale"], dtype=np.float64)
        self.meta = bundle.get("meta", {})
        for est in (self.iso, self.clf):
            if est is not None and hasattr(est, "n_jobs"):
                est.n_jobs = 1

    @property
    def has_anomaly(self) -> bool:
        return self.iso is not None

    @property
    def has_classifier(self) -> bool:
        return self.clf is not None

    @classmethod
    def load(cls, path: Path = MODEL_FILE) -> "Models | None":
        if not Path(path).exists():
            log.warning("no trained models at %s (run: python -m feedsentinel train)", path)
            return None
        import joblib
        try:
            return cls(joblib.load(path))
        except Exception as exc:  # corrupt or incompatible file: run without ML rather than crash
            log.error("could not load models from %s: %s", path, exc)
            return None

    def anomaly_scores(self, X: np.ndarray) -> np.ndarray:
        """Higher = more anomalous."""
        return -self.iso.score_samples(X)

    def classify(self, X: np.ndarray) -> list[dict]:
        proba = self.clf.predict_proba(X)
        out = []
        for row in proba:
            order = np.argsort(row)[::-1]
            out.append({"label": self.labels[order[0]], "p": round(float(row[order[0]]), 3),
                        "alternatives": [{"label": self.labels[j], "p": round(float(row[j]), 3)}
                                         for j in order[1:3] if row[j] > 0.01]})
        return out

    def zscores(self, x: np.ndarray) -> np.ndarray:
        return (x - self.med) / self.scale

    def top_features(self, x: np.ndarray, k: int = 3) -> list[dict]:
        z = self.zscores(x)
        order = np.argsort(-np.abs(z))[:k]
        return [{"name": FEATURES[i], "label": FEATURE_TEXT[FEATURES[i]],
                 "value": round(float(x[i]), 3), "baseline": round(float(self.med[i]), 3),
                 "z": round(float(np.clip(z[i], -99, 99)), 1)} for i in order if abs(z[i]) >= 1.0]


def robust_scale(X: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    """Median and a scale that never collapses to zero (features that are always 0 when healthy)."""
    med = np.median(X, axis=0)
    mad = np.median(np.abs(X - med), axis=0) * 1.4826
    std = X.std(axis=0)
    scale = np.maximum.reduce([mad, 0.25 * std, np.full(X.shape[1], 0.05)])
    return med, scale
