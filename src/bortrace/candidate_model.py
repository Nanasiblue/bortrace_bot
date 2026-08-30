from __future__ import annotations

from dataclasses import dataclass
from typing import Any
import warnings

import numpy as np


@dataclass
class CalibratedProbabilityModel:
    base_model: Any
    calibrator: Any
    feature_columns: list[str]
    raw_clip_low: float = 0.0
    raw_clip_high: float = 1.0
    prob_floor: float = 0.0
    prob_ceiling: float = 1.0

    is_calibrated_probability_model: bool = True

    def predict_proba(self, x: Any) -> np.ndarray:
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", message="X does not have valid feature names.*")
            raw = np.asarray(self.base_model.predict_proba(x)[:, 1], dtype=float)
        raw = np.clip(raw, self.raw_clip_low, self.raw_clip_high)
        calibrated = np.asarray(self.calibrator.predict(raw), dtype=float)
        calibrated = np.clip(calibrated, self.prob_floor, self.prob_ceiling)
        return np.column_stack([1.0 - calibrated, calibrated])
