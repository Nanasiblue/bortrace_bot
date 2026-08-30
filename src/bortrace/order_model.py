from __future__ import annotations

from dataclasses import dataclass
from typing import Any
import warnings

import numpy as np
import pandas as pd

from .upset_model import add_upset_meta_features, ensure_features


@dataclass
class PositionProbabilityModel:
    models: dict[str, Any]
    feature_columns: list[str]
    label: str = "position_model"

    def predict_position_probs(self, frame: pd.DataFrame) -> dict[str, np.ndarray]:
        x = ensure_features(add_upset_meta_features(frame), self.feature_columns)
        probs: dict[str, np.ndarray] = {}
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", message="X does not have valid feature names.*")
            for pos, model in self.models.items():
                probs[pos] = np.asarray(model.predict_proba(x), dtype=float)
        return probs

