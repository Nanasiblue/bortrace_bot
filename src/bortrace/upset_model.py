from __future__ import annotations

from dataclasses import dataclass
from typing import Any
import warnings

import numpy as np
import pandas as pd


LEAK_PREFIXES = ("target_pos", "finish_pos", "result_st")
LEAK_COLUMNS = {
    "winner",
    "payout_3t",
    "popularity_3t",
    "trifecta",
    "winning_method",
    "upset",
    "is_high_payout_5000",
    "is_high_payout_10000",
    "is_high_payout_30000",
}
ID_PREFIXES = ("reg_no_", "motor_no_", "boat_no_")


def select_live_numeric_features(df: pd.DataFrame) -> list[str]:
    features: list[str] = []
    for col in df.columns:
        if col in LEAK_COLUMNS or col.startswith(LEAK_PREFIXES) or col.startswith(ID_PREFIXES):
            continue
        if pd.api.types.is_numeric_dtype(df[col]):
            features.append(col)
    return sorted(features)


def ensure_features(frame: pd.DataFrame, feature_columns: list[str]) -> pd.DataFrame:
    out = frame.copy() if all(col in frame.columns for col in feature_columns) else add_upset_meta_features(frame)
    for col in feature_columns:
        if col not in out.columns:
            out[col] = np.nan
    return out[feature_columns]


def add_upset_meta_features(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    added: dict[str, pd.Series] = {}
    metrics = [
        "class_val",
        "avg_st",
        "national_win_rate",
        "national_quinella_rate",
        "national_trio_rate",
        "local_win_rate",
        "local_quinella_rate",
        "local_trio_rate",
        "motor_quinella_rate",
        "motor_trio_rate",
        "boat_quinella_rate",
        "boat_trio_rate",
        "entry_weight",
        "weight",
        "ex_time",
        "tilt",
        "start_ex_st_by_course",
    ]
    for metric in metrics:
        cols = [f"{metric}_{boat}" for boat in range(1, 7)]
        if not all(col in out.columns for col in cols):
            continue
        values = out[cols].astype(float)
        boat1 = values[cols[0]]
        others = values[cols[1:]]
        candidate_features = {
            f"{metric}_boat1_minus_outer_mean": boat1 - others.mean(axis=1),
            f"{metric}_boat1_minus_outer_best_high": boat1 - others.max(axis=1),
            f"{metric}_boat1_minus_outer_best_low": boat1 - others.min(axis=1),
            f"{metric}_outer_range": others.max(axis=1) - others.min(axis=1),
            f"{metric}_all_range": values.max(axis=1) - values.min(axis=1),
            f"{metric}_boat1_rank_high": values.rank(axis=1, ascending=False, method="min")[cols[0]],
            f"{metric}_boat1_rank_low": values.rank(axis=1, ascending=True, method="min")[cols[0]],
        }
        for feature, value in candidate_features.items():
            if feature not in out.columns:
                added[feature] = value
    if not added:
        return out
    return pd.concat([out, pd.DataFrame(added, index=out.index)], axis=1)


@dataclass
class LogisticProbabilityModel:
    weights: np.ndarray
    bias: float
    standardizer: Any

    def predict_proba(self, x: pd.DataFrame | np.ndarray) -> np.ndarray:
        arr = x.to_numpy(dtype=float) if isinstance(x, pd.DataFrame) else np.asarray(x, dtype=float)
        z = self.standardizer.transform(arr) @ self.weights + self.bias
        p = 1.0 / (1.0 + np.exp(-np.clip(z, -40, 40)))
        return np.column_stack([1.0 - p, p])


@dataclass
class UpsetProbabilityModel:
    model: Any
    feature_columns: list[str]
    label: str = "lgbm_csv_features"
    is_upset_probability_model: bool = True

    def predict_proba(self, frame: pd.DataFrame) -> np.ndarray:
        x = ensure_features(frame, self.feature_columns)
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", message="X does not have valid feature names.*")
            prob = np.asarray(self.model.predict_proba(x)[:, 1], dtype=float)
        prob = np.clip(prob, 0.0, 1.0)
        return prob
