from __future__ import annotations

from dataclasses import dataclass
from typing import Any
import warnings

import numpy as np
import pandas as pd


BOAT_METRICS = (
    "class_val", "age", "entry_weight", "f_count", "l_count", "avg_st",
    "national_win_rate", "national_quinella_rate", "national_trio_rate",
    "local_win_rate", "local_quinella_rate", "local_trio_rate",
    "motor_quinella_rate", "motor_trio_rate", "boat_quinella_rate",
    "boat_trio_rate", "weight", "ex_time", "tilt", "start_ex_course",
    "start_ex_st_by_course",
    "gender_val", "is_female", "height", "ability_index",
    "ability_index_previous", "training_term", "class_val_lag1",
    "class_val_lag2", "class_val_lag3", "historical_course_entries",
    "historical_course_quinella_rate", "historical_course_avg_st",
    "historical_course_avg_st_rank", "historical_course_win_rate",
    "historical_course_top3_rate", "historical_course_incident_rate",
)
RACE_FEATURES = (
    "month", "jcd", "rno", "grade_val", "event_day", "event_days_total", "is_final_day",
    "race_distance", "is_qualifying", "is_semifinal", "is_final",
    "is_selection", "is_special_selection", "is_general_race",
    "is_womens_event", "is_rookie_event", "is_senior_event",
    "is_fixed_entry", "uses_stabilizer", "deadline_hour", "deadline_minute",
    "deadline_minutes", "weather_asof_rno", "weather_code", "air_temperature",
    "water_temperature", "wind_speed", "wind_direction_code",
    "course_direction_code", "wave_height",
    "female_count", "is_all_female", "is_mixed_gender",
)


def build_boat_ranking_features(frame: pd.DataFrame) -> pd.DataFrame:
    """Convert one wide race row into six boat rows with race-relative features."""
    n = len(frame)
    out: dict[str, np.ndarray] = {
        "boat": np.tile(np.arange(1, 7, dtype=float), n),
        "lane_from_inside": np.tile(np.arange(0, 6, dtype=float) / 5.0, n),
        "is_inner_3": np.tile(np.array([1, 1, 1, 0, 0, 0], dtype=float), n),
    }
    for col in RACE_FEATURES:
        if col in frame.columns:
            out[col] = np.repeat(pd.to_numeric(frame[col], errors="coerce").to_numpy(), 6)

    def repeated_numeric(name: str) -> np.ndarray | None:
        if name not in frame.columns:
            return None
        return np.repeat(pd.to_numeric(frame[name], errors="coerce").to_numpy(dtype=float), 6)

    month = repeated_numeric("month")
    if month is not None:
        out["month_sin"] = np.sin(2 * np.pi * (month - 1) / 12)
        out["month_cos"] = np.cos(2 * np.pi * (month - 1) / 12)
    deadline = repeated_numeric("deadline_minutes")
    if deadline is not None:
        out["deadline_time_sin"] = np.sin(2 * np.pi * deadline / (24 * 60))
        out["deadline_time_cos"] = np.cos(2 * np.pi * deadline / (24 * 60))
    event_day = repeated_numeric("event_day")
    event_total = repeated_numeric("event_days_total")
    if event_day is not None and event_total is not None:
        out["event_progress"] = np.divide(
            event_day, event_total, out=np.full_like(event_day, np.nan), where=event_total > 0
        )
    air = repeated_numeric("air_temperature")
    water = repeated_numeric("water_temperature")
    if air is not None and water is not None:
        out["air_minus_water_temperature"] = air - water
    race_no = repeated_numeric("rno")
    weather_asof = repeated_numeric("weather_asof_rno")
    if race_no is not None and weather_asof is not None:
        out["weather_age_races"] = race_no - weather_asof
    for name in ("wind_direction_code", "course_direction_code"):
        direction = repeated_numeric(name)
        if direction is not None:
            # Official icon codes form a 16-direction circle.
            out[f"{name}_sin"] = np.sin(2 * np.pi * direction / 16)
            out[f"{name}_cos"] = np.cos(2 * np.pi * direction / 16)

    if "jcd" in frame.columns:
        jcd = pd.to_numeric(frame["jcd"], errors="coerce").fillna(-1).astype(int).to_numpy()
        boats = np.tile(np.arange(1, 7, dtype=int), n)
        repeated_jcd = np.repeat(jcd, 6)
        out["jcd_boat_code"] = repeated_jcd * 10 + boats

    for metric in BOAT_METRICS:
        cols = [f"{metric}_{boat}" for boat in range(1, 7)]
        if not all(col in frame.columns for col in cols):
            continue
        values = frame[cols].apply(pd.to_numeric, errors="coerce").to_numpy(dtype=float)
        flat = values.reshape(-1)
        # Some old races have no exhibition value for any of the six boats.
        # Keep those race-level aggregates missing without emitting one warning per metric.
        valid_count = np.isfinite(values).sum(axis=1)
        total = np.nansum(values, axis=1)
        mean = np.divide(total, valid_count, out=np.full(n, np.nan), where=valid_count > 0)
        centered = values - mean[:, None]
        variance = np.divide(
            np.nansum(centered * centered, axis=1),
            valid_count,
            out=np.full(n, np.nan),
            where=valid_count > 0,
        )
        std = np.sqrt(variance)
        filled_min = np.where(np.isfinite(values), values, np.inf)
        filled_max = np.where(np.isfinite(values), values, -np.inf)
        minimum = filled_min.min(axis=1)
        maximum = filled_max.max(axis=1)
        minimum[valid_count == 0] = np.nan
        maximum[valid_count == 0] = np.nan
        high_rank = pd.DataFrame(values).rank(axis=1, ascending=False, method="average").to_numpy().reshape(-1)
        low_rank = pd.DataFrame(values).rank(axis=1, ascending=True, method="average").to_numpy().reshape(-1)
        safe_std = np.where(std > 1e-9, std, np.nan)
        out[metric] = flat
        out[f"{metric}_minus_mean"] = (values - mean[:, None]).reshape(-1)
        out[f"{metric}_zscore"] = ((values - mean[:, None]) / safe_std[:, None]).reshape(-1)
        out[f"{metric}_rank_high"] = high_rank
        out[f"{metric}_rank_low"] = low_rank
        out[f"{metric}_race_mean"] = np.repeat(mean, 6)
        out[f"{metric}_race_range"] = np.repeat(maximum - minimum, 6)
        out[f"{metric}_missing"] = (~np.isfinite(flat)).astype(float)
        out[f"{metric}_race_missing_count"] = np.repeat(6 - valid_count, 6).astype(float)
        inner_count = np.isfinite(values[:, :3]).sum(axis=1)
        outer_count = np.isfinite(values[:, 3:]).sum(axis=1)
        inner_mean = np.divide(
            np.nansum(values[:, :3], axis=1), inner_count,
            out=np.full(n, np.nan), where=inner_count > 0,
        )
        outer_mean = np.divide(
            np.nansum(values[:, 3:], axis=1), outer_count,
            out=np.full(n, np.nan), where=outer_count > 0,
        )
        out[f"{metric}_inner_minus_outer"] = np.repeat(inner_mean - outer_mean, 6)
        inside_neighbor = np.column_stack([np.full(n, np.nan), values[:, :-1]]).reshape(-1)
        outside_neighbor = np.column_stack([values[:, 1:], np.full(n, np.nan)]).reshape(-1)
        out[f"{metric}_minus_inside_neighbor"] = flat - inside_neighbor
        out[f"{metric}_minus_outside_neighbor"] = flat - outside_neighbor

    # Convert before DataFrame construction. Pandas otherwise consolidates the
    # dict as float64 first (over 5 GiB for the expanded full training set) and
    # only then reaches the float32 cast below.
    compact = {name: np.asarray(values, dtype=np.float32) for name, values in out.items()}
    result = pd.DataFrame(compact, copy=False)
    if "start_ex_course" in result:
        result["start_course_minus_boat"] = result["start_ex_course"] - result["boat"]
        result["start_course_changed"] = (result["start_course_minus_boat"].abs() > 0.1).astype(float)
    if {"national_win_rate", "local_win_rate", "motor_quinella_rate", "ex_time"} <= set(result.columns):
        result["form_strength"] = (
            0.45 * result["national_win_rate"]
            + 0.25 * result["local_win_rate"]
            + 0.20 * result["motor_quinella_rate"]
            - 0.10 * result["ex_time"]
        )
    result = result.replace([np.inf, -np.inf], np.nan)
    # The full training set expands to more than 1.5 million boat rows.
    # float32 is sufficient for tree splits and roughly halves peak memory.
    return result.astype(np.float32, copy=False)


def ranking_relevance(frame: pd.DataFrame) -> np.ndarray:
    targets = frame[[f"target_pos{i}" for i in range(1, 7)]].to_numpy(dtype=int)
    relevance = np.zeros((len(frame), 6), dtype=int)
    for pos in range(6):
        relevance[np.arange(len(frame)), targets[:, pos]] = 5 - pos
    return relevance.reshape(-1)


def select_features_by_gain(
    model: Any,
    feature_columns: list[str],
    cumulative_gain: float = 0.995,
    min_features: int = 40,
    max_features: int = 180,
) -> tuple[list[str], pd.DataFrame]:
    gain = np.asarray(model.booster_.feature_importance(importance_type="gain"), dtype=float)
    importance = pd.DataFrame({"feature": feature_columns, "gain": gain}).sort_values("gain", ascending=False)
    total = float(gain.sum())
    importance["gain_fraction"] = importance["gain"] / total if total > 0 else 0.0
    importance["cumulative_gain"] = importance["gain_fraction"].cumsum()
    wanted = int((importance["cumulative_gain"] < cumulative_gain).sum()) + 1
    wanted = min(max(wanted, min_features), max_features, len(importance))
    selected = importance.head(wanted)["feature"].tolist()
    importance["selected"] = importance["feature"].isin(selected)
    return selected, importance


@dataclass
class RaceRankingModel:
    model: Any
    feature_columns: list[str]
    label: str = "lgbm_lambdarank"

    def predict_scores(self, frame: pd.DataFrame) -> np.ndarray:
        features = build_boat_ranking_features(frame)
        for col in self.feature_columns:
            if col not in features:
                features[col] = np.nan
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", message="X does not have valid feature names.*")
            scores = np.asarray(self.model.predict(features[self.feature_columns]), dtype=float)
        return scores.reshape(len(frame), 6)

    def predict_order(self, frame: pd.DataFrame) -> np.ndarray:
        # Sorting six boat scores is the order constraint: no duplicate or missing boat.
        return np.argsort(-self.predict_scores(frame), axis=1, kind="stable") + 1
