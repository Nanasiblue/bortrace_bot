from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd


FEATURE_PATTERNS = [
    "wind_speed",
    "wave",
    "rank_val",
    "win_rate",
    "ex_time",
    "ex_diff",
    "ex_rank",
    "st_",
    "is_debuff_1",
]


def select_feature_columns(df: pd.DataFrame) -> list[str]:
    cols = []
    for col in df.columns:
        if any(pattern in col for pattern in FEATURE_PATTERNS):
            if pd.api.types.is_numeric_dtype(df[col]):
                cols.append(col)
    return sorted(set(cols))


@dataclass
class Standardizer:
    mean: np.ndarray
    scale: np.ndarray

    @classmethod
    def fit(cls, x: np.ndarray) -> "Standardizer":
        mean = np.nanmean(x, axis=0)
        scale = np.nanstd(x, axis=0)
        scale[scale == 0] = 1.0
        return cls(mean=mean, scale=scale)

    def transform(self, x: np.ndarray) -> np.ndarray:
        x = np.where(np.isnan(x), self.mean, x)
        return (x - self.mean) / self.scale


@dataclass
class SoftmaxRegression:
    weights: np.ndarray
    bias: np.ndarray
    standardizer: Standardizer
    feature_columns: list[str]

    @staticmethod
    def _softmax(z: np.ndarray) -> np.ndarray:
        z = z - z.max(axis=1, keepdims=True)
        exp = np.exp(z)
        return exp / exp.sum(axis=1, keepdims=True)

    @classmethod
    def fit(
        cls,
        df: pd.DataFrame,
        feature_columns: list[str],
        target_col: str = "target",
        num_classes: int = 6,
        epochs: int = 900,
        lr: float = 0.04,
        l2: float = 0.001,
    ) -> "SoftmaxRegression":
        x_raw = df[feature_columns].to_numpy(dtype=float)
        standardizer = Standardizer.fit(x_raw)
        x = standardizer.transform(x_raw)
        y = df[target_col].to_numpy(dtype=int)
        n, d = x.shape
        weights = np.zeros((d, num_classes), dtype=float)
        bias = np.zeros(num_classes, dtype=float)
        one_hot = np.eye(num_classes)[y]

        for _ in range(epochs):
            probs = cls._softmax(x @ weights + bias)
            grad = (probs - one_hot) / n
            weights -= lr * (x.T @ grad + l2 * weights)
            bias -= lr * grad.sum(axis=0)

        return cls(weights=weights, bias=bias, standardizer=standardizer, feature_columns=feature_columns)

    def predict_proba(self, df: pd.DataFrame) -> np.ndarray:
        x = self.standardizer.transform(df[self.feature_columns].to_numpy(dtype=float))
        return self._softmax(x @ self.weights + self.bias)

    def save(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        np.savez_compressed(
            path,
            weights=self.weights,
            bias=self.bias,
            mean=self.standardizer.mean,
            scale=self.standardizer.scale,
            feature_columns=np.array(self.feature_columns, dtype=object),
        )

    @classmethod
    def load(cls, path: Path) -> "SoftmaxRegression":
        data = np.load(path, allow_pickle=True)
        return cls(
            weights=data["weights"],
            bias=data["bias"],
            standardizer=Standardizer(mean=data["mean"], scale=data["scale"]),
            feature_columns=[str(col) for col in data["feature_columns"].tolist()],
        )


@dataclass
class BinaryLogisticRegression:
    weights: np.ndarray
    bias: float
    standardizer: Standardizer
    feature_columns: list[str]

    @staticmethod
    def _sigmoid(z: np.ndarray) -> np.ndarray:
        return 1.0 / (1.0 + np.exp(-np.clip(z, -40, 40)))

    @classmethod
    def fit(
        cls,
        df: pd.DataFrame,
        feature_columns: list[str],
        target_col: str,
        epochs: int = 900,
        lr: float = 0.04,
        l2: float = 0.001,
    ) -> "BinaryLogisticRegression":
        x_raw = df[feature_columns].to_numpy(dtype=float)
        standardizer = Standardizer.fit(x_raw)
        x = standardizer.transform(x_raw)
        y = df[target_col].to_numpy(dtype=float)
        n, d = x.shape
        weights = np.zeros(d, dtype=float)
        bias = 0.0

        for _ in range(epochs):
            probs = cls._sigmoid(x @ weights + bias)
            grad = (probs - y) / n
            weights -= lr * (x.T @ grad + l2 * weights)
            bias -= lr * float(grad.sum())

        return cls(weights=weights, bias=bias, standardizer=standardizer, feature_columns=feature_columns)

    def predict_proba(self, df: pd.DataFrame) -> np.ndarray:
        x = self.standardizer.transform(df[self.feature_columns].to_numpy(dtype=float))
        return self._sigmoid(x @ self.weights + self.bias)

    def save(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        np.savez_compressed(
            path,
            weights=self.weights,
            bias=np.array([self.bias], dtype=float),
            mean=self.standardizer.mean,
            scale=self.standardizer.scale,
            feature_columns=np.array(self.feature_columns, dtype=object),
        )

    @classmethod
    def load(cls, path: Path) -> "BinaryLogisticRegression":
        data = np.load(path, allow_pickle=True)
        return cls(
            weights=data["weights"],
            bias=float(data["bias"][0]),
            standardizer=Standardizer(mean=data["mean"], scale=data["scale"]),
            feature_columns=[str(col) for col in data["feature_columns"].tolist()],
        )


def log_loss(y_true: np.ndarray, probs: np.ndarray) -> float:
    clipped = np.clip(probs, 1e-9, 1.0)
    return float(-np.log(clipped[np.arange(len(y_true)), y_true]).mean())


def binary_log_loss(y_true: np.ndarray, probs: np.ndarray) -> float:
    clipped = np.clip(probs, 1e-9, 1.0 - 1e-9)
    return float(-(y_true * np.log(clipped) + (1.0 - y_true) * np.log(1.0 - clipped)).mean())


def feature_summary(df: pd.DataFrame, feature_columns: list[str]) -> pd.DataFrame:
    rows = []
    target = df["target"].to_numpy(dtype=float)
    for col in feature_columns:
        values = df[col].to_numpy(dtype=float)
        mask = np.isfinite(values)
        if mask.sum() < 3 or np.nanstd(values[mask]) == 0:
            corr = 0.0
        else:
            corr = float(np.corrcoef(values[mask], target[mask])[0, 1])
        rows.append(
            {
                "feature": col,
                "missing_rate": float(pd.isna(df[col]).mean()),
                "mean": float(np.nanmean(values)),
                "std": float(np.nanstd(values)),
                "winner_corr": corr,
            }
        )
    return pd.DataFrame(rows).sort_values("winner_corr", key=lambda s: s.abs(), ascending=False)
