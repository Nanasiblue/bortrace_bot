from __future__ import annotations

import argparse
import json
import pickle
import sys
from pathlib import Path

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from bortrace.baseline_model import Standardizer
from bortrace.upset_model import LogisticProbabilityModel, UpsetProbabilityModel, add_upset_meta_features, select_live_numeric_features


def load_parts(dataset_dir: Path, start_month: str, end_month: str) -> pd.DataFrame:
    parts = []
    for path in sorted(dataset_dir.glob("official_race_dataset_*.csv")):
        month = path.stem.rsplit("_", 1)[-1]
        if start_month <= month <= end_month and path.stat().st_size > 1000:
            parts.append(pd.read_csv(path))
    if not parts:
        raise SystemExit(f"No dataset parts: {dataset_dir} {start_month}-{end_month}")
    return pd.concat(parts, ignore_index=True, sort=False)


def fit_logistic(
    x_raw: np.ndarray,
    y: np.ndarray,
    *,
    epochs: int,
    lr: float,
    l2: float,
    batch_size: int,
) -> LogisticProbabilityModel:
    standardizer = Standardizer.fit(x_raw)
    x = standardizer.transform(x_raw)
    n, d = x.shape
    w = np.zeros(d, dtype=float)
    b = float(np.log(y.mean() / max(1e-9, 1.0 - y.mean())))
    for epoch in range(epochs):
        p = 1.0 / (1.0 + np.exp(-np.clip(x @ w + b, -40, 40)))
        grad = (p - y) / n
        w -= lr * (x.T @ grad + l2 * w)
        b -= lr * float(grad.sum())
        if epoch and epoch % 50 == 0:
            lr *= 0.8
    return LogisticProbabilityModel(weights=w, bias=b, standardizer=standardizer)


def auc_score(y: np.ndarray, score: np.ndarray) -> float:
    order = np.argsort(score)
    ranks = np.empty_like(order, dtype=float)
    ranks[order] = np.arange(1, len(score) + 1)
    pos = y == 1
    n_pos = float(pos.sum())
    n_neg = float((~pos).sum())
    if n_pos == 0 or n_neg == 0:
        return float("nan")
    return float((ranks[pos].sum() - n_pos * (n_pos + 1) / 2) / (n_pos * n_neg))


def binary_log_loss(y: np.ndarray, p: np.ndarray) -> float:
    p = np.clip(p, 1e-9, 1 - 1e-9)
    return float(-(y * np.log(p) + (1 - y) * np.log(1 - p)).mean())


def metrics_for(y: np.ndarray, p: np.ndarray, threshold: float) -> dict[str, Any]:
    pred = p >= threshold
    tp = int(((pred == 1) & (y == 1)).sum())
    fp = int(((pred == 1) & (y == 0)).sum())
    fn = int(((pred == 0) & (y == 1)).sum())
    precision = tp / (tp + fp) if tp + fp else 0.0
    recall = tp / (tp + fn) if tp + fn else 0.0
    f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0
    return {
        "rows": int(len(y)),
        "actual_rate": float(y.mean()),
        "avg_pred": float(p.mean()),
        "auc": auc_score(y, p),
        "brier": float(((p - y) ** 2).mean()),
        "log_loss": binary_log_loss(y, p),
        "threshold": float(threshold),
        "accuracy": float((pred == y).mean()),
        "precision": float(precision),
        "recall": float(recall),
        "f1": float(f1),
        "positive_rate": float(pred.mean()),
    }


def threshold_table(df: pd.DataFrame, prob_col: str) -> pd.DataFrame:
    y = df["upset"].to_numpy(dtype=int)
    p = df[prob_col].to_numpy(dtype=float)
    return pd.DataFrame([metrics_for(y, p, t) for t in np.linspace(0.25, 0.75, 21)])


def level_table(df: pd.DataFrame, prob_col: str) -> pd.DataFrame:
    tmp = df[["upset", prob_col]].copy()
    tmp["level"] = pd.cut(tmp[prob_col], [-0.001, 0.30, 0.42, 0.54, 0.66, 1.001], labels=[1, 2, 3, 4, 5]).astype(int)
    return tmp.groupby("level", as_index=False).agg(races=("upset", "size"), pred_avg=(prob_col, "mean"), actual_rate=("upset", "mean"))


def monthly(df: pd.DataFrame, prob_col: str, threshold: float) -> pd.DataFrame:
    rows = []
    for month, group in df.groupby("yyyymm"):
        row = metrics_for(group["upset"].to_numpy(dtype=int), group[prob_col].to_numpy(dtype=float), threshold)
        row["month"] = month
        rows.append(row)
    return pd.DataFrame(rows)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset-dir", required=True)
    parser.add_argument("--start-month", default="202101")
    parser.add_argument("--end-month", default="202607")
    parser.add_argument("--train-end-month", default="202512")
    parser.add_argument("--test-start-month", default="202601")
    parser.add_argument("--test-end-month", default="202607")
    parser.add_argument("--out-dir", required=True)
    parser.add_argument("--epochs", type=int, default=180)
    parser.add_argument("--lr", type=float, default=0.08)
    parser.add_argument("--l2", type=float, default=0.002)
    parser.add_argument("--batch-size", type=int, default=8192)
    args = parser.parse_args()

    df = load_parts(Path(args.dataset_dir), args.start_month, args.end_month).dropna(subset=["upset"]).copy()
    df["date"] = df["date"].astype(str)
    df["yyyymm"] = df["date"].str.slice(0, 6)
    df["upset"] = df["upset"].astype(int)
    df = add_upset_meta_features(df)
    features = select_live_numeric_features(df)

    train = df[df["yyyymm"] <= args.train_end_month].copy()
    test = df[(df["yyyymm"] >= args.test_start_month) & (df["yyyymm"] <= args.test_end_month)].copy()
    if train.empty or test.empty:
        raise SystemExit(f"Bad split: train={len(train)} test={len(test)}")

    model = fit_logistic(
        train[features].to_numpy(dtype=float),
        train["upset"].to_numpy(dtype=float),
        epochs=args.epochs,
        lr=args.lr,
        l2=args.l2,
        batch_size=args.batch_size,
    )
    wrapped = UpsetProbabilityModel(model=model, feature_columns=features, label="logistic_plus_csv_features")
    train["upset_plus_prob"] = wrapped.predict_proba(train)
    test["upset_plus_prob"] = wrapped.predict_proba(test)
    thresholds = threshold_table(test, "upset_plus_prob")
    default_threshold = float(thresholds.sort_values(["f1", "precision"], ascending=[False, False]).iloc[0]["threshold"])

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    with (out_dir / "upset_model.pkl").open("wb") as f:
        pickle.dump(wrapped, f)
    thresholds.to_csv(out_dir / "thresholds_202601_202607.csv", index=False, encoding="utf-8")
    monthly(test, "upset_plus_prob", default_threshold).to_csv(out_dir / "monthly_metrics_202601_202607.csv", index=False, encoding="utf-8")
    level_table(test, "upset_plus_prob").to_csv(out_dir / "five_level_202601_202607.csv", index=False, encoding="utf-8")

    coef = pd.DataFrame({"feature": features, "coef": model.weights}).sort_values("coef", key=lambda s: s.abs(), ascending=False)
    coef.to_csv(out_dir / "feature_coefficients.csv", index=False, encoding="utf-8")
    test[["date", "jcd", "rno", "winner", "upset", "payout_3t", "trifecta", "upset_plus_prob"]].to_csv(
        out_dir / "predictions_202601_202607.csv", index=False, encoding="utf-8"
    )

    metrics = {
        "rows_train": int(len(train)),
        "rows_test": int(len(test)),
        "features": int(len(features)),
        "default_threshold": default_threshold,
        "train": metrics_for(train["upset"].to_numpy(dtype=int), train["upset_plus_prob"].to_numpy(dtype=float), default_threshold),
        "test": metrics_for(test["upset"].to_numpy(dtype=int), test["upset_plus_prob"].to_numpy(dtype=float), default_threshold),
        "top_coefficients": coef.head(40).to_dict(orient="records"),
    }
    (out_dir / "metrics.json").write_text(json.dumps(metrics, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({k: v for k, v in metrics.items() if k != "top_coefficients"}, ensure_ascii=False, indent=2))
    print(coef.head(30).to_string(index=False))
    print(f"out_dir={out_dir}")


if __name__ == "__main__":
    main()
