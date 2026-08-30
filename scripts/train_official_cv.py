from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd

from bortrace.baseline_model import (
    BinaryLogisticRegression,
    SoftmaxRegression,
    binary_log_loss,
)
from bortrace.paths import OUTPUT_DIR
from predict_official_models import load_dataset
from train_official_models import (
    evaluate_position_model,
    greedy_order_from_position_probs,
    select_official_features,
)


BINARY_TARGETS = ["upset", "is_high_payout_5000", "is_high_payout_10000", "is_high_payout_30000"]


def prepare_dataset(df: pd.DataFrame) -> pd.DataFrame:
    needed_targets = [f"target_pos{i}" for i in range(1, 7)]
    df = df.dropna(subset=needed_targets + ["winner", "upset"]).copy()
    for col in needed_targets:
        df[col] = df[col].astype(int)
    df["upset"] = df["upset"].astype(int)
    return df


def train_fold(
    df: pd.DataFrame,
    feature_columns: list[str],
    valid_year: int,
    out_dir: Path,
    epochs: int,
) -> dict[str, object]:
    train_df = df[df["year"] < valid_year].copy()
    valid_df = df[df["year"] == valid_year].copy()
    if train_df.empty or valid_df.empty:
        raise SystemExit(f"Bad fold valid_year={valid_year}: train={len(train_df)} valid={len(valid_df)}")

    fold_dir = out_dir / f"fold_valid_{valid_year}"
    fold_dir.mkdir(parents=True, exist_ok=True)

    metrics: dict[str, object] = {
        "valid_year": valid_year,
        "train_years": sorted(int(year) for year in train_df["year"].dropna().unique()),
        "rows_train": int(len(train_df)),
        "rows_valid": int(len(valid_df)),
        "feature_count": int(len(feature_columns)),
        "position_models": {},
        "binary_models": {},
    }

    valid_position_probs: dict[str, np.ndarray] = {}
    for pos in range(1, 7):
        target = f"target_pos{pos}"
        print(f"fold={valid_year} train {target}", flush=True)
        model = SoftmaxRegression.fit(train_df, feature_columns, target_col=target, epochs=epochs)
        model.save(fold_dir / f"finish_pos{pos}_softmax.npz")
        valid_position_probs[f"pos{pos}"] = model.predict_proba(valid_df)
        metrics["position_models"][f"pos{pos}"] = {
            "valid": evaluate_position_model(model, valid_df, target),
        }

    valid_order = greedy_order_from_position_probs(valid_position_probs)
    actual_valid = valid_df[[f"target_pos{i}" for i in range(1, 7)]].to_numpy(dtype=int)
    metrics["valid_order_exact_all6"] = float((valid_order == actual_valid).all(axis=1).mean())
    metrics["valid_order_top3_exact"] = float((valid_order[:, :3] == actual_valid[:, :3]).all(axis=1).mean())

    for target in BINARY_TARGETS:
        if target not in df.columns:
            continue
        target_train = train_df.dropna(subset=[target]).copy()
        target_valid = valid_df.dropna(subset=[target]).copy()
        if target_train.empty or target_valid.empty:
            continue
        target_train[target] = target_train[target].astype(int)
        target_valid[target] = target_valid[target].astype(int)
        print(f"fold={valid_year} train {target}", flush=True)
        model = BinaryLogisticRegression.fit(target_train, feature_columns, target_col=target, epochs=epochs)
        model.save(fold_dir / f"{target}_logistic.npz")
        valid_prob = model.predict_proba(target_valid)
        metrics["binary_models"][target] = {
            "valid": {
                "log_loss": binary_log_loss(target_valid[target].to_numpy(dtype=float), valid_prob),
                "brier": float(np.mean((valid_prob - target_valid[target].to_numpy(dtype=float)) ** 2)),
                "actual_rate": float(target_valid[target].mean()),
                "avg_pred": float(valid_prob.mean()),
            }
        }

    (fold_dir / "metrics.json").write_text(json.dumps(metrics, ensure_ascii=False, indent=2), encoding="utf-8")
    return metrics


def flatten_summary(metrics: dict[str, object]) -> dict[str, float | int | str]:
    row: dict[str, float | int | str] = {
        "valid_year": int(metrics["valid_year"]),
        "train_years": ",".join(map(str, metrics["train_years"])),
        "rows_train": int(metrics["rows_train"]),
        "rows_valid": int(metrics["rows_valid"]),
        "valid_order_exact_all6": float(metrics["valid_order_exact_all6"]),
        "valid_order_top3_exact": float(metrics["valid_order_top3_exact"]),
    }
    for pos, item in metrics["position_models"].items():
        valid = item["valid"]
        row[f"{pos}_accuracy"] = float(valid["accuracy"])
        row[f"{pos}_log_loss"] = float(valid["log_loss"])
    for target, item in metrics["binary_models"].items():
        valid = item["valid"]
        row[f"{target}_log_loss"] = float(valid["log_loss"])
        row[f"{target}_brier"] = float(valid["brier"])
        row[f"{target}_actual_rate"] = float(valid["actual_rate"])
        row[f"{target}_avg_pred"] = float(valid["avg_pred"])
    return row


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset", default=str(OUTPUT_DIR / "official_race_dataset.csv"))
    parser.add_argument("--dataset-dir", default=None)
    parser.add_argument("--start-month", default="202101")
    parser.add_argument("--end-month", default="202512")
    parser.add_argument("--out-dir", default=str(OUTPUT_DIR / "official_cv"))
    parser.add_argument("--valid-years", nargs="+", type=int, default=[2023, 2024, 2025])
    parser.add_argument("--epochs", type=int, default=700)
    args = parser.parse_args()

    df = prepare_dataset(load_dataset(args))
    feature_columns = select_official_features(df)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    pd.Series(feature_columns, name="feature").to_csv(out_dir / "features.csv", index=False, encoding="utf-8")

    all_metrics = []
    for valid_year in args.valid_years:
        all_metrics.append(train_fold(df, feature_columns, valid_year, out_dir, args.epochs))

    summary = pd.DataFrame([flatten_summary(metrics) for metrics in all_metrics])
    summary.to_csv(out_dir / "cv_summary.csv", index=False, encoding="utf-8")
    (out_dir / "cv_metrics.json").write_text(json.dumps(all_metrics, ensure_ascii=False, indent=2), encoding="utf-8")
    print(summary.to_string(index=False), flush=True)
    print(f"out_dir={out_dir}", flush=True)


if __name__ == "__main__":
    main()
