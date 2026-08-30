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
    log_loss,
)
from bortrace.paths import OUTPUT_DIR


LEAK_PREFIXES = (
    "target_pos",
    "finish_pos",
    "result_st",
)
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


def select_official_features(df: pd.DataFrame) -> list[str]:
    cols: list[str] = []
    for col in df.columns:
        if col in LEAK_COLUMNS or col.startswith(LEAK_PREFIXES) or col.startswith(ID_PREFIXES):
            continue
        if pd.api.types.is_numeric_dtype(df[col]):
            cols.append(col)
    return sorted(cols)


def evaluate_position_model(model: SoftmaxRegression, df: pd.DataFrame, target_col: str) -> dict[str, float]:
    probs = model.predict_proba(df)
    y = df[target_col].to_numpy(dtype=int)
    pred = probs.argmax(axis=1)
    return {
        "accuracy": float((pred == y).mean()),
        "log_loss": log_loss(y, probs),
    }


def greedy_order_from_position_probs(position_probs: dict[str, np.ndarray]) -> np.ndarray:
    n = next(iter(position_probs.values())).shape[0]
    order = np.full((n, 6), -1, dtype=int)
    used = np.zeros((n, 6), dtype=bool)
    for pos in range(1, 7):
        probs = position_probs[f"pos{pos}"].copy()
        probs[used] = -1.0
        pick = probs.argmax(axis=1)
        order[:, pos - 1] = pick
        used[np.arange(n), pick] = True
    return order


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset", default=str(OUTPUT_DIR / "official_race_dataset.csv"))
    parser.add_argument("--dataset-dir", default=None)
    parser.add_argument("--start-month", default=None)
    parser.add_argument("--end-month", default=None)
    parser.add_argument("--out-dir", default=str(OUTPUT_DIR / "official_models"))
    parser.add_argument("--train-through", type=int, default=2024)
    parser.add_argument("--valid-year", type=int, default=2025)
    parser.add_argument("--test-year", type=int, default=2026)
    parser.add_argument("--epochs", type=int, default=700)
    args = parser.parse_args()

    dataset_path = Path(args.dataset)
    if args.dataset_dir:
        part_dir = Path(args.dataset_dir)
        parts = sorted(part_dir.glob("official_race_dataset_*.csv"))
        if args.start_month or args.end_month:
            filtered_parts = []
            for part in parts:
                month = part.stem.rsplit("_", 1)[-1]
                if args.start_month and month < args.start_month:
                    continue
                if args.end_month and month > args.end_month:
                    continue
                filtered_parts.append(part)
            parts = filtered_parts
        if not parts:
            raise SystemExit(f"No monthly dataset parts found: {part_dir}")
        frames = [pd.read_csv(part) for part in parts if part.stat().st_size > 1000]
        if not frames:
            raise SystemExit(f"No readable dataset parts found: {part_dir}")
        df = pd.concat(frames, ignore_index=True, sort=False)
        dataset_label = str(part_dir)
    else:
        if not dataset_path.exists():
            raise SystemExit(f"Dataset not found: {dataset_path}")
        df = pd.read_csv(dataset_path)
        dataset_label = str(dataset_path)
    needed_targets = [f"target_pos{i}" for i in range(1, 7)]
    df = df.dropna(subset=needed_targets + ["winner", "upset"]).copy()
    for col in needed_targets:
        df[col] = df[col].astype(int)
    df["upset"] = df["upset"].astype(int)

    feature_columns = select_official_features(df)
    train_df = df[df["year"] <= args.train_through].copy()
    valid_df = df[df["year"] == args.valid_year].copy()
    test_df = df[df["year"] == args.test_year].copy()
    if train_df.empty or valid_df.empty:
        raise SystemExit(f"Bad split: train={len(train_df)} valid={len(valid_df)} test={len(test_df)}")

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    metrics: dict[str, object] = {
        "dataset": dataset_label,
        "rows_total": int(len(df)),
        "rows_train": int(len(train_df)),
        "rows_valid": int(len(valid_df)),
        "rows_test": int(len(test_df)),
        "feature_count": int(len(feature_columns)),
        "features": feature_columns,
        "position_models": {},
        "binary_models": {},
    }

    valid_position_probs: dict[str, np.ndarray] = {}
    test_position_probs: dict[str, np.ndarray] = {}
    for pos in range(1, 7):
        target = f"target_pos{pos}"
        model = SoftmaxRegression.fit(train_df, feature_columns, target_col=target, epochs=args.epochs)
        model.save(out_dir / f"finish_pos{pos}_softmax.npz")
        valid_position_probs[f"pos{pos}"] = model.predict_proba(valid_df)
        if not test_df.empty:
            test_position_probs[f"pos{pos}"] = model.predict_proba(test_df)
        metrics["position_models"][f"pos{pos}"] = {
            "valid": evaluate_position_model(model, valid_df, target),
            "test": evaluate_position_model(model, test_df, target) if not test_df.empty else None,
        }

    valid_order = greedy_order_from_position_probs(valid_position_probs)
    actual_valid = valid_df[[f"target_pos{i}" for i in range(1, 7)]].to_numpy(dtype=int)
    metrics["valid_order_exact_all6"] = float((valid_order == actual_valid).all(axis=1).mean())
    metrics["valid_order_top3_exact"] = float((valid_order[:, :3] == actual_valid[:, :3]).all(axis=1).mean())

    if not test_df.empty:
        test_order = greedy_order_from_position_probs(test_position_probs)
        actual_test = test_df[[f"target_pos{i}" for i in range(1, 7)]].to_numpy(dtype=int)
        metrics["test_order_exact_all6"] = float((test_order == actual_test).all(axis=1).mean())
        metrics["test_order_top3_exact"] = float((test_order[:, :3] == actual_test[:, :3]).all(axis=1).mean())

    for target in ["upset", "is_high_payout_5000", "is_high_payout_10000", "is_high_payout_30000"]:
        if target not in df.columns:
            continue
        target_train = train_df.dropna(subset=[target]).copy()
        target_valid = valid_df.dropna(subset=[target]).copy()
        target_test = test_df.dropna(subset=[target]).copy()
        if target_train.empty or target_valid.empty:
            continue
        target_train[target] = target_train[target].astype(int)
        target_valid[target] = target_valid[target].astype(int)
        model = BinaryLogisticRegression.fit(target_train, feature_columns, target_col=target, epochs=args.epochs)
        model.save(out_dir / f"{target}_logistic.npz")
        valid_prob = model.predict_proba(target_valid)
        binary_metrics: dict[str, object] = {
            "valid": {
                "log_loss": binary_log_loss(target_valid[target].to_numpy(dtype=float), valid_prob),
                "brier": float(np.mean((valid_prob - target_valid[target].to_numpy(dtype=float)) ** 2)),
                "actual_rate": float(target_valid[target].mean()),
                "avg_pred": float(valid_prob.mean()),
            }
        }
        if not target_test.empty:
            target_test[target] = target_test[target].astype(int)
            test_prob = model.predict_proba(target_test)
            binary_metrics["test"] = {
                "log_loss": binary_log_loss(target_test[target].to_numpy(dtype=float), test_prob),
                "brier": float(np.mean((test_prob - target_test[target].to_numpy(dtype=float)) ** 2)),
                "actual_rate": float(target_test[target].mean()),
                "avg_pred": float(test_prob.mean()),
            }
        metrics["binary_models"][target] = binary_metrics

    metrics_path = out_dir / "metrics.json"
    metrics_path.write_text(json.dumps(metrics, ensure_ascii=False, indent=2), encoding="utf-8")
    pd.Series(feature_columns, name="feature").to_csv(out_dir / "features.csv", index=False, encoding="utf-8")
    print(json.dumps({k: v for k, v in metrics.items() if k != "features"}, ensure_ascii=False, indent=2))
    print(f"out_dir={out_dir}")


if __name__ == "__main__":
    main()
