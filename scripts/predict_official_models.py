from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd

from bortrace.baseline_model import BinaryLogisticRegression, SoftmaxRegression
from bortrace.paths import OUTPUT_DIR
from train_official_models import greedy_order_from_position_probs


BINARY_TARGETS = ["upset", "is_high_payout_5000", "is_high_payout_10000", "is_high_payout_30000"]


def load_dataset(args: argparse.Namespace) -> pd.DataFrame:
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
        frames = [pd.read_csv(part) for part in parts if part.stat().st_size > 1000]
        if not frames:
            raise SystemExit(f"No readable dataset parts found: {part_dir}")
        return pd.concat(frames, ignore_index=True, sort=False)

    dataset_path = Path(args.dataset)
    if not dataset_path.exists():
        raise SystemExit(f"Dataset not found: {dataset_path}")
    return pd.read_csv(dataset_path)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset", default=str(OUTPUT_DIR / "official_race_dataset.csv"))
    parser.add_argument("--dataset-dir", default=None)
    parser.add_argument("--start-month", default=None)
    parser.add_argument("--end-month", default=None)
    parser.add_argument("--models-dir", default=str(OUTPUT_DIR / "official_models"))
    parser.add_argument("--out", default=str(OUTPUT_DIR / "official_predictions.csv"))
    parser.add_argument("--year", type=int, default=None)
    parser.add_argument("--date", default=None)
    parser.add_argument("--limit", type=int, default=None)
    args = parser.parse_args()

    df = load_dataset(args)
    if args.year is not None:
        df = df[df["year"] == args.year].copy()
    if args.date:
        df = df[df["date"].astype(str) == args.date].copy()
    if args.limit:
        df = df.head(args.limit).copy()
    if df.empty:
        raise SystemExit("No rows to predict")

    models_dir = Path(args.models_dir)
    position_probs: dict[str, np.ndarray] = {}
    out = df[["date", "jcd", "rno"]].copy()

    for pos in range(1, 7):
        model_path = models_dir / f"finish_pos{pos}_softmax.npz"
        if not model_path.exists():
            raise SystemExit(f"Model not found: {model_path}")
        model = SoftmaxRegression.load(model_path)
        probs = model.predict_proba(df)
        position_probs[f"pos{pos}"] = probs
        pick = probs.argmax(axis=1) + 1
        out[f"pred_pos{pos}"] = pick
        out[f"pred_pos{pos}_prob"] = probs.max(axis=1)

    order = greedy_order_from_position_probs(position_probs) + 1
    out["pred_order"] = ["-".join(map(str, row)) for row in order]
    out["pred_trifecta"] = ["-".join(map(str, row[:3])) for row in order]

    target_cols = [f"target_pos{i}" for i in range(1, 7)]
    if all(col in df.columns for col in target_cols):
        actual = df[target_cols].to_numpy(dtype=float)
        mask = np.isfinite(actual).all(axis=1)
        actual_order = np.full(actual.shape, -1, dtype=int)
        actual_order[mask] = actual[mask].astype(int) + 1
        out["actual_order"] = [("-".join(map(str, row)) if ok else "") for row, ok in zip(actual_order, mask)]
        out["hit_winner"] = np.where(mask, order[:, 0] == actual_order[:, 0], np.nan)
        out["hit_top3_exact"] = np.where(mask, (order[:, :3] == actual_order[:, :3]).all(axis=1), np.nan)

    for target in BINARY_TARGETS:
        model_path = models_dir / f"{target}_logistic.npz"
        if model_path.exists():
            model = BinaryLogisticRegression.load(model_path)
            out[f"{target}_prob"] = model.predict_proba(df)

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(out_path, index=False, encoding="utf-8")
    print(f"rows={len(out)}")
    print(f"out={out_path}")


if __name__ == "__main__":
    main()
