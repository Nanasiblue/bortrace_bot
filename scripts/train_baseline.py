from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np

from bortrace.baseline_model import SoftmaxRegression, feature_summary, log_loss, select_feature_columns
from bortrace.legacy_bridge import parse_legacy_dataset
from bortrace.paths import OUTPUT_DIR, WORK_DIR


def year_from_short_date(value: str) -> int:
    return 2000 + int(str(value)[:2])


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-year", type=int, default=2023)
    parser.add_argument("--end-year", type=int, default=2026)
    parser.add_argument("--train-through", type=int, default=2025)
    parser.add_argument("--test-year", type=int, default=2026)
    parser.add_argument("--epochs", type=int, default=900)
    args = parser.parse_args()

    df = parse_legacy_dataset(args.start_year, args.end_year)
    if df.empty:
        raise SystemExit("No parsed races.")

    df["year"] = df["date"].map(year_from_short_date)
    feature_columns = select_feature_columns(df)
    train_df = df[df["year"] <= args.train_through].copy()
    test_df = df[df["year"] == args.test_year].copy()

    if train_df.empty or test_df.empty:
        raise SystemExit(f"Bad split: train={len(train_df)}, test={len(test_df)}")

    model = SoftmaxRegression.fit(train_df, feature_columns, epochs=args.epochs)
    probs = model.predict_proba(test_df)
    y = test_df["target"].to_numpy(dtype=int)
    pred = probs.argmax(axis=1)
    in_jump_prob = 1.0 - probs[:, 0]
    upset_true = (test_df["winner"].to_numpy(dtype=int) != 1).astype(float)

    metrics = {
        "rows_total": int(len(df)),
        "rows_train": int(len(train_df)),
        "rows_test": int(len(test_df)),
        "features": feature_columns,
        "winner_accuracy": float((pred == y).mean()),
        "winner_log_loss": log_loss(y, probs),
        "upset_brier": float(np.mean((in_jump_prob - upset_true) ** 2)),
        "avg_in_jump_prob": float(in_jump_prob.mean()),
        "actual_upset_rate": float(upset_true.mean()),
    }

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    WORK_DIR.mkdir(parents=True, exist_ok=True)
    model_path = OUTPUT_DIR / "baseline_softmax_model.npz"
    metrics_path = OUTPUT_DIR / "baseline_metrics.json"
    feature_path = OUTPUT_DIR / "feature_summary.csv"

    model.save(model_path)
    metrics_path.write_text(json.dumps(metrics, ensure_ascii=False, indent=2), encoding="utf-8")
    feature_summary(df, feature_columns).to_csv(feature_path, index=False, encoding="utf-8")

    print(json.dumps({k: v for k, v in metrics.items() if k != "features"}, ensure_ascii=False, indent=2))
    print(f"model={model_path}")
    print(f"metrics={metrics_path}")
    print(f"features={feature_path}")


if __name__ == "__main__":
    main()
