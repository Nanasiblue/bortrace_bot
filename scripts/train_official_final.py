from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

from bortrace.baseline_model import BinaryLogisticRegression, SoftmaxRegression
from bortrace.paths import OUTPUT_DIR
from predict_official_models import load_dataset
from train_official_cv import BINARY_TARGETS, prepare_dataset
from train_official_models import select_official_features


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset", default=str(OUTPUT_DIR / "official_race_dataset.csv"))
    parser.add_argument("--dataset-dir", default=None)
    parser.add_argument("--start-month", default="202101")
    parser.add_argument("--end-month", default="202512")
    parser.add_argument("--out-dir", default=str(OUTPUT_DIR / "official_models_final_2021_2025"))
    parser.add_argument("--epochs", type=int, default=700)
    args = parser.parse_args()

    df = prepare_dataset(load_dataset(args))
    feature_columns = select_official_features(df)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    metrics: dict[str, object] = {
        "dataset": args.dataset_dir or args.dataset,
        "start_month": args.start_month,
        "end_month": args.end_month,
        "train_years": sorted(int(year) for year in df["year"].dropna().unique()),
        "rows_train": int(len(df)),
        "feature_count": int(len(feature_columns)),
        "features": feature_columns,
        "models": [],
    }

    for pos in range(1, 7):
        target = f"target_pos{pos}"
        print(f"train {target}", flush=True)
        model = SoftmaxRegression.fit(df, feature_columns, target_col=target, epochs=args.epochs)
        model.save(out_dir / f"finish_pos{pos}_softmax.npz")
        metrics["models"].append(target)

    for target in BINARY_TARGETS:
        if target not in df.columns:
            continue
        target_df = df.dropna(subset=[target]).copy()
        if target_df.empty:
            continue
        target_df[target] = target_df[target].astype(int)
        print(f"train {target}", flush=True)
        model = BinaryLogisticRegression.fit(target_df, feature_columns, target_col=target, epochs=args.epochs)
        model.save(out_dir / f"{target}_logistic.npz")
        metrics["models"].append(target)

    (out_dir / "metrics.json").write_text(json.dumps(metrics, ensure_ascii=False, indent=2), encoding="utf-8")
    pd.Series(feature_columns, name="feature").to_csv(out_dir / "features.csv", index=False, encoding="utf-8")
    print(json.dumps({k: v for k, v in metrics.items() if k != "features"}, ensure_ascii=False, indent=2))
    print(f"out_dir={out_dir}")


if __name__ == "__main__":
    main()
