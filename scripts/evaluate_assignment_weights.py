from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from experiment_ranking_ensemble import load_parts, metrics, optimal_assignment, positional_probs

TARGET_COLS = [f"target_pos{i}" for i in range(1, 7)]
WEIGHTS = {
    "all_equal": [1, 1, 1, 1, 1, 1],
    "top3_only": [1, 1, 1, 0, 0, 0],
    "top3_soft_tail": [1, 1, 1, 0.20, 0.10, 0.05],
    "top3_tiny_tail": [1, 1, 1, 0.05, 0.02, 0.01],
    "ordered_top3": [1.40, 1.20, 1.00, 0.20, 0.10, 0.05],
    "winner_heavy": [2.00, 1.20, 1.00, 0.20, 0.10, 0.05],
    "top2_heavy": [1.40, 1.40, 1.00, 0.20, 0.10, 0.05],
    "decay": [1.00, 0.80, 0.60, 0.30, 0.15, 0.08],
}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset-dir", required=True)
    parser.add_argument("--start-month", default="202101")
    parser.add_argument("--end-month", default="202607")
    parser.add_argument("--test-start-month", default="202601")
    parser.add_argument("--test-end-month", default="202607")
    parser.add_argument("--existing-models-dir", default="outputs/official_models_final_2021_2025")
    parser.add_argument("--tuned-position-dir", default="outputs/order_model_tune_2021_2025_valid_202601_202607")
    parser.add_argument("--out-dir", required=True)
    args = parser.parse_args()

    df = load_parts(Path(args.dataset_dir), args.start_month, args.end_month).dropna(subset=TARGET_COLS).copy()
    df["date"] = df["date"].astype(str)
    df["yyyymm"] = df["date"].str[:6]
    for col in TARGET_COLS:
        df[col] = df[col].astype(int)
    test = df[(df["yyyymm"] >= args.test_start_month) & (df["yyyymm"] <= args.test_end_month)].copy()
    print(f"test_rows={len(test)} loading position probabilities...", flush=True)
    probs = positional_probs(test, Path(args.existing_models_dir), Path(args.tuned_position_dir))

    rows = []
    for name, weights in WEIGHTS.items():
        print(f"evaluating {name} weights={weights}...", flush=True)
        order = optimal_assignment(probs, position_weights=np.asarray(weights, dtype=float))
        row = metrics(test, order, name)
        row["weights"] = ",".join(map(str, weights))
        rows.append(row)
    summary = pd.DataFrame(rows).sort_values(["top3_exact", "top2_exact", "winner_hit"], ascending=False)
    out = Path(args.out_dir)
    out.mkdir(parents=True, exist_ok=True)
    summary.to_csv(out / "assignment_weight_summary.csv", index=False, encoding="utf-8")
    (out / "metrics.json").write_text(json.dumps(summary.to_dict(orient="records"), indent=2), encoding="utf-8")
    print(summary.to_string(index=False))
    print(f"out_dir={out}")


if __name__ == "__main__":
    main()
