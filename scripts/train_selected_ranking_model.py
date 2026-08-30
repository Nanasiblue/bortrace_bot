from __future__ import annotations

import argparse
import json
import pickle
import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from bortrace.ranking_order_model import RaceRankingModel, build_boat_ranking_features, ranking_relevance
from tune_ranking_order_model import (
    TARGET_COLS, constrained_order_from_scores, existing_order, load_parts,
    order_metrics, train_ranker,
)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset-dir", required=True)
    parser.add_argument("--selected-features", required=True)
    parser.add_argument("--existing-models-dir", required=True)
    parser.add_argument("--out-dir", required=True)
    parser.add_argument("--start-month", default="202101")
    parser.add_argument("--end-month", default="202607")
    parser.add_argument("--train-end-month", default="202512")
    parser.add_argument("--test-start-month", default="202601")
    parser.add_argument("--test-end-month", default="202607")
    parser.add_argument("--estimators", type=int, default=800)
    args = parser.parse_args()

    frame = load_parts(Path(args.dataset_dir), args.start_month, args.end_month).dropna(subset=TARGET_COLS).copy()
    frame["date"] = frame["date"].astype(str)
    frame["yyyymm"] = frame["date"].str[:6]
    train = frame[frame["yyyymm"] <= args.train_end_month].copy()
    test = frame[(frame["yyyymm"] >= args.test_start_month) & (frame["yyyymm"] <= args.test_end_month)].copy()
    features = pd.read_csv(args.selected_features)["feature"].tolist()
    print(f"building features train={len(train)} test={len(test)} selected={len(features)}", flush=True)
    train_x = build_boat_ranking_features(train)
    test_x = build_boat_ranking_features(test)
    missing = [x for x in features if x not in train_x]
    if missing:
        raise SystemExit(f"Missing selected features: {missing}")
    model = train_ranker(
        train_x, ranking_relevance(train), features, seed=20260831,
        n_estimators=args.estimators,
    )
    label = f"ranker_selected_{args.estimators}"
    order = constrained_order_from_scores(model.predict(test_x[features]))
    rows = [
        order_metrics(test, existing_order(test, Path(args.existing_models_dir)), "existing_softmax"),
        order_metrics(test, order, label),
    ]
    summary = pd.DataFrame(rows).sort_values(["top3_exact", "top2_exact"], ascending=False)
    out = Path(args.out_dir)
    out.mkdir(parents=True, exist_ok=True)
    summary.to_csv(out / "ranking_order_summary.csv", index=False, encoding="utf-8")
    with (out / "ranking_model_selected.pkl").open("wb") as handle:
        pickle.dump(RaceRankingModel(model, features, label), handle)
    (out / "metrics.json").write_text(json.dumps({
        "rows_train": len(train), "rows_test": len(test), "features": len(features),
        "estimators": args.estimators, "models": summary.to_dict(orient="records"),
    }, ensure_ascii=False, indent=2), encoding="utf-8")
    print(summary.to_string(index=False))
    print(f"out_dir={out}")


if __name__ == "__main__":
    main()
