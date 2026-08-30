from __future__ import annotations

import argparse
import json
import pickle
import sys
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from bortrace.baseline_model import SoftmaxRegression
from bortrace.ranking_order_model import (
    RaceRankingModel,
    build_boat_ranking_features,
    ranking_relevance,
    select_features_by_gain,
)
from train_official_models import greedy_order_from_position_probs

TARGET_COLS = [f"target_pos{i}" for i in range(1, 7)]


def load_parts(dataset_dir: Path, start_month: str, end_month: str) -> pd.DataFrame:
    parts = []
    for path in sorted(dataset_dir.glob("official_race_dataset_*.csv")):
        month = path.stem.rsplit("_", 1)[-1]
        if start_month <= month <= end_month and path.stat().st_size > 1000:
            parts.append(pd.read_csv(path))
    if not parts:
        raise SystemExit(f"No dataset parts: {dataset_dir} {start_month}-{end_month}")
    return pd.concat(parts, ignore_index=True, sort=False)


def order_metrics(df: pd.DataFrame, order: np.ndarray, label: str) -> dict[str, Any]:
    actual = df[TARGET_COLS].to_numpy(dtype=int) + 1
    return {
        "model": label,
        "rows": int(len(df)),
        "winner_hit": float((order[:, 0] == actual[:, 0]).mean()),
        "top2_exact": float((order[:, :2] == actual[:, :2]).all(axis=1).mean()),
        "top3_exact": float((order[:, :3] == actual[:, :3]).all(axis=1).mean()),
        "all6_exact": float((order == actual).all(axis=1).mean()),
    }


def train_ranker(
    train_x: pd.DataFrame, train_y: np.ndarray, features: list[str], seed: int,
    n_estimators: int = 1100,
) -> Any:
    import lightgbm as lgb

    model = lgb.LGBMRanker(
        objective="lambdarank",
        metric="ndcg",
        eval_at=(1, 2, 3, 6),
        label_gain=[0, 1, 3, 7, 15, 31],
        n_estimators=n_estimators,
        learning_rate=0.025,
        num_leaves=31,
        min_child_samples=250,
        subsample=0.85,
        colsample_bytree=0.85,
        reg_lambda=15.0,
        random_state=seed,
        n_jobs=-1,
        verbosity=-1,
    )
    model.fit(train_x[features], train_y, group=np.full(len(train_x) // 6, 6, dtype=int))
    return model


def constrained_order_from_scores(scores: np.ndarray) -> np.ndarray:
    return np.argsort(-scores.reshape(-1, 6), axis=1, kind="stable") + 1


def existing_order(test: pd.DataFrame, models_dir: Path) -> np.ndarray:
    probs = {
        f"pos{pos}": SoftmaxRegression.load(models_dir / f"finish_pos{pos}_softmax.npz").predict_proba(test)
        for pos in range(1, 7)
    }
    return greedy_order_from_position_probs(probs) + 1


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset-dir", required=True)
    parser.add_argument("--start-month", default="202101")
    parser.add_argument("--end-month", default="202607")
    parser.add_argument("--train-end-month", default="202512")
    parser.add_argument("--test-start-month", default="202601")
    parser.add_argument("--test-end-month", default="202607")
    parser.add_argument("--existing-models-dir", default="outputs/official_models_final_2021_2025")
    parser.add_argument("--out-dir", required=True)
    parser.add_argument("--importance-threshold", type=float, default=0.995)
    parser.add_argument("--min-features", type=int, default=40)
    parser.add_argument("--max-features", type=int, default=180)
    parser.add_argument("--full-estimators", type=int, default=350)
    parser.add_argument("--selected-estimators", type=int, default=800)
    args = parser.parse_args()

    try:
        import lightgbm  # noqa: F401
    except ModuleNotFoundError as exc:
        raise SystemExit("lightgbm が必要です。") from exc

    df = load_parts(Path(args.dataset_dir), args.start_month, args.end_month).dropna(subset=TARGET_COLS).copy()
    df["date"] = df["date"].astype(str)
    df["yyyymm"] = df["date"].str.slice(0, 6)
    for col in TARGET_COLS:
        df[col] = df[col].astype(int)
    train = df[df["yyyymm"] <= args.train_end_month].copy()
    test = df[(df["yyyymm"] >= args.test_start_month) & (df["yyyymm"] <= args.test_end_month)].copy()
    if train.empty or test.empty:
        raise SystemExit(f"Bad split: train={len(train)} test={len(test)}")

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    print("building race-level boat features...", flush=True)
    train_x = build_boat_ranking_features(train)
    test_x = build_boat_ranking_features(test)
    train_y = ranking_relevance(train)
    full_features = train_x.columns.tolist()
    print(f"training ranker_full features={len(full_features)}...", flush=True)
    full_lgbm = train_ranker(
        train_x, train_y, full_features, seed=20260830, n_estimators=args.full_estimators
    )
    selected, importance = select_features_by_gain(
        full_lgbm, full_features, args.importance_threshold, args.min_features, args.max_features
    )
    print(f"training ranker_selected features={len(selected)}...", flush=True)
    selected_lgbm = train_ranker(
        train_x, train_y, selected, seed=20260831, n_estimators=args.selected_estimators
    )

    full_model = RaceRankingModel(full_lgbm, full_features, "ranker_full")
    selected_model = RaceRankingModel(selected_lgbm, selected, "ranker_selected")
    rows = [order_metrics(test, existing_order(test, Path(args.existing_models_dir)), "existing_softmax")]
    full_order = constrained_order_from_scores(full_lgbm.predict(test_x[full_features]))
    selected_order = constrained_order_from_scores(selected_lgbm.predict(test_x[selected]))
    rows.append(order_metrics(test, full_order, full_model.label))
    rows.append(order_metrics(test, selected_order, selected_model.label))
    summary = pd.DataFrame(rows).sort_values(["top3_exact", "top2_exact", "winner_hit"], ascending=False)

    importance.to_csv(out_dir / "ranking_feature_importance.csv", index=False, encoding="utf-8")
    pd.Series(selected, name="feature").to_csv(out_dir / "ranking_selected_features.csv", index=False, encoding="utf-8")
    summary.to_csv(out_dir / "ranking_order_summary.csv", index=False, encoding="utf-8")
    with (out_dir / "ranking_model_full.pkl").open("wb") as f:
        pickle.dump(full_model, f)
    with (out_dir / "ranking_model_selected.pkl").open("wb") as f:
        pickle.dump(selected_model, f)
    (out_dir / "metrics.json").write_text(
        json.dumps(
            {
                "rows_train": int(len(train)), "rows_test": int(len(test)),
                "features_full": len(full_features), "features_selected": len(selected),
                "importance_threshold": args.importance_threshold,
                "models": summary.to_dict(orient="records"),
            }, ensure_ascii=False, indent=2,
        ), encoding="utf-8",
    )
    print(summary.to_string(index=False))
    print(importance.head(30).to_string(index=False))
    print(f"out_dir={out_dir}")


if __name__ == "__main__":
    main()
