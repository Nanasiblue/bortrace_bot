from __future__ import annotations

import argparse
import itertools
import json
import pickle
import sys
from dataclasses import dataclass
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
from bortrace.upset_model import add_upset_meta_features

TARGET_COLS = [f"target_pos{i}" for i in range(1, 7)]


@dataclass(frozen=True)
class RankConfig:
    name: str
    seed: int
    leaves: int
    min_child: int
    reg_lambda: float


CONFIGS = (
    RankConfig("seed_a", 20260901, 31, 250, 15.0),
    RankConfig("seed_b", 20260917, 31, 250, 15.0),
    RankConfig("shallow", 20261003, 15, 350, 25.0),
)


def load_parts(path: Path, start: str, end: str) -> pd.DataFrame:
    frames = []
    for part in sorted(path.glob("official_race_dataset_*.csv")):
        month = part.stem.rsplit("_", 1)[-1]
        if start <= month <= end and part.stat().st_size > 1000:
            frames.append(pd.read_csv(part))
    if not frames:
        raise SystemExit("No dataset parts")
    return pd.concat(frames, ignore_index=True, sort=False)


def metrics(df: pd.DataFrame, order: np.ndarray, name: str) -> dict[str, Any]:
    actual = df[TARGET_COLS].to_numpy(dtype=int) + 1
    return {
        "model": name, "rows": len(df),
        "winner_hit": float((order[:, 0] == actual[:, 0]).mean()),
        "top2_exact": float((order[:, :2] == actual[:, :2]).all(axis=1).mean()),
        "top3_exact": float((order[:, :3] == actual[:, :3]).all(axis=1).mean()),
        "all6_exact": float((order == actual).all(axis=1).mean()),
    }


def normalize(scores: np.ndarray) -> np.ndarray:
    mean = scores.mean(axis=1, keepdims=True)
    std = scores.std(axis=1, keepdims=True)
    return (scores - mean) / np.where(std > 1e-9, std, 1.0)


def train_ranker(x: pd.DataFrame, y: np.ndarray, features: list[str], cfg: RankConfig) -> Any:
    import lightgbm as lgb
    model = lgb.LGBMRanker(
        objective="lambdarank", metric="ndcg", label_gain=[0, 1, 3, 7, 15, 31],
        n_estimators=1100, learning_rate=0.025, num_leaves=cfg.leaves,
        min_child_samples=cfg.min_child, subsample=0.85, colsample_bytree=0.85,
        reg_lambda=cfg.reg_lambda, random_state=cfg.seed, n_jobs=-1, verbosity=-1,
    )
    model.fit(x[features], y, group=np.full(len(x) // 6, 6, dtype=int))
    return model


def positional_probs(test: pd.DataFrame, existing_dir: Path, tuned_dir: Path) -> dict[str, np.ndarray]:
    existing = {
        f"pos{p}": SoftmaxRegression.load(existing_dir / f"finish_pos{p}_softmax.npz").predict_proba(test)
        for p in range(1, 7)
    }
    with (tuned_dir / "order_model_shallow_regularized.pkl").open("rb") as f:
        shallow = pickle.load(f)
    shallow_probs = shallow.predict_position_probs(add_upset_meta_features(test))
    return {key: 0.5 * existing[key] + 0.5 * shallow_probs[key] for key in existing}


def expected_position_score(probs: dict[str, np.ndarray]) -> np.ndarray:
    weights = np.array([6, 5, 4, 3, 2, 1], dtype=float)
    return sum(weights[p - 1] * probs[f"pos{p}"] for p in range(1, 7))


def optimal_assignment(
    probs: dict[str, np.ndarray],
    batch_size: int = 512,
    position_weights: np.ndarray | None = None,
) -> np.ndarray:
    """Maximize joint position log-probability over all 720 valid permutations."""
    cube = np.stack([np.clip(probs[f"pos{p}"], 1e-12, 1.0) for p in range(1, 7)], axis=1)
    permutations = np.asarray(list(itertools.permutations(range(6))), dtype=int)
    weights = np.ones(6, dtype=float) if position_weights is None else np.asarray(position_weights, dtype=float)
    result = np.empty((len(cube), 6), dtype=int)
    for start in range(0, len(cube), batch_size):
        block = np.log(cube[start : start + batch_size])
        scores = np.zeros((len(block), len(permutations)), dtype=float)
        for pos in range(6):
            scores += weights[pos] * block[:, pos, :][:, permutations[:, pos]]
        result[start : start + len(block)] = permutations[scores.argmax(axis=1)] + 1
    return result


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset-dir", required=True)
    parser.add_argument("--start-month", default="202101")
    parser.add_argument("--end-month", default="202607")
    parser.add_argument("--train-end-month", default="202512")
    parser.add_argument("--test-start-month", default="202601")
    parser.add_argument("--test-end-month", default="202607")
    parser.add_argument("--existing-models-dir", default="outputs/official_models_final_2021_2025")
    parser.add_argument("--tuned-position-dir", default="outputs/order_model_tune_2021_2025_valid_202601_202607")
    parser.add_argument("--previous-ranking-dir", default="outputs/ranking_order_tune_2021_2025_valid_202601_202607")
    parser.add_argument("--out-dir", required=True)
    args = parser.parse_args()

    df = load_parts(Path(args.dataset_dir), args.start_month, args.end_month).dropna(subset=TARGET_COLS).copy()
    df["date"] = df["date"].astype(str)
    df["yyyymm"] = df["date"].str[:6]
    for col in TARGET_COLS:
        df[col] = df[col].astype(int)
    train = df[df["yyyymm"] <= args.train_end_month].copy()
    test = df[(df["yyyymm"] >= args.test_start_month) & (df["yyyymm"] <= args.test_end_month)].copy()
    print("building expanded features...", flush=True)
    train_x = build_boat_ranking_features(train)
    test_x = build_boat_ranking_features(test)
    y = ranking_relevance(train)
    all_features = train_x.columns.tolist()

    print(f"feature-selection model features={len(all_features)}...", flush=True)
    selector = train_ranker(train_x, y, all_features, CONFIGS[0])
    selected, importance = select_features_by_gain(selector, all_features, 0.995, 50, 220)
    print(f"selected_features={len(selected)}", flush=True)

    models = [selector]
    model_names = ["seed_a"]
    for cfg in CONFIGS[1:]:
        print(f"training {cfg.name}...", flush=True)
        models.append(train_ranker(train_x, y, selected, cfg))
        model_names.append(cfg.name)

    score_list = [model.predict(test_x[all_features] if i == 0 else test_x[selected]).reshape(-1, 6) for i, model in enumerate(models)]
    rows = []
    for name, score in zip(model_names, score_list):
        rows.append(metrics(test, np.argsort(-score, axis=1) + 1, name))
    ensemble = np.mean([normalize(score) for score in score_list], axis=0)
    rows.append(metrics(test, np.argsort(-ensemble, axis=1) + 1, "ranker_ensemble_3"))

    probs = positional_probs(test, Path(args.existing_models_dir), Path(args.tuned_position_dir))
    rows.append(metrics(test, optimal_assignment(probs), "position_blend_optimal_assignment"))
    pos_score = normalize(expected_position_score(probs))
    for weight_ranker in np.arange(0.1, 1.0, 0.1):
        blended = weight_ranker * normalize(ensemble) + (1.0 - weight_ranker) * pos_score
        rows.append(metrics(test, np.argsort(-blended, axis=1) + 1, f"score_blend_ranker_{weight_ranker:.1f}"))

    summary = pd.DataFrame(rows).sort_values(["top3_exact", "top2_exact", "winner_hit"], ascending=False)
    out = Path(args.out_dir)
    out.mkdir(parents=True, exist_ok=True)
    summary.to_csv(out / "experiment_summary.csv", index=False, encoding="utf-8")
    importance.to_csv(out / "expanded_feature_importance.csv", index=False, encoding="utf-8")
    pd.Series(selected, name="feature").to_csv(out / "expanded_selected_features.csv", index=False, encoding="utf-8")
    for name, model in zip(model_names, models):
        features = all_features if name == "seed_a" else selected
        with (out / f"ranking_{name}.pkl").open("wb") as f:
            pickle.dump(RaceRankingModel(model, features, name), f)
    (out / "metrics.json").write_text(json.dumps({"features_all": len(all_features), "features_selected": len(selected), "models": summary.to_dict(orient="records")}, indent=2), encoding="utf-8")
    print(summary.to_string(index=False))
    print(importance.head(40).to_string(index=False))
    print(f"out_dir={out}")


if __name__ == "__main__":
    main()
