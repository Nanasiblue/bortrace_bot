from __future__ import annotations

import argparse
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
from bortrace.order_model import PositionProbabilityModel
from bortrace.upset_model import add_upset_meta_features, select_live_numeric_features
from train_official_models import greedy_order_from_position_probs


TARGET_COLS = [f"target_pos{i}" for i in range(1, 7)]


@dataclass(frozen=True)
class LgbmConfig:
    name: str
    n_estimators: int
    learning_rate: float
    num_leaves: int
    min_child_samples: int
    reg_lambda: float
    feature_fraction: float
    bagging_fraction: float


CONFIGS = [
    LgbmConfig("base", 900, 0.025, 31, 250, 10.0, 0.85, 0.85),
    LgbmConfig("shallow_regularized", 1200, 0.018, 15, 350, 25.0, 0.85, 0.85),
    LgbmConfig("deeper", 900, 0.025, 63, 200, 12.0, 0.80, 0.85),
    LgbmConfig("fast_wide", 650, 0.04, 31, 180, 8.0, 0.90, 0.90),
]


def require_deps() -> None:
    try:
        import lightgbm  # noqa: F401
    except ModuleNotFoundError as exc:
        raise SystemExit("lightgbm がありません。`python -m pip install lightgbm scikit-learn` を実行してください。") from exc


def load_parts(dataset_dir: Path, start_month: str, end_month: str) -> pd.DataFrame:
    parts = []
    for path in sorted(dataset_dir.glob("official_race_dataset_*.csv")):
        month = path.stem.rsplit("_", 1)[-1]
        if start_month <= month <= end_month and path.stat().st_size > 1000:
            parts.append(pd.read_csv(path))
    if not parts:
        raise SystemExit(f"No dataset parts: {dataset_dir} {start_month}-{end_month}")
    return pd.concat(parts, ignore_index=True, sort=False)


def existing_position_probs(test: pd.DataFrame, models_dir: Path) -> dict[str, np.ndarray]:
    return {
        f"pos{pos}": SoftmaxRegression.load(models_dir / f"finish_pos{pos}_softmax.npz").predict_proba(test)
        for pos in range(1, 7)
    }


def order_metrics(df: pd.DataFrame, order: np.ndarray, label: str) -> dict[str, Any]:
    actual = df[TARGET_COLS].to_numpy(dtype=int) + 1
    exact2 = (order[:, :2] == actual[:, :2]).all(axis=1)
    exact3 = (order[:, :3] == actual[:, :3]).all(axis=1)
    return {
        "model": label,
        "rows": int(len(df)),
        "winner_hit": float((order[:, 0] == actual[:, 0]).mean()),
        "top2_exact": float(exact2.mean()),
        "top3_exact": float(exact3.mean()),
        "all6_exact": float((order == actual).all(axis=1).mean()),
    }


def subgroup_metrics(df: pd.DataFrame, order: np.ndarray, label: str) -> pd.DataFrame:
    actual = df[TARGET_COLS].to_numpy(dtype=int) + 1
    out = df[["upset"]].copy()
    out["winner_hit"] = order[:, 0] == actual[:, 0]
    out["top2_exact"] = (order[:, :2] == actual[:, :2]).all(axis=1)
    out["top3_exact"] = (order[:, :3] == actual[:, :3]).all(axis=1)
    rows = []
    for value, group in out.groupby("upset"):
        rows.append(
            {
                "model": label,
                "group": "upset",
                "value": int(value),
                "rows": int(len(group)),
                "winner_hit": float(group["winner_hit"].mean()),
                "top2_exact": float(group["top2_exact"].mean()),
                "top3_exact": float(group["top3_exact"].mean()),
            }
        )
    return pd.DataFrame(rows)


def train_lgbm_positions(train: pd.DataFrame, features: list[str], cfg: LgbmConfig) -> PositionProbabilityModel:
    import lightgbm as lgb

    models: dict[str, Any] = {}
    train_x = train[features]
    for pos in range(1, 7):
        target = f"target_pos{pos}"
        print(f"training {cfg.name} {target}...", flush=True)
        model = lgb.LGBMClassifier(
            objective="multiclass",
            num_class=6,
            n_estimators=cfg.n_estimators,
            learning_rate=cfg.learning_rate,
            num_leaves=cfg.num_leaves,
            min_child_samples=cfg.min_child_samples,
            subsample=cfg.bagging_fraction,
            colsample_bytree=cfg.feature_fraction,
            reg_lambda=cfg.reg_lambda,
            random_state=100 + pos,
            n_jobs=-1,
            verbosity=-1,
        )
        model.fit(train_x, train[target].astype(int))
        models[f"pos{pos}"] = model
    return PositionProbabilityModel(models=models, feature_columns=features, label=cfg.name)


def blend_probs(a: dict[str, np.ndarray], b: dict[str, np.ndarray], weight_b: float) -> dict[str, np.ndarray]:
    return {key: (1.0 - weight_b) * a[key] + weight_b * b[key] for key in a}


def main() -> None:
    require_deps()
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset-dir", required=True)
    parser.add_argument("--start-month", default="202101")
    parser.add_argument("--end-month", default="202607")
    parser.add_argument("--train-end-month", default="202512")
    parser.add_argument("--test-start-month", default="202601")
    parser.add_argument("--test-end-month", default="202607")
    parser.add_argument("--existing-models-dir", default="outputs/official_models_final_2021_2025")
    parser.add_argument("--out-dir", required=True)
    args = parser.parse_args()

    df = load_parts(Path(args.dataset_dir), args.start_month, args.end_month).dropna(
        subset=TARGET_COLS + ["winner", "upset", "trifecta"]
    ).copy()
    df["date"] = df["date"].astype(str)
    df["yyyymm"] = df["date"].str.slice(0, 6)
    for col in TARGET_COLS:
        df[col] = df[col].astype(int)
    train = add_upset_meta_features(df[df["yyyymm"] <= args.train_end_month].copy())
    test = add_upset_meta_features(
        df[(df["yyyymm"] >= args.test_start_month) & (df["yyyymm"] <= args.test_end_month)].copy()
    )
    features = select_live_numeric_features(train)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    existing_probs = existing_position_probs(test, Path(args.existing_models_dir))
    existing_order = greedy_order_from_position_probs(existing_probs) + 1
    summary = [order_metrics(test, existing_order, "existing_softmax")]
    subgroups = [subgroup_metrics(test, existing_order, "existing_softmax")]

    best_score = summary[0]["top3_exact"]
    best_name = "existing_softmax"
    best_model: PositionProbabilityModel | None = None
    best_blend_weight = 1.0

    for cfg in CONFIGS:
        model = train_lgbm_positions(train, features, cfg)
        probs = model.predict_position_probs(test)
        order = greedy_order_from_position_probs(probs) + 1
        row = order_metrics(test, order, cfg.name)
        summary.append(row)
        subgroups.append(subgroup_metrics(test, order, cfg.name))
        if row["top3_exact"] > best_score:
            best_score = row["top3_exact"]
            best_name = cfg.name
            best_model = model
            best_blend_weight = 1.0

        for weight in [0.25, 0.40, 0.50, 0.60, 0.75]:
            blended = blend_probs(existing_probs, probs, weight)
            blended_order = greedy_order_from_position_probs(blended) + 1
            label = f"blend_existing_{cfg.name}_{weight:.2f}"
            blend_row = order_metrics(test, blended_order, label)
            summary.append(blend_row)
            subgroups.append(subgroup_metrics(test, blended_order, label))
            if blend_row["top3_exact"] > best_score:
                best_score = blend_row["top3_exact"]
                best_name = label
                best_model = model
                best_blend_weight = weight

        with (out_dir / f"order_model_{cfg.name}.pkl").open("wb") as f:
            pickle.dump(model, f)

    summary_df = pd.DataFrame(summary).sort_values(["top3_exact", "top2_exact", "winner_hit"], ascending=False)
    summary_df.to_csv(out_dir / "tuned_order_model_summary.csv", index=False, encoding="utf-8")
    pd.concat(subgroups, ignore_index=True, sort=False).to_csv(out_dir / "tuned_order_subgroup_summary.csv", index=False, encoding="utf-8")
    if best_model is not None:
        with (out_dir / "best_order_lgbm_model.pkl").open("wb") as f:
            pickle.dump(best_model, f)
    (out_dir / "metrics.json").write_text(
        json.dumps(
            {
                "rows_train": int(len(train)),
                "rows_test": int(len(test)),
                "features": int(len(features)),
                "best_name": best_name,
                "best_blend_weight": best_blend_weight,
                "models": summary_df.to_dict(orient="records"),
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    print(summary_df.head(40).to_string(index=False))
    print(f"best_name={best_name}")
    print(f"best_blend_weight={best_blend_weight}")
    print(f"out_dir={out_dir}")


if __name__ == "__main__":
    main()
