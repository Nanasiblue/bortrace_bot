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
from bortrace.order_model import PositionProbabilityModel
from bortrace.upset_model import add_upset_meta_features, select_live_numeric_features
from train_official_models import greedy_order_from_position_probs


TARGET_COLS = [f"target_pos{i}" for i in range(1, 7)]


def require_deps() -> None:
    try:
        import lightgbm  # noqa: F401
        import sklearn  # noqa: F401
    except ModuleNotFoundError as exc:
        raise SystemExit("必要ライブラリがありません。`python -m pip install scikit-learn lightgbm` を実行してください。") from exc


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
    trifecta_actual = df["trifecta"].astype(str).to_numpy()
    trifecta_pred = np.array(["-".join(map(str, row[:3])) for row in order])
    exact2 = (order[:, :2] == actual[:, :2]).all(axis=1)
    exact3 = (order[:, :3] == actual[:, :3]).all(axis=1)
    winner = order[:, 0] == actual[:, 0]
    return {
        "model": label,
        "rows": int(len(df)),
        "winner_hit": float(winner.mean()),
        "top2_exact": float(exact2.mean()),
        "top3_exact": float(exact3.mean()),
        "trifecta_exact": float((trifecta_pred == trifecta_actual).mean()),
        "all6_exact": float((order == actual).all(axis=1).mean()),
    }


def subgroup_metrics(df: pd.DataFrame, order: np.ndarray, label: str) -> pd.DataFrame:
    actual = df[TARGET_COLS].to_numpy(dtype=int) + 1
    out = df[["date", "jcd", "rno", "upset", "winner"]].copy()
    out["pred_winner"] = order[:, 0]
    out["winner_hit"] = order[:, 0] == actual[:, 0]
    out["top2_exact"] = (order[:, :2] == actual[:, :2]).all(axis=1)
    out["top3_exact"] = (order[:, :3] == actual[:, :3]).all(axis=1)
    if "upset_level" in df.columns:
        out["upset_level"] = df["upset_level"].to_numpy()
    rows = []
    for key in ["upset", "upset_level"]:
        if key not in out.columns:
            continue
        for value, group in out.groupby(key):
            rows.append(
                {
                    "model": label,
                    "group": key,
                    "value": value,
                    "rows": int(len(group)),
                    "winner_hit": float(group["winner_hit"].mean()),
                    "top2_exact": float(group["top2_exact"].mean()),
                    "top3_exact": float(group["top3_exact"].mean()),
                }
            )
    return pd.DataFrame(rows)


def train_lightgbm_positions(train: pd.DataFrame, features: list[str]) -> PositionProbabilityModel:
    import lightgbm as lgb

    models: dict[str, Any] = {}
    train_x = train[features]
    for pos in range(1, 7):
        target = f"target_pos{pos}"
        print(f"training lightgbm {target}...", flush=True)
        model = lgb.LGBMClassifier(
            objective="multiclass",
            num_class=6,
            n_estimators=900,
            learning_rate=0.025,
            num_leaves=31,
            min_child_samples=250,
            subsample=0.85,
            colsample_bytree=0.85,
            reg_lambda=10.0,
            random_state=42 + pos,
            n_jobs=-1,
            verbosity=-1,
        )
        model.fit(train_x, train[target].astype(int))
        models[f"pos{pos}"] = model
    return PositionProbabilityModel(models=models, feature_columns=features, label="lightgbm_positions")


def existing_position_probs(test: pd.DataFrame, models_dir: Path) -> dict[str, np.ndarray]:
    return {
        f"pos{pos}": SoftmaxRegression.load(models_dir / f"finish_pos{pos}_softmax.npz").predict_proba(test)
        for pos in range(1, 7)
    }


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
    parser.add_argument("--upset-predictions", default="outputs/upset_model_compare_2021_2025_valid_202601_202607/predictions_202601_202607.csv")
    parser.add_argument("--out-dir", required=True)
    args = parser.parse_args()

    df = load_parts(Path(args.dataset_dir), args.start_month, args.end_month).dropna(subset=TARGET_COLS + ["winner", "upset", "trifecta"]).copy()
    df["date"] = df["date"].astype(str)
    df["yyyymm"] = df["date"].str.slice(0, 6)
    for col in TARGET_COLS:
        df[col] = df[col].astype(int)
    train = df[df["yyyymm"] <= args.train_end_month].copy()
    test = df[(df["yyyymm"] >= args.test_start_month) & (df["yyyymm"] <= args.test_end_month)].copy()
    train = add_upset_meta_features(train)
    test = add_upset_meta_features(test)
    features = select_live_numeric_features(train)

    upset_path = Path(args.upset_predictions)
    if upset_path.exists():
        upset_cols = pd.read_csv(upset_path, usecols=lambda c: c in {"date", "jcd", "rno", "lightgbm_prob"})
        if "lightgbm_prob" in upset_cols.columns:
            upset_cols["date"] = upset_cols["date"].astype(str)
            upset_cols["jcd"] = upset_cols["jcd"].astype(int)
            upset_cols["rno"] = upset_cols["rno"].astype(int)
            test["date"] = test["date"].astype(str)
            test["jcd"] = test["jcd"].astype(int)
            test["rno"] = test["rno"].astype(int)
            test = test.merge(upset_cols.rename(columns={"lightgbm_prob": "upset_prob"}), on=["date", "jcd", "rno"], how="left")
            test["upset_level"] = pd.cut(test["upset_prob"], [-0.001, 0.30, 0.42, 0.54, 0.66, 1.001], labels=[1, 2, 3, 4, 5]).astype(float)

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    summary = []
    subgroup = []
    existing_probs = existing_position_probs(test, Path(args.existing_models_dir))
    existing_order = greedy_order_from_position_probs(existing_probs) + 1
    summary.append(order_metrics(test, existing_order, "existing_softmax"))
    subgroup.append(subgroup_metrics(test, existing_order, "existing_softmax"))

    lightgbm_model = train_lightgbm_positions(train, features)
    lightgbm_probs = lightgbm_model.predict_position_probs(test)
    lightgbm_order = greedy_order_from_position_probs(lightgbm_probs) + 1
    summary.append(order_metrics(test, lightgbm_order, "lightgbm_positions"))
    subgroup.append(subgroup_metrics(test, lightgbm_order, "lightgbm_positions"))

    with (out_dir / "order_lightgbm_model.pkl").open("wb") as f:
        pickle.dump(lightgbm_model, f)
    pd.DataFrame(summary).to_csv(out_dir / "order_model_summary.csv", index=False, encoding="utf-8")
    pd.concat(subgroup, ignore_index=True, sort=False).to_csv(out_dir / "order_subgroup_summary.csv", index=False, encoding="utf-8")

    pred = test[["date", "jcd", "rno", "winner", "upset", "trifecta", "payout_3t"]].copy()
    pred["existing_order"] = ["-".join(map(str, row)) for row in existing_order]
    pred["lightgbm_order"] = ["-".join(map(str, row)) for row in lightgbm_order]
    pred["existing_winner_hit"] = existing_order[:, 0] == (test["winner"].to_numpy(dtype=int))
    pred["lightgbm_winner_hit"] = lightgbm_order[:, 0] == (test["winner"].to_numpy(dtype=int))
    pred.to_csv(out_dir / "order_predictions_202601_202607.csv", index=False, encoding="utf-8")

    print(pd.DataFrame(summary).to_string(index=False))
    print("\nsubgroups")
    print(pd.concat(subgroup, ignore_index=True, sort=False).to_string(index=False))
    print(f"out_dir={out_dir}")


if __name__ == "__main__":
    main()
