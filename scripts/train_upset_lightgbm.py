from __future__ import annotations

import argparse
import json
import pickle
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.calibration import calibration_curve
from sklearn.metrics import brier_score_loss, log_loss, precision_recall_fscore_support, roc_auc_score

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from bortrace.upset_model import UpsetProbabilityModel, select_live_numeric_features


def load_parts(dataset_dir: Path, start_month: str, end_month: str) -> pd.DataFrame:
    parts = []
    for path in sorted(dataset_dir.glob("official_race_dataset_*.csv")):
        month = path.stem.rsplit("_", 1)[-1]
        if month < start_month or month > end_month:
            continue
        if path.stat().st_size > 1000:
            parts.append(pd.read_csv(path))
    if not parts:
        raise SystemExit(f"No dataset parts: {dataset_dir} {start_month}-{end_month}")
    return pd.concat(parts, ignore_index=True, sort=False)


def metrics_for(y: np.ndarray, prob: np.ndarray, threshold: float) -> dict[str, float]:
    pred = prob >= threshold
    precision, recall, f1, _ = precision_recall_fscore_support(y, pred, average="binary", zero_division=0)
    return {
        "rows": int(len(y)),
        "actual_rate": float(y.mean()),
        "avg_pred": float(prob.mean()),
        "roc_auc": float(roc_auc_score(y, prob)),
        "brier": float(brier_score_loss(y, prob)),
        "log_loss": float(log_loss(y, np.clip(prob, 1e-9, 1 - 1e-9), labels=[0, 1])),
        "threshold": float(threshold),
        "accuracy": float((pred == y).mean()),
        "precision": float(precision),
        "recall": float(recall),
        "f1": float(f1),
        "positive_rate": float(pred.mean()),
    }


def threshold_table(df: pd.DataFrame, prob_col: str) -> pd.DataFrame:
    rows = []
    y = df["upset"].astype(int).to_numpy()
    prob = df[prob_col].to_numpy(dtype=float)
    for threshold in np.linspace(0.25, 0.75, 21):
        row = metrics_for(y, prob, float(threshold))
        rows.append(row)
    return pd.DataFrame(rows)


def five_level_table(df: pd.DataFrame, prob_col: str) -> pd.DataFrame:
    out = df[["date", "year", "month", "jcd", "rno", "upset", prob_col]].copy()
    out["level"] = pd.cut(
        out[prob_col],
        bins=[-0.001, 0.30, 0.42, 0.54, 0.66, 1.001],
        labels=[1, 2, 3, 4, 5],
    ).astype(int)
    return (
        out.groupby("level", as_index=False)
        .agg(races=("upset", "size"), pred_avg=(prob_col, "mean"), actual_rate=("upset", "mean"))
    )


def monthly_metrics(df: pd.DataFrame, prob_col: str, threshold: float) -> pd.DataFrame:
    rows = []
    for ym, group in df.groupby(df["date"].astype(str).str.slice(0, 6)):
        y = group["upset"].astype(int).to_numpy()
        prob = group[prob_col].to_numpy(dtype=float)
        row = metrics_for(y, prob, threshold)
        row["month"] = ym
        rows.append(row)
    return pd.DataFrame(rows)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset-dir", required=True)
    parser.add_argument("--start-month", default="202101")
    parser.add_argument("--end-month", default="202607")
    parser.add_argument("--train-end-month", default="202512")
    parser.add_argument("--test-start-month", default="202601")
    parser.add_argument("--test-end-month", default="202607")
    parser.add_argument("--out-dir", required=True)
    parser.add_argument("--n-estimators", type=int, default=1200)
    parser.add_argument("--learning-rate", type=float, default=0.025)
    parser.add_argument("--num-leaves", type=int, default=31)
    parser.add_argument("--min-child-samples", type=int, default=250)
    parser.add_argument("--reg-lambda", type=float, default=10.0)
    args = parser.parse_args()

    df = load_parts(Path(args.dataset_dir), args.start_month, args.end_month)
    df = df.dropna(subset=["upset"]).copy()
    df["yyyymm"] = df["date"].astype(str).str.slice(0, 6)
    df["upset"] = df["upset"].astype(int)
    features = select_live_numeric_features(df)

    train = df[df["yyyymm"] <= args.train_end_month].copy()
    test = df[(df["yyyymm"] >= args.test_start_month) & (df["yyyymm"] <= args.test_end_month)].copy()
    if train.empty or test.empty:
        raise SystemExit(f"Bad split: train={len(train)} test={len(test)}")

    y_train = train["upset"].to_numpy(dtype=int)
    y_test = test["upset"].to_numpy(dtype=int)
    model = HistGradientBoostingClassifier(
        max_iter=args.n_estimators,
        learning_rate=args.learning_rate,
        max_leaf_nodes=args.num_leaves,
        min_samples_leaf=args.min_child_samples,
        l2_regularization=args.reg_lambda,
        random_state=42,
    )
    model.fit(train[features], y_train)
    wrapped = UpsetProbabilityModel(model=model, feature_columns=features)

    train["upset_lgbm_prob"] = wrapped.predict_proba(train)
    test["upset_lgbm_prob"] = wrapped.predict_proba(test)

    thresholds = threshold_table(test, "upset_lgbm_prob")
    best_f1 = thresholds.sort_values(["f1", "precision"], ascending=[False, False]).iloc[0]
    default_threshold = float(best_f1["threshold"])

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    with (out_dir / "upset_lgbm.pkl").open("wb") as f:
        pickle.dump(wrapped, f)

    thresholds.to_csv(out_dir / "thresholds_202601_202607.csv", index=False, encoding="utf-8")
    monthly_metrics(test, "upset_lgbm_prob", default_threshold).to_csv(
        out_dir / "monthly_metrics_202601_202607.csv", index=False, encoding="utf-8"
    )
    five_level_table(test, "upset_lgbm_prob").to_csv(out_dir / "five_level_202601_202607.csv", index=False, encoding="utf-8")
    test[["date", "jcd", "rno", "upset", "upset_lgbm_prob", "winner", "payout_3t", "trifecta"]].to_csv(
        out_dir / "predictions_202601_202607.csv", index=False, encoding="utf-8"
    )
    importance = pd.DataFrame({"feature": features, "importance": np.zeros(len(features), dtype=float)})
    if hasattr(model, "feature_importances_"):
        importance["importance"] = model.feature_importances_
    importance = importance.sort_values("importance", ascending=False)
    importance.to_csv(out_dir / "feature_importance.csv", index=False, encoding="utf-8")

    metrics = {
        "rows_train": int(len(train)),
        "rows_test": int(len(test)),
        "features": int(len(features)),
        "train": metrics_for(y_train, train["upset_lgbm_prob"].to_numpy(dtype=float), default_threshold),
        "test": metrics_for(y_test, test["upset_lgbm_prob"].to_numpy(dtype=float), default_threshold),
        "default_threshold": default_threshold,
        "top_features": importance.head(30).to_dict(orient="records"),
    }
    (out_dir / "metrics.json").write_text(json.dumps(metrics, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({k: v for k, v in metrics.items() if k != "top_features"}, ensure_ascii=False, indent=2))
    print(importance.head(30).to_string(index=False))
    print(f"out_dir={out_dir}")


if __name__ == "__main__":
    main()
