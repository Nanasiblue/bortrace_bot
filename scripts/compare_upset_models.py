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

from bortrace.upset_model import UpsetProbabilityModel, add_upset_meta_features, select_live_numeric_features


def require_sklearn() -> None:
    try:
        import sklearn  # noqa: F401
    except ModuleNotFoundError as exc:
        raise SystemExit(
            "scikit-learn がありません。先に `python -m pip install scikit-learn lightgbm` を実行してください。"
        ) from exc


def load_parts(dataset_dir: Path, start_month: str, end_month: str) -> pd.DataFrame:
    parts = []
    for path in sorted(dataset_dir.glob("official_race_dataset_*.csv")):
        month = path.stem.rsplit("_", 1)[-1]
        if start_month <= month <= end_month and path.stat().st_size > 1000:
            parts.append(pd.read_csv(path))
    if not parts:
        raise SystemExit(f"No dataset parts: {dataset_dir} {start_month}-{end_month}")
    return pd.concat(parts, ignore_index=True, sort=False)


def metrics_for(y: np.ndarray, prob: np.ndarray, threshold: float) -> dict[str, Any]:
    from sklearn.metrics import brier_score_loss, log_loss, precision_recall_fscore_support, roc_auc_score

    pred = prob >= threshold
    precision, recall, f1, _ = precision_recall_fscore_support(y, pred, average="binary", zero_division=0)
    return {
        "rows": int(len(y)),
        "actual_rate": float(y.mean()),
        "avg_pred": float(prob.mean()),
        "auc": float(roc_auc_score(y, prob)),
        "brier": float(brier_score_loss(y, prob)),
        "log_loss": float(log_loss(y, np.clip(prob, 1e-9, 1 - 1e-9), labels=[0, 1])),
        "threshold": float(threshold),
        "accuracy": float((pred == y).mean()),
        "precision": float(precision),
        "recall": float(recall),
        "f1": float(f1),
        "positive_rate": float(pred.mean()),
    }


def best_threshold(y: np.ndarray, prob: np.ndarray) -> tuple[float, pd.DataFrame]:
    rows = [metrics_for(y, prob, float(th)) for th in np.linspace(0.25, 0.75, 21)]
    table = pd.DataFrame(rows)
    best = table.sort_values(["f1", "precision"], ascending=[False, False]).iloc[0]
    return float(best["threshold"]), table


def level_table(test: pd.DataFrame, prob_col: str) -> pd.DataFrame:
    tmp = test[["upset", prob_col]].copy()
    tmp["level"] = pd.cut(tmp[prob_col], [-0.001, 0.30, 0.42, 0.54, 0.66, 1.001], labels=[1, 2, 3, 4, 5]).astype(int)
    return tmp.groupby("level", as_index=False).agg(
        races=("upset", "size"),
        pred_avg=(prob_col, "mean"),
        actual_rate=("upset", "mean"),
    )


def monthly_table(test: pd.DataFrame, prob_col: str, threshold: float) -> pd.DataFrame:
    rows = []
    for month, group in test.groupby("yyyymm"):
        row = metrics_for(group["upset"].to_numpy(dtype=int), group[prob_col].to_numpy(dtype=float), threshold)
        row["month"] = month
        rows.append(row)
    return pd.DataFrame(rows)


def model_specs() -> list[tuple[str, Any]]:
    from sklearn.ensemble import HistGradientBoostingClassifier, RandomForestClassifier
    from sklearn.impute import SimpleImputer
    from sklearn.linear_model import LogisticRegression
    from sklearn.pipeline import make_pipeline
    from sklearn.preprocessing import StandardScaler

    specs: list[tuple[str, Any]] = [
        (
            "sk_logistic_balanced",
            make_pipeline(
                SimpleImputer(strategy="median"),
                StandardScaler(),
                LogisticRegression(max_iter=1500, class_weight="balanced", solver="saga", random_state=42),
            ),
        ),
        (
            "hist_gbdt",
            HistGradientBoostingClassifier(
                max_iter=600,
                learning_rate=0.035,
                max_leaf_nodes=31,
                min_samples_leaf=200,
                l2_regularization=5.0,
                random_state=42,
            ),
        ),
        (
            "random_forest_light",
            make_pipeline(
                SimpleImputer(strategy="median"),
                RandomForestClassifier(
                    n_estimators=300,
                    max_depth=10,
                    min_samples_leaf=80,
                    class_weight="balanced_subsample",
                    n_jobs=-1,
                    random_state=42,
                ),
            ),
        ),
    ]
    try:
        import lightgbm as lgb

        specs.append(
            (
                "lightgbm",
                lgb.LGBMClassifier(
                    objective="binary",
                    n_estimators=1200,
                    learning_rate=0.025,
                    num_leaves=31,
                    min_child_samples=250,
                    subsample=0.85,
                    colsample_bytree=0.85,
                    reg_lambda=10.0,
                    random_state=42,
                    n_jobs=-1,
                    verbosity=-1,
                ),
            )
        )
    except ModuleNotFoundError:
        pass
    return specs


def main() -> None:
    require_sklearn()
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset-dir", required=True)
    parser.add_argument("--start-month", default="202101")
    parser.add_argument("--end-month", default="202607")
    parser.add_argument("--train-end-month", default="202512")
    parser.add_argument("--test-start-month", default="202601")
    parser.add_argument("--test-end-month", default="202607")
    parser.add_argument("--out-dir", required=True)
    args = parser.parse_args()

    df = load_parts(Path(args.dataset_dir), args.start_month, args.end_month).dropna(subset=["upset"]).copy()
    df["date"] = df["date"].astype(str)
    df["yyyymm"] = df["date"].str.slice(0, 6)
    df["upset"] = df["upset"].astype(int)
    df = add_upset_meta_features(df)
    features = select_live_numeric_features(df)

    train = df[df["yyyymm"] <= args.train_end_month].copy()
    test = df[(df["yyyymm"] >= args.test_start_month) & (df["yyyymm"] <= args.test_end_month)].copy()
    train_x = train[features]
    train_y = train["upset"].to_numpy(dtype=int)
    test_x = test[features]
    test_y = test["upset"].to_numpy(dtype=int)

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    summary = []
    best_name = ""
    best_auc = -1.0

    for name, model in model_specs():
        print(f"training {name}...", flush=True)
        model.fit(train_x, train_y)
        prob = model.predict_proba(test_x)[:, 1]
        threshold, thresholds = best_threshold(test_y, prob)
        test[f"{name}_prob"] = prob
        thresholds.to_csv(out_dir / f"{name}_thresholds.csv", index=False, encoding="utf-8")
        level_table(test.rename(columns={f"{name}_prob": "prob"}), "prob").to_csv(
            out_dir / f"{name}_levels.csv", index=False, encoding="utf-8"
        )
        monthly_table(test.rename(columns={f"{name}_prob": "prob"}), "prob", threshold).to_csv(
            out_dir / f"{name}_monthly.csv", index=False, encoding="utf-8"
        )
        metrics = metrics_for(test_y, prob, threshold)
        metrics["model"] = name
        summary.append(metrics)
        if metrics["auc"] > best_auc:
            best_auc = float(metrics["auc"])
            best_name = name
            wrapped = UpsetProbabilityModel(model=model, feature_columns=features, label=name)
            with (out_dir / "best_upset_model.pkl").open("wb") as f:
                pickle.dump(wrapped, f)

    summary_df = pd.DataFrame(summary).sort_values(["auc", "f1"], ascending=[False, False])
    summary_df.to_csv(out_dir / "model_summary.csv", index=False, encoding="utf-8")
    test.to_csv(out_dir / "predictions_202601_202607.csv", index=False, encoding="utf-8")
    (out_dir / "metrics.json").write_text(
        json.dumps(
            {
                "rows_train": int(len(train)),
                "rows_test": int(len(test)),
                "features": int(len(features)),
                "best_model": best_name,
                "models": summary_df.to_dict(orient="records"),
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    print(summary_df.to_string(index=False))
    print(f"best_model={best_name}")
    print(f"out_dir={out_dir}")


if __name__ == "__main__":
    main()
