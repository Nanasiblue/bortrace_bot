from __future__ import annotations

import argparse
import json
import pickle
import sys
from pathlib import Path

import lightgbm as lgb
import numpy as np
import pandas as pd
from sklearn.isotonic import IsotonicRegression
from sklearn.metrics import average_precision_score, brier_score_loss, log_loss, roc_auc_score

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from bortrace.candidate_model import CalibratedProbabilityModel
from train_candidate_value_model import FEATURE_COLUMNS, add_features


BASE_COLUMNS = [
    "valid_year",
    "sample",
    "date",
    "jcd",
    "rno",
    "candidate_rank",
    "combination",
    "first",
    "second",
    "third",
    "raw_prob",
    "odds",
    "hit",
    "payout_3t",
    "upset_prob",
    "high10000_prob",
]


def load_candidates(path: Path, prob_scale: float) -> pd.DataFrame:
    df = pd.read_csv(path, usecols=lambda col: col in BASE_COLUMNS)
    df = add_features(df, prob_scale)
    df["hit"] = df["hit"].astype(bool)
    return df


def predict_positive(model: object, x: pd.DataFrame | np.ndarray) -> np.ndarray:
    return np.asarray(model.predict_proba(x)[:, 1], dtype=float)


def probability_metrics(y: np.ndarray, prob: np.ndarray) -> dict[str, float]:
    out = {
        "rows": int(len(y)),
        "hit_rate": float(np.mean(y)),
        "prob_avg": float(np.mean(prob)),
        "brier": float(brier_score_loss(y, prob)),
        "log_loss": float(log_loss(y, np.clip(prob, 1e-9, 1 - 1e-9), labels=[0, 1])),
    }
    if len(np.unique(y)) > 1:
        out["roc_auc"] = float(roc_auc_score(y, prob))
        out["avg_precision"] = float(average_precision_score(y, prob))
    return out


def calibration_bins(frame: pd.DataFrame, prob_col: str, bins: int = 10) -> pd.DataFrame:
    scored = frame[["hit", prob_col]].dropna().copy()
    scored["bin"] = pd.qcut(scored[prob_col].rank(method="first"), bins, labels=False, duplicates="drop")
    return (
        scored.groupby("bin", as_index=False)
        .agg(rows=("hit", "size"), pred_avg=(prob_col, "mean"), actual_rate=("hit", "mean"))
    )


def strategy_summary(
    frame: pd.DataFrame,
    *,
    prob_col: str,
    ev_col: str,
    top_per_race_values: list[int],
    min_ev_values: list[float],
    odds_bands: list[tuple[float, float]],
) -> pd.DataFrame:
    rows = []
    for top_per_race in top_per_race_values:
        for min_ev in min_ev_values:
            for min_odds, max_odds in odds_bands:
                picked = frame[
                    (frame["odds"] >= min_odds)
                    & (frame["odds"] <= max_odds)
                    & (frame[ev_col] >= min_ev)
                ].copy()
                if picked.empty:
                    continue
                picked = picked.sort_values(["date", "jcd", "rno", ev_col], ascending=[True, True, True, False])
                picked = picked.groupby(["date", "jcd", "rno"], group_keys=False).head(top_per_race)
                stake = len(picked) * 100.0
                ret = float(picked.loc[picked["hit"], "payout_3t"].sum())
                day_summary = picked.groupby("date")["hit"].sum()
                rows.append(
                    {
                        "top_per_race": top_per_race,
                        "min_ev": min_ev,
                        "min_odds": min_odds,
                        "max_odds": max_odds,
                        "races": picked[["date", "jcd", "rno"]].drop_duplicates().shape[0],
                        "bets": len(picked),
                        "hits": int(picked["hit"].sum()),
                        "hit_rate": float(picked["hit"].mean()),
                        "stake": stake,
                        "ret": ret,
                        "roi": ret / stake if stake else 0.0,
                        "profit": ret - stake,
                        "avg_prob": float(picked[prob_col].mean()),
                        "avg_ev": float(picked[ev_col].mean()),
                        "days": int(day_summary.shape[0]),
                        "zero_hit_days": int((day_summary == 0).sum()),
                    }
                )
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows).sort_values(["roi", "hits", "bets"], ascending=[False, False, False])


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--candidates", required=True)
    parser.add_argument("--out-dir", required=True)
    parser.add_argument("--eval-candidates", nargs="*", default=[])
    parser.add_argument("--prob-scale", type=float, default=0.2)
    parser.add_argument("--train-years", nargs="+", type=int, default=[2023, 2024])
    parser.add_argument("--calibration-years", nargs="+", type=int, default=[2025])
    parser.add_argument("--num-leaves", type=int, default=7)
    parser.add_argument("--max-depth", type=int, default=3)
    parser.add_argument("--n-estimators", type=int, default=700)
    parser.add_argument("--learning-rate", type=float, default=0.025)
    parser.add_argument("--min-child-samples", type=int, default=400)
    parser.add_argument("--reg-lambda", type=float, default=20.0)
    args = parser.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    df = load_candidates(Path(args.candidates), args.prob_scale)
    train = df[df["valid_year"].isin(args.train_years)].copy()
    calib = df[df["valid_year"].isin(args.calibration_years)].copy()
    if train.empty or calib.empty:
        raise SystemExit(f"bad split: train={len(train)} calibration={len(calib)}")

    train_x = train[FEATURE_COLUMNS]
    train_y = train["hit"].astype(int).to_numpy()
    calib_x = calib[FEATURE_COLUMNS]
    calib_y = calib["hit"].astype(int).to_numpy()

    pos = max(int(train_y.sum()), 1)
    neg = max(int((train_y == 0).sum()), 1)
    model = lgb.LGBMClassifier(
        objective="binary",
        n_estimators=args.n_estimators,
        learning_rate=args.learning_rate,
        num_leaves=args.num_leaves,
        max_depth=args.max_depth,
        min_child_samples=args.min_child_samples,
        subsample=0.85,
        colsample_bytree=0.85,
        reg_lambda=args.reg_lambda,
        scale_pos_weight=min(120.0, neg / pos),
        random_state=42,
        n_jobs=-1,
        verbosity=-1,
    )
    model.fit(train_x, train_y)
    raw_calib = predict_positive(model, calib_x)
    iso = IsotonicRegression(out_of_bounds="clip", y_min=0.0, y_max=1.0)
    iso.fit(raw_calib, calib_y)

    wrapped = CalibratedProbabilityModel(model, iso, list(FEATURE_COLUMNS))
    calib["raw_model_prob"] = raw_calib
    calib["calibrated_prob"] = predict_positive(wrapped, calib_x)
    calib["calibrated_ev"] = calib["calibrated_prob"] * calib["odds"] - 1.0

    metrics = {
        "train_rows": int(len(train)),
        "calibration_rows": int(len(calib)),
        "train_hit_rate": float(train["hit"].mean()),
        "calibration_hit_rate": float(calib["hit"].mean()),
        "raw_on_calibration": probability_metrics(calib_y, calib["raw_model_prob"].to_numpy()),
        "calibrated_on_calibration": probability_metrics(calib_y, calib["calibrated_prob"].to_numpy()),
        "feature_columns": list(FEATURE_COLUMNS),
    }

    with (out_dir / "lightgbm_calibrated.pkl").open("wb") as f:
        pickle.dump(wrapped, f)
    calib.head(50000).to_csv(out_dir / "calibration_scored_top.csv", index=False, encoding="utf-8")
    calibration_bins(calib, "raw_model_prob").to_csv(out_dir / "raw_calibration_bins.csv", index=False, encoding="utf-8")
    calibration_bins(calib, "calibrated_prob").to_csv(out_dir / "calibrated_bins.csv", index=False, encoding="utf-8")

    eval_reports = []
    for eval_path_text in args.eval_candidates:
        eval_path = Path(eval_path_text)
        eval_df = load_candidates(eval_path, args.prob_scale)
        eval_x = eval_df[FEATURE_COLUMNS]
        eval_y = eval_df["hit"].astype(int).to_numpy()
        eval_df["raw_model_prob"] = predict_positive(model, eval_x)
        eval_df["calibrated_prob"] = predict_positive(wrapped, eval_x)
        eval_df["calibrated_ev"] = eval_df["calibrated_prob"] * eval_df["odds"] - 1.0
        label = eval_path.stem
        eval_metrics = {
            "label": label,
            "path": str(eval_path),
            "raw": probability_metrics(eval_y, eval_df["raw_model_prob"].to_numpy()),
            "calibrated": probability_metrics(eval_y, eval_df["calibrated_prob"].to_numpy()),
        }
        eval_reports.append(eval_metrics)
        calibration_bins(eval_df, "calibrated_prob").to_csv(
            out_dir / f"{label}_calibrated_bins.csv", index=False, encoding="utf-8"
        )
        strategies = strategy_summary(
            eval_df,
            prob_col="calibrated_prob",
            ev_col="calibrated_ev",
            top_per_race_values=[1, 2, 3],
            min_ev_values=[0.0, 0.05, 0.1, 0.2, 0.5],
            odds_bands=[(1, 80), (20, 100), (50, 150), (50, 300)],
        )
        strategies.to_csv(out_dir / f"{label}_strategy_summary.csv", index=False, encoding="utf-8")

    metrics["eval_reports"] = eval_reports
    (out_dir / "metrics.json").write_text(json.dumps(metrics, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(metrics, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
