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

from bortrace.baseline_model import BinaryLogisticRegression, SoftmaxRegression
from bortrace.paths import OUTPUT_DIR
from predict_official_models import load_dataset
from train_official_models import greedy_order_from_position_probs


def auc_score(y: np.ndarray, score: np.ndarray) -> float:
    order = np.argsort(score)
    ranks = np.empty_like(order, dtype=float)
    ranks[order] = np.arange(1, len(score) + 1)
    pos = y == 1
    n_pos = pos.sum()
    n_neg = (~pos).sum()
    if n_pos == 0 or n_neg == 0:
        return float("nan")
    return float((ranks[pos].sum() - n_pos * (n_pos + 1) / 2) / (n_pos * n_neg))


def add_profile_columns(df: pd.DataFrame, pos1: np.ndarray, order: np.ndarray, upset_prob: np.ndarray) -> pd.DataFrame:
    out = df[["date", "jcd", "rno", "winner", "upset", "trifecta", "payout_3t"]].copy()
    for boat in range(1, 7):
        out[f"win_prob_{boat}"] = pos1[:, boat - 1]
    sorted_probs = np.sort(pos1, axis=1)[:, ::-1]
    out["pred_winner"] = order[:, 0]
    out["pred_order"] = ["-".join(map(str, row)) for row in order]
    out["boat1_win_prob"] = pos1[:, 0]
    out["top_win_prob"] = sorted_probs[:, 0]
    out["second_win_prob"] = sorted_probs[:, 1]
    out["winner_margin"] = sorted_probs[:, 0] - sorted_probs[:, 1]
    out["top_challenger"] = np.argmax(pos1[:, 1:], axis=1) + 2
    out["top_challenger_prob"] = pos1[np.arange(len(pos1)), out["top_challenger"].to_numpy(dtype=int) - 1]
    out["boat1_gap_vs_challenger"] = out["boat1_win_prob"] - out["top_challenger_prob"]
    out["upset_prob"] = upset_prob
    out["winner_hit"] = out["pred_winner"].to_numpy(dtype=int) == out["winner"].to_numpy(dtype=int)
    out["boat1_weak"] = out["boat1_win_prob"] < 0.42
    out["challenger_close"] = out["boat1_gap_vs_challenger"] < 0.08
    out["chaos_profile"] = (
        out["boat1_weak"].astype(int)
        + out["challenger_close"].astype(int)
        + (out["upset_prob"] >= 0.54).astype(int)
        + (out["winner_margin"] < 0.08).astype(int)
    )
    return out


def grouped_rates(frame: pd.DataFrame, col: str) -> pd.DataFrame:
    return (
        frame.groupby(col, as_index=False)
        .agg(
            races=("upset", "size"),
            upset_rate=("upset", "mean"),
            winner_hit_rate=("winner_hit", "mean"),
            avg_upset_prob=("upset_prob", "mean"),
            avg_boat1_prob=("boat1_win_prob", "mean"),
        )
        .sort_values(col)
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset-dir", required=True)
    parser.add_argument("--start-month", default="202601")
    parser.add_argument("--end-month", default="202607")
    parser.add_argument("--models-dir", default=str(OUTPUT_DIR / "official_models_final_2021_2025"))
    parser.add_argument("--upset-model", default=None)
    parser.add_argument("--out-dir", required=True)
    args = parser.parse_args()

    df = load_dataset(args).dropna(subset=["winner", "upset", "trifecta", "payout_3t"]).copy().reset_index(drop=True)
    df["winner"] = df["winner"].astype(int)
    df["upset"] = df["upset"].astype(int)
    models_dir = Path(args.models_dir)
    pos_probs = {
        f"pos{pos}": SoftmaxRegression.load(models_dir / f"finish_pos{pos}_softmax.npz").predict_proba(df)
        for pos in range(1, 7)
    }
    order = greedy_order_from_position_probs(pos_probs) + 1
    if args.upset_model:
        with Path(args.upset_model).open("rb") as f:
            upset_model = pickle.load(f)
        upset_prob = upset_model.predict_proba(df)
    else:
        upset_prob = BinaryLogisticRegression.load(models_dir / "upset_logistic.npz").predict_proba(df)

    profile = add_profile_columns(df, pos_probs["pos1"], order, upset_prob)
    profile["upset_level"] = pd.cut(
        profile["upset_prob"],
        [-0.001, 0.30, 0.42, 0.54, 0.66, 1.001],
        labels=[1, 2, 3, 4, 5],
    ).astype(int)
    profile["boat1_prob_band"] = pd.cut(
        profile["boat1_win_prob"],
        [-0.001, 0.25, 0.35, 0.45, 0.55, 1.001],
        labels=["<=25", "25-35", "35-45", "45-55", "55+"],
    )
    profile["margin_band"] = pd.cut(
        profile["winner_margin"],
        [-0.001, 0.03, 0.08, 0.15, 0.30, 1.001],
        labels=["<=3pt", "3-8pt", "8-15pt", "15-30pt", "30pt+"],
    )

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    profile.to_csv(out_dir / "race_profile_predictions_202601_202607.csv", index=False, encoding="utf-8")
    grouped_rates(profile, "upset_level").to_csv(out_dir / "by_upset_level.csv", index=False, encoding="utf-8")
    grouped_rates(profile, "chaos_profile").to_csv(out_dir / "by_chaos_profile.csv", index=False, encoding="utf-8")
    grouped_rates(profile, "boat1_prob_band").to_csv(out_dir / "by_boat1_prob_band.csv", index=False, encoding="utf-8")
    grouped_rates(profile, "margin_band").to_csv(out_dir / "by_margin_band.csv", index=False, encoding="utf-8")

    metrics = {
        "rows": int(len(profile)),
        "upset_rate": float(profile["upset"].mean()),
        "winner_hit_rate": float(profile["winner_hit"].mean()),
        "upset_auc": auc_score(profile["upset"].to_numpy(dtype=int), profile["upset_prob"].to_numpy(dtype=float)),
        "boat1_prob_auc_for_no_upset": auc_score((1 - profile["upset"]).to_numpy(dtype=int), profile["boat1_win_prob"].to_numpy(dtype=float)),
        "avg_upset_prob": float(profile["upset_prob"].mean()),
        "avg_boat1_prob": float(profile["boat1_win_prob"].mean()),
    }
    (out_dir / "metrics.json").write_text(json.dumps(metrics, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(metrics, ensure_ascii=False, indent=2))
    for name in ["by_upset_level", "by_chaos_profile", "by_boat1_prob_band", "by_margin_band"]:
        print(f"\n{name}")
        print(pd.read_csv(out_dir / f"{name}.csv").to_string(index=False))


if __name__ == "__main__":
    main()
