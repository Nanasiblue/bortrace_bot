from __future__ import annotations

import argparse
import json
import pickle
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from bortrace.baseline_model import BinaryLogisticRegression, SoftmaxRegression
from bortrace.paths import OUTPUT_DIR
from build_odds3t_dataset import parse_odds3t
from predict_official_models import load_dataset
from predict_trifecta_ev import combo_probabilities, format_date, odds_path
from train_candidate_value_model import FEATURE_COLUMNS, add_features
from train_official_models import greedy_order_from_position_probs


def load_candidate_model(path: Path) -> Any:
    with path.open("rb") as f:
        return pickle.load(f)


def build_race_candidates(task: dict[str, Any]) -> list[dict[str, Any]]:
    odds_file = odds_path(task["date"], task["jcd"], task["rno"])
    if not odds_file.exists() or not task["actual"] or task["payout_3t"] <= 0:
        return []
    odds_rows = parse_odds3t(odds_file)
    if not odds_rows:
        return []
    odds_by_combo = {str(item["combination"]): float(item["odds"]) for item in odds_rows}
    candidates = []
    for combo, first, second, third, raw_prob in combo_probabilities(
        np.asarray(task["pos1"], dtype=float),
        np.asarray(task["pos2"], dtype=float),
        np.asarray(task["pos3"], dtype=float),
    ):
        odds = odds_by_combo.get(combo)
        if odds is not None:
            candidates.append((raw_prob * odds, raw_prob, odds, combo, first, second, third))
    candidates.sort(reverse=True)
    rows = []
    for rank, (_, raw_prob, odds, combo, first, second, third) in enumerate(candidates[: task["top_candidates"]], start=1):
        rows.append(
            {
                "valid_year": int(task["date"][:4]),
                "sample": "eval",
                "date": task["date"],
                "jcd": task["jcd"],
                "rno": task["rno"],
                "candidate_rank": rank,
                "combination": combo,
                "first": first,
                "second": second,
                "third": third,
                "raw_prob": raw_prob,
                "odds": odds,
                "actual": task["actual"],
                "hit": combo == task["actual"],
                "payout_3t": task["payout_3t"],
                "upset_prob": task["upset_prob"],
                "high10000_prob": task["high10000_prob"],
                "pred_order": task["pred_order"],
            }
        )
    return rows


def summarize(picked: pd.DataFrame) -> dict[str, Any]:
    if picked.empty:
        return {
            "races": 0,
            "bets": 0,
            "hits": 0,
            "hit_rate": 0.0,
            "stake": 0.0,
            "ret": 0.0,
            "roi": 0.0,
            "profit": 0.0,
            "days": 0,
            "zero_hit_days": 0,
        }
    stake = len(picked) * 100.0
    ret = float(picked.loc[picked["hit"], "payout_3t"].sum())
    daily = picked.groupby("date", as_index=False).agg(bets=("hit", "size"), hits=("hit", "sum"))
    return {
        "races": int(picked[["date", "jcd", "rno"]].drop_duplicates().shape[0]),
        "bets": int(len(picked)),
        "hits": int(picked["hit"].sum()),
        "hit_rate": float(picked["hit"].mean()),
        "stake": stake,
        "ret": ret,
        "roi": ret / stake if stake else 0.0,
        "profit": ret - stake,
        "days": int(len(daily)),
        "zero_hit_days": int((daily["hits"] == 0).sum()),
    }


def pick_rule(frame: pd.DataFrame, top_per_race: int, min_ev: float, min_odds: float, max_odds: float) -> pd.DataFrame:
    picked = frame[
        (frame["odds"] >= min_odds)
        & (frame["odds"] <= max_odds)
        & (frame["model_ev"] >= min_ev)
    ].copy()
    if picked.empty:
        return picked
    picked = picked.sort_values(["date", "jcd", "rno", "model_ev"], ascending=[True, True, True, False])
    return picked.groupby(["date", "jcd", "rno"], group_keys=False).head(top_per_race)


def strategy_grid(candidates: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for top_per_race in [1, 2, 3, 5]:
        for min_ev in [-0.25, -0.1, 0.0, 0.05, 0.1, 0.2, 0.35, 0.5, 0.8, 1.0]:
            for min_odds, max_odds in [(1, 50), (1, 80), (1, 120), (20, 100), (20, 150), (50, 150), (50, 300), (100, 500)]:
                picked = pick_rule(candidates, top_per_race, min_ev, min_odds, max_odds)
                summary = summarize(picked)
                summary.update(
                    {
                        "top_per_race": top_per_race,
                        "min_model_ev": min_ev,
                        "min_odds": min_odds,
                        "max_odds": max_odds,
                    }
                )
                rows.append(summary)
    return pd.DataFrame(rows).sort_values(["roi", "hits", "zero_hit_days", "bets"], ascending=[False, False, True, False])


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset-dir", required=True)
    parser.add_argument("--start-month", default="202601")
    parser.add_argument("--end-month", default="202607")
    parser.add_argument("--models-dir", default=str(OUTPUT_DIR / "official_models_final_2021_2025"))
    parser.add_argument("--candidate-model", required=True)
    parser.add_argument("--out-dir", default=str(OUTPUT_DIR / "live_candidate_eval_202601_202607_calibrated"))
    parser.add_argument("--top-candidates", type=int, default=10)
    parser.add_argument("--prob-scale", type=float, default=0.2)
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--chunksize", type=int, default=100)
    args = parser.parse_args()

    df = load_dataset(args).dropna(subset=["trifecta", "payout_3t"]).copy().reset_index(drop=True)
    if df.empty:
        raise SystemExit("No validation rows")

    models_dir = Path(args.models_dir)
    pos_probs = {
        f"pos{pos}": SoftmaxRegression.load(models_dir / f"finish_pos{pos}_softmax.npz").predict_proba(df)
        for pos in range(1, 7)
    }
    upset_prob = BinaryLogisticRegression.load(models_dir / "upset_logistic.npz").predict_proba(df)
    high10000_prob = BinaryLogisticRegression.load(models_dir / "is_high_payout_10000_logistic.npz").predict_proba(df)
    order = greedy_order_from_position_probs(pos_probs) + 1

    tasks = []
    for idx, race in df.iterrows():
        date = format_date(race["date"])
        tasks.append(
            {
                "date": date,
                "jcd": int(race["jcd"]),
                "rno": int(race["rno"]),
                "actual": str(race.get("trifecta", "")),
                "payout_3t": float(race.get("payout_3t", 0.0) or 0.0),
                "pos1": pos_probs["pos1"][idx].tolist(),
                "pos2": pos_probs["pos2"][idx].tolist(),
                "pos3": pos_probs["pos3"][idx].tolist(),
                "upset_prob": float(upset_prob[idx]),
                "high10000_prob": float(high10000_prob[idx]),
                "pred_order": "-".join(map(str, order[idx])),
                "top_candidates": args.top_candidates,
            }
        )

    rows: list[dict[str, Any]] = []
    done = 0
    with ProcessPoolExecutor(max_workers=args.workers) as pool:
        for race_rows in pool.map(build_race_candidates, tasks, chunksize=args.chunksize):
            done += 1
            rows.extend(race_rows)
            if done % 3000 == 0:
                print(f"races={done}/{len(tasks)} candidates={len(rows)}", flush=True)

    candidates = add_features(pd.DataFrame(rows), args.prob_scale)
    model_x = candidates[FEATURE_COLUMNS].to_numpy(dtype=float)
    candidate_model = load_candidate_model(Path(args.candidate_model))
    candidates["model_prob"] = candidate_model.predict_proba(model_x)[:, 1]
    candidates["model_ev"] = candidates["model_prob"] * candidates["odds"] - 1.0
    candidates["month"] = candidates["date"].str.slice(0, 6)

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    summary = strategy_grid(candidates)
    summary.to_csv(out_dir / "strategy_summary.csv", index=False, encoding="utf-8")
    month_rows = []
    for month, group in candidates.groupby("month"):
        monthly = strategy_grid(group)
        monthly.insert(0, "month", month)
        monthly.to_csv(out_dir / f"strategy_summary_{month}.csv", index=False, encoding="utf-8")
        month_rows.append(monthly)
    pd.concat(month_rows, ignore_index=True).to_csv(out_dir / "strategy_summary_by_month.csv", index=False, encoding="utf-8")
    candidates.nlargest(100000, "model_ev").to_csv(out_dir / "scored_top.csv", index=False, encoding="utf-8")

    calibration = candidates.copy()
    calibration["prob_bin"] = pd.qcut(calibration["model_prob"], q=10, duplicates="drop")
    calibration_df = calibration.groupby("prob_bin", observed=False).agg(
        n=("hit", "size"),
        pred=("model_prob", "mean"),
        actual=("hit", "mean"),
    )
    calibration_df.to_csv(out_dir / "model_prob_calibration.csv", encoding="utf-8")

    metrics = {
        "races": int(df[["date", "jcd", "rno"]].drop_duplicates().shape[0]),
        "candidate_rows": int(len(candidates)),
        "base_candidate_hit_rate": float(candidates["hit"].mean()),
        "model_prob_avg": float(candidates["model_prob"].mean()),
        "best_strategy": summary.head(1).to_dict(orient="records")[0] if not summary.empty else {},
    }
    (out_dir / "metrics.json").write_text(json.dumps(metrics, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(metrics, ensure_ascii=False, indent=2))
    print(summary.head(30).to_string(index=False))
    print(f"out_dir={out_dir}")


if __name__ == "__main__":
    main()
