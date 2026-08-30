from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd

from bortrace.baseline_model import BinaryLogisticRegression, SoftmaxRegression
from bortrace.paths import OUTPUT_DIR
from build_odds3t_dataset import parse_odds3t
from predict_official_models import load_dataset
from predict_trifecta_ev import combo_probabilities, format_date, odds_path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset-dir", required=True)
    parser.add_argument("--start-month", default="202301")
    parser.add_argument("--end-month", default="202512")
    parser.add_argument("--cv-dir", default=str(OUTPUT_DIR / "official_cv"))
    parser.add_argument("--models-dir", default=None)
    parser.add_argument("--valid-years", nargs="+", type=int, default=[2023, 2024, 2025])
    parser.add_argument("--top-candidates", type=int, default=30)
    parser.add_argument("--limit-per-year", type=int, default=None)
    parser.add_argument("--sample-mode", choices=["head", "tail"], default="head")
    parser.add_argument("--out", default=str(OUTPUT_DIR / "cv_ev_candidates.csv"))
    parser.add_argument("--progress-every", type=int, default=5000)
    parser.add_argument("--write-every", type=int, default=2000)
    args = parser.parse_args()

    df = load_dataset(args)
    if args.limit_per_year:
        sorted_df = df.sort_values(["year", "date", "jcd", "rno"])
        if args.sample_mode == "tail":
            df = sorted_df.groupby("year", group_keys=False).tail(args.limit_per_year)
        else:
            df = sorted_df.groupby("year", group_keys=False).head(args.limit_per_year)
        sample_label = args.sample_mode
    else:
        sample_label = "all"
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    if out_path.exists():
        out_path.unlink()
    first_write = True
    rows = []

    def flush_rows() -> None:
        nonlocal first_write, rows
        if not rows:
            return
        pd.DataFrame(rows).to_csv(out_path, mode="w" if first_write else "a", header=first_write, index=False, encoding="utf-8")
        first_write = False
        rows = []
    for valid_year in args.valid_years:
        valid_df = df[df["year"] == valid_year].copy().reset_index(drop=True)
        models_dir = Path(args.models_dir) if args.models_dir else Path(args.cv_dir) / f"fold_valid_{valid_year}"
        probs = {
            f"pos{pos}": SoftmaxRegression.load(models_dir / f"finish_pos{pos}_softmax.npz").predict_proba(valid_df)
            for pos in range(1, 4)
        }
        upset = BinaryLogisticRegression.load(models_dir / "upset_logistic.npz").predict_proba(valid_df)
        high10000 = BinaryLogisticRegression.load(models_dir / "is_high_payout_10000_logistic.npz").predict_proba(valid_df)

        for idx, race in valid_df.iterrows():
            if args.progress_every and (idx + 1) % args.progress_every == 0:
                print(f"year={valid_year} races={idx + 1}/{len(valid_df)}", flush=True)
            date = format_date(race["date"])
            jcd = int(race["jcd"])
            rno = int(race["rno"])
            actual = str(race.get("trifecta", ""))
            payout = float(race.get("payout_3t", 0.0) or 0.0)
            odds_file = odds_path(date, jcd, rno)
            if not odds_file.exists() or not actual or payout <= 0:
                continue
            odds_rows = parse_odds3t(odds_file)
            if not odds_rows:
                continue
            odds_by_combo = {str(item["combination"]): float(item["odds"]) for item in odds_rows}
            candidates = []
            for combo, first, second, third, raw_prob in combo_probabilities(
                probs["pos1"][idx], probs["pos2"][idx], probs["pos3"][idx]
            ):
                odds = odds_by_combo.get(combo)
                if odds is None:
                    continue
                candidates.append((raw_prob * odds, raw_prob, odds, combo, first, second, third))
            candidates.sort(reverse=True)
            for rank, (_, raw_prob, odds, combo, first, second, third) in enumerate(candidates[: args.top_candidates], start=1):
                rows.append(
                    {
                        "valid_year": valid_year,
                        "sample": sample_label,
                        "date": date,
                        "jcd": jcd,
                        "rno": rno,
                        "candidate_rank": rank,
                        "combination": combo,
                        "first": first,
                        "second": second,
                        "third": third,
                        "raw_prob": raw_prob,
                        "odds": odds,
                        "actual": actual,
                        "hit": combo == actual,
                        "payout_3t": payout,
                        "upset_prob": float(upset[idx]),
                        "high10000_prob": float(high10000[idx]),
                    }
                )
            if args.write_every and (idx + 1) % args.write_every == 0:
                flush_rows()

        flush_rows()

    total_rows = sum(1 for _ in out_path.open("r", encoding="utf-8")) - 1 if out_path.exists() else 0
    print(f"rows={total_rows}")
    print(f"out={out_path}")


if __name__ == "__main__":
    main()
