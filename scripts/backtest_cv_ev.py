from __future__ import annotations

import argparse
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd

from bortrace.baseline_model import SoftmaxRegression
from bortrace.baseline_model import BinaryLogisticRegression
from bortrace.paths import OUTPUT_DIR
from build_odds3t_dataset import parse_odds3t
from predict_official_models import load_dataset
from predict_trifecta_ev import combo_probabilities, format_date, kelly_fraction, odds_path


@dataclass(frozen=True)
class Strategy:
    prob_scale: float
    min_ev: float
    top_n: int
    kelly_scale: float
    min_odds: float
    max_odds: float
    min_upset: float
    max_upset: float
    min_high10000: float

    @property
    def key(self) -> tuple[float, float, int, float, float, float, float, float, float]:
        return (
            self.prob_scale,
            self.min_ev,
            self.top_n,
            self.kelly_scale,
            self.min_odds,
            self.max_odds,
            self.min_upset,
            self.max_upset,
            self.min_high10000,
        )


def load_position_probs(models_dir: Path, df: pd.DataFrame) -> dict[str, np.ndarray]:
    probs: dict[str, np.ndarray] = {}
    for pos in range(1, 4):
        model = SoftmaxRegression.load(models_dir / f"finish_pos{pos}_softmax.npz")
        probs[f"pos{pos}"] = model.predict_proba(df)
    return probs


def init_summary(strategy: Strategy) -> dict[str, float | int]:
    return {
        "prob_scale": strategy.prob_scale,
        "min_ev": strategy.min_ev,
        "top_n": strategy.top_n,
        "kelly_scale": strategy.kelly_scale,
        "min_odds": strategy.min_odds,
        "max_odds": strategy.max_odds,
        "min_upset": strategy.min_upset,
        "max_upset": strategy.max_upset,
        "min_high10000": strategy.min_high10000,
        "races": 0,
        "bets": 0,
        "hits": 0,
        "unit_stake": 0.0,
        "unit_return": 0.0,
        "kelly_stake": 0.0,
        "kelly_return": 0.0,
        "sum_ev": 0.0,
        "sum_prob": 0.0,
        "sum_odds": 0.0,
    }


def add_bet(
    summary: dict[str, float | int],
    *,
    hit: bool,
    payout_3t: float,
    odds: float,
    prob: float,
    ev: float,
    kelly: float,
    unit_stake: float,
    bankroll: float,
    max_kelly_fraction: float,
) -> None:
    summary["bets"] += 1
    summary["unit_stake"] += unit_stake
    summary["sum_ev"] += ev
    summary["sum_prob"] += prob
    summary["sum_odds"] += odds
    kelly_stake = bankroll * min(max_kelly_fraction, kelly)
    summary["kelly_stake"] += kelly_stake
    if hit:
        summary["hits"] += 1
        summary["unit_return"] += payout_3t * (unit_stake / 100.0)
        summary["kelly_return"] += payout_3t * (kelly_stake / 100.0)


def finalize_summary(row: dict[str, float | int], valid_year: int) -> dict[str, float | int]:
    bets = int(row["bets"])
    unit_stake = float(row["unit_stake"])
    kelly_stake = float(row["kelly_stake"])
    row = dict(row)
    row["valid_year"] = valid_year
    row["hit_rate"] = float(row["hits"]) / bets if bets else 0.0
    row["unit_roi"] = float(row["unit_return"]) / unit_stake if unit_stake else 0.0
    row["unit_profit"] = float(row["unit_return"]) - unit_stake
    row["kelly_roi"] = float(row["kelly_return"]) / kelly_stake if kelly_stake else 0.0
    row["kelly_profit"] = float(row["kelly_return"]) - kelly_stake
    row["avg_ev"] = float(row["sum_ev"]) / bets if bets else 0.0
    row["avg_prob"] = float(row["sum_prob"]) / bets if bets else 0.0
    row["avg_odds"] = float(row["sum_odds"]) / bets if bets else 0.0
    row["bets_per_race"] = bets / int(row["races"]) if int(row["races"]) else 0.0
    return row


def backtest_fold(
    df: pd.DataFrame,
    cv_dir: Path,
    valid_year: int,
    strategies: list[Strategy],
    unit_stake: float,
    bankroll: float,
    max_kelly_fraction: float,
    progress_every: int,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    valid_df = df[df["year"] == valid_year].copy().reset_index(drop=True)
    if valid_df.empty:
        raise SystemExit(f"No rows for valid year: {valid_year}")

    models_dir = cv_dir / f"fold_valid_{valid_year}"
    probs = load_position_probs(models_dir, valid_df)
    upset_path = models_dir / "upset_logistic.npz"
    high10000_path = models_dir / "is_high_payout_10000_logistic.npz"
    upset_probs = BinaryLogisticRegression.load(upset_path).predict_proba(valid_df) if upset_path.exists() else np.zeros(len(valid_df))
    high10000_probs = (
        BinaryLogisticRegression.load(high10000_path).predict_proba(valid_df)
        if high10000_path.exists()
        else np.zeros(len(valid_df))
    )
    summaries = {strategy.key: init_summary(strategy) for strategy in strategies}
    best_strategy = max(strategies, key=lambda item: (item.prob_scale, item.min_ev, item.top_n))
    sample_rows: list[dict[str, float | int | str | bool]] = []

    for idx, row in valid_df.iterrows():
        if progress_every and (idx + 1) % progress_every == 0:
            print(f"year={valid_year} races={idx + 1}/{len(valid_df)}", flush=True)
        date = format_date(row["date"])
        jcd = int(row["jcd"])
        rno = int(row["rno"])
        trifecta = str(row.get("trifecta", ""))
        payout_3t = float(row.get("payout_3t", 0.0) or 0.0)
        odds_file = odds_path(date, jcd, rno)
        if not odds_file.exists() or payout_3t <= 0 or not trifecta:
            continue
        odds_rows = parse_odds3t(odds_file)
        if not odds_rows:
            continue
        odds_by_combo = {str(item["combination"]): float(item["odds"]) for item in odds_rows}

        race_upset = float(upset_probs[idx])
        race_high10000 = float(high10000_probs[idx])
        race_candidates = []
        for combo, first, second, third, raw_prob in combo_probabilities(
            probs["pos1"][idx], probs["pos2"][idx], probs["pos3"][idx]
        ):
            odds = odds_by_combo.get(combo)
            if odds is None:
                continue
            race_candidates.append(
                {
                    "combination": combo,
                    "first": first,
                    "second": second,
                    "third": third,
                    "raw_prob": raw_prob,
                    "odds": odds,
                    "score": raw_prob * odds,
                }
            )
        race_candidates.sort(key=lambda item: (float(item["score"]), float(item["raw_prob"])), reverse=True)

        used_in_strategy: set[tuple[float, float, int, float]] = set()
        for strategy in strategies:
            selected = []
            for candidate in race_candidates:
                prob = float(candidate["raw_prob"]) * strategy.prob_scale
                odds = float(candidate["odds"])
                ev = prob * odds - 1.0
                odds = float(candidate["odds"])
                if odds < strategy.min_odds or odds > strategy.max_odds:
                    continue
                if race_upset < strategy.min_upset or race_upset > strategy.max_upset:
                    continue
                if race_high10000 < strategy.min_high10000:
                    continue
                if ev < strategy.min_ev:
                    continue
                selected.append((candidate, prob, ev))
                if len(selected) >= strategy.top_n:
                    break
            if selected:
                summaries[strategy.key]["races"] += 1
            for candidate, prob, ev in selected:
                combo = str(candidate["combination"])
                hit = combo == trifecta
                kelly = kelly_fraction(prob, float(candidate["odds"])) * strategy.kelly_scale
                add_bet(
                    summaries[strategy.key],
                    hit=hit,
                    payout_3t=payout_3t,
                    odds=float(candidate["odds"]),
                    prob=prob,
                    ev=ev,
                    kelly=kelly,
                    unit_stake=unit_stake,
                    bankroll=bankroll,
                    max_kelly_fraction=max_kelly_fraction,
                )
                used_in_strategy.add(strategy.key)
                if strategy == best_strategy and len(sample_rows) < 50000:
                    sample_rows.append(
                        {
                            "valid_year": valid_year,
                            "date": date,
                            "jcd": jcd,
                            "rno": rno,
                            "combination": combo,
                            "actual": trifecta,
                            "hit": hit,
                            "raw_prob": float(candidate["raw_prob"]),
                            "prob": prob,
                            "odds": float(candidate["odds"]),
                            "ev": ev,
                            "kelly": kelly,
                            "payout_3t": payout_3t,
                            "upset_prob": race_upset,
                            "high10000_prob": race_high10000,
                        }
                    )

    summary_rows = [finalize_summary(summary, valid_year) for summary in summaries.values()]
    return pd.DataFrame(summary_rows), pd.DataFrame(sample_rows)


def parse_float_list(values: list[str]) -> list[float]:
    return [float(value) for value in values]


def parse_int_list(values: list[str]) -> list[int]:
    return [int(value) for value in values]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset", default=None)
    parser.add_argument("--dataset-dir", default=None)
    parser.add_argument("--start-month", default="202101")
    parser.add_argument("--end-month", default="202512")
    parser.add_argument("--cv-dir", default=str(OUTPUT_DIR / "official_cv"))
    parser.add_argument("--valid-years", nargs="+", type=int, default=[2023, 2024, 2025])
    parser.add_argument("--prob-scales", nargs="+", default=["0.35", "0.5", "0.65", "0.8", "1.0"])
    parser.add_argument("--min-evs", nargs="+", default=["0.05", "0.15", "0.3", "0.5", "1.0"])
    parser.add_argument("--top-ns", nargs="+", default=["1", "3", "5"])
    parser.add_argument("--kelly-scales", nargs="+", default=["0.25"])
    parser.add_argument("--min-odds", nargs="+", default=["1.0"])
    parser.add_argument("--max-odds", nargs="+", default=["999999"])
    parser.add_argument("--min-upsets", nargs="+", default=["0.0"])
    parser.add_argument("--max-upsets", nargs="+", default=["1.0"])
    parser.add_argument("--min-high10000s", nargs="+", default=["0.0"])
    parser.add_argument("--unit-stake", type=float, default=100.0)
    parser.add_argument("--bankroll", type=float, default=100000.0)
    parser.add_argument("--max-kelly-fraction", type=float, default=0.003)
    parser.add_argument("--out-summary", default=str(OUTPUT_DIR / "cv_ev_backtest_summary.csv"))
    parser.add_argument("--out-sample-bets", default=str(OUTPUT_DIR / "cv_ev_backtest_sample_bets.csv"))
    parser.add_argument("--progress-every", type=int, default=5000)
    parser.add_argument("--limit-per-year", type=int, default=None)
    args = parser.parse_args()

    df = load_dataset(args)
    if args.limit_per_year:
        df = (
            df.sort_values(["year", "date", "jcd", "rno"])
            .groupby("year", group_keys=False)
            .head(args.limit_per_year)
            .copy()
        )

    strategies = [
        Strategy(
            prob_scale=prob_scale,
            min_ev=min_ev,
            top_n=top_n,
            kelly_scale=kelly_scale,
            min_odds=min_odds,
            max_odds=max_odds,
            min_upset=min_upset,
            max_upset=max_upset,
            min_high10000=min_high10000,
        )
        for prob_scale in parse_float_list(args.prob_scales)
        for min_ev in parse_float_list(args.min_evs)
        for top_n in parse_int_list(args.top_ns)
        for kelly_scale in parse_float_list(args.kelly_scales)
        for min_odds in parse_float_list(args.min_odds)
        for max_odds in parse_float_list(args.max_odds)
        for min_upset in parse_float_list(args.min_upsets)
        for max_upset in parse_float_list(args.max_upsets)
        for min_high10000 in parse_float_list(args.min_high10000s)
        if min_odds <= max_odds and min_upset <= max_upset
    ]

    all_summaries = []
    sample_bets = []
    for valid_year in args.valid_years:
        summary, bets = backtest_fold(
            df=df,
            cv_dir=Path(args.cv_dir),
            valid_year=valid_year,
            strategies=strategies,
            unit_stake=args.unit_stake,
            bankroll=args.bankroll,
            max_kelly_fraction=args.max_kelly_fraction,
            progress_every=args.progress_every,
        )
        all_summaries.append(summary)
        sample_bets.append(bets)

    summary_df = pd.concat(all_summaries, ignore_index=True)
    group_cols = [
        "prob_scale",
        "min_ev",
        "top_n",
        "kelly_scale",
        "min_odds",
        "max_odds",
        "min_upset",
        "max_upset",
        "min_high10000",
    ]
    total = summary_df.groupby(group_cols, as_index=False).agg(
        valid_year=("valid_year", lambda values: "ALL"),
        races=("races", "sum"),
        bets=("bets", "sum"),
        hits=("hits", "sum"),
        unit_stake=("unit_stake", "sum"),
        unit_return=("unit_return", "sum"),
        kelly_stake=("kelly_stake", "sum"),
        kelly_return=("kelly_return", "sum"),
        sum_ev=("sum_ev", "sum"),
        sum_prob=("sum_prob", "sum"),
        sum_odds=("sum_odds", "sum"),
    )
    total["hit_rate"] = np.where(total["bets"] > 0, total["hits"] / total["bets"], 0.0)
    total["unit_roi"] = np.where(total["unit_stake"] > 0, total["unit_return"] / total["unit_stake"], 0.0)
    total["unit_profit"] = total["unit_return"] - total["unit_stake"]
    total["kelly_roi"] = np.where(total["kelly_stake"] > 0, total["kelly_return"] / total["kelly_stake"], 0.0)
    total["kelly_profit"] = total["kelly_return"] - total["kelly_stake"]
    total["avg_ev"] = np.where(total["bets"] > 0, total["sum_ev"] / total["bets"], 0.0)
    total["avg_prob"] = np.where(total["bets"] > 0, total["sum_prob"] / total["bets"], 0.0)
    total["avg_odds"] = np.where(total["bets"] > 0, total["sum_odds"] / total["bets"], 0.0)
    total["bets_per_race"] = np.where(total["races"] > 0, total["bets"] / total["races"], 0.0)

    out_summary = pd.concat([summary_df, total], ignore_index=True, sort=False)
    out_summary = out_summary.sort_values(["valid_year", "unit_roi", "bets"], ascending=[True, False, False])
    out_summary_path = Path(args.out_summary)
    out_summary_path.parent.mkdir(parents=True, exist_ok=True)
    out_summary.to_csv(out_summary_path, index=False, encoding="utf-8")

    bets_df = pd.concat(sample_bets, ignore_index=True, sort=False) if sample_bets else pd.DataFrame()
    bets_path = Path(args.out_sample_bets)
    bets_df.to_csv(bets_path, index=False, encoding="utf-8")

    print("Top ALL strategies by unit_roi:")
    print(
        out_summary[out_summary["valid_year"].astype(str) == "ALL"]
        .sort_values(["unit_roi", "bets"], ascending=[False, False])
        .head(20)
        .to_string(index=False)
    )
    print(f"summary={out_summary_path}")
    print(f"sample_bets={bets_path}")


if __name__ == "__main__":
    main()
