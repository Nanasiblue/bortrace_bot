from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd

from bortrace.baseline_model import BinaryLogisticRegression, SoftmaxRegression
from bortrace.paths import LEGACY_DATA_ROOT, OUTPUT_DIR
from build_odds3t_dataset import parse_odds3t
from predict_official_models import load_dataset
from train_official_models import greedy_order_from_position_probs


def odds_path(date: str, jcd: int, rno: int) -> Path:
    return LEGACY_DATA_ROOT / "official_pages" / str(date) / f"{int(jcd):02d}" / str(int(rno)) / "odds3t.html"


def format_date(value: object) -> str:
    if pd.isna(value):
        return ""
    if isinstance(value, (int, np.integer)):
        return f"{int(value):08d}"
    if isinstance(value, (float, np.floating)):
        return f"{int(value):08d}"
    text = str(value).strip()
    if text.endswith(".0") and text[:-2].isdigit():
        return f"{int(float(text)):08d}"
    return text.replace("-", "")


def combo_probabilities(pos1: np.ndarray, pos2: np.ndarray, pos3: np.ndarray) -> list[tuple[str, int, int, int, float]]:
    rows: list[tuple[str, int, int, int, float]] = []
    total = 0.0
    for first in range(1, 7):
        for second in range(1, 7):
            if second == first:
                continue
            for third in range(1, 7):
                if third == first or third == second:
                    continue
                prob = float(pos1[first - 1] * pos2[second - 1] * pos3[third - 1])
                rows.append((f"{first}-{second}-{third}", first, second, third, prob))
                total += prob
    if total > 0:
        rows = [(combo, first, second, third, prob / total) for combo, first, second, third, prob in rows]
    return rows


def kelly_fraction(prob: float, odds: float) -> float:
    if odds <= 1.0:
        return 0.0
    return max(0.0, (prob * odds - 1.0) / (odds - 1.0))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset", default=str(OUTPUT_DIR / "official_race_dataset.csv"))
    parser.add_argument("--dataset-dir", default=None)
    parser.add_argument("--start-month", default=None)
    parser.add_argument("--end-month", default=None)
    parser.add_argument("--models-dir", default=str(OUTPUT_DIR / "official_models"))
    parser.add_argument("--out", default=str(OUTPUT_DIR / "trifecta_ev_candidates.csv"))
    parser.add_argument("--year", type=int, default=None)
    parser.add_argument("--date", default=None)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--top-n", type=int, default=5)
    parser.add_argument("--min-ev", type=float, default=0.0)
    args = parser.parse_args()

    df = load_dataset(args)
    if args.year is not None:
        df = df[df["year"] == args.year].copy()
    if args.date:
        df = df[df["date"].astype(str) == args.date].copy()
    if args.limit:
        df = df.head(args.limit).copy()
    if df.empty:
        raise SystemExit("No rows to predict")

    models_dir = Path(args.models_dir)
    pos_probs = {}
    for pos in range(1, 7):
        model = SoftmaxRegression.load(models_dir / f"finish_pos{pos}_softmax.npz")
        pos_probs[f"pos{pos}"] = model.predict_proba(df)
    pred_order = greedy_order_from_position_probs(pos_probs) + 1

    binary_probs: dict[str, np.ndarray] = {}
    for target in ["upset", "is_high_payout_5000", "is_high_payout_10000", "is_high_payout_30000"]:
        path = models_dir / f"{target}_logistic.npz"
        if path.exists():
            binary_probs[target] = BinaryLogisticRegression.load(path).predict_proba(df)

    output_rows: list[dict[str, float | int | str]] = []
    for idx, row in df.reset_index(drop=True).iterrows():
        date = format_date(row["date"])
        jcd = int(row["jcd"])
        rno = int(row["rno"])
        odds_file = odds_path(date, jcd, rno)
        if not odds_file.exists():
            continue
        odds_rows = parse_odds3t(odds_file)
        if not odds_rows:
            continue
        odds_by_combo = {str(item["combination"]): float(item["odds"]) for item in odds_rows}
        candidates = []
        for combo, first, second, third, prob in combo_probabilities(
            pos_probs["pos1"][idx], pos_probs["pos2"][idx], pos_probs["pos3"][idx]
        ):
            odds = odds_by_combo.get(combo)
            if odds is None:
                continue
            ev = prob * odds - 1.0
            if ev < args.min_ev:
                continue
            candidates.append(
                {
                    "date": date,
                    "jcd": jcd,
                    "rno": rno,
                    "combination": combo,
                    "first": first,
                    "second": second,
                    "third": third,
                    "prob": prob,
                    "odds": odds,
                    "ev": ev,
                    "kelly": kelly_fraction(prob, odds),
                    "pred_order": "-".join(map(str, pred_order[idx])),
                }
            )
        candidates.sort(key=lambda item: (float(item["ev"]), float(item["prob"])), reverse=True)
        for item in candidates[: args.top_n]:
            for target, probs in binary_probs.items():
                item[f"{target}_prob"] = float(probs[idx])
            output_rows.append(item)

    columns = [
        "date",
        "jcd",
        "rno",
        "combination",
        "first",
        "second",
        "third",
        "prob",
        "odds",
        "ev",
        "kelly",
        "pred_order",
        "upset_prob",
        "is_high_payout_5000_prob",
        "is_high_payout_10000_prob",
        "is_high_payout_30000_prob",
    ]
    out = pd.DataFrame(output_rows, columns=columns)
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(out_path, index=False, encoding="utf-8")
    print(f"rows={len(out)}")
    print(f"out={out_path}")


if __name__ == "__main__":
    main()
