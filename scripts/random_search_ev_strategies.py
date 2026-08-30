from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd


def evaluate(df: pd.DataFrame, cfg: dict[str, float | int]) -> dict[str, float | int | str] | None:
    ev = df["raw_prob"] * float(cfg["prob_scale"]) * df["odds"] - 1.0
    mask = (
        (df["candidate_rank"] <= int(cfg["top_rank"]))
        & (df["odds"] >= float(cfg["min_odds"]))
        & (df["odds"] <= float(cfg["max_odds"]))
        & (df["upset_prob"] >= float(cfg["min_upset"]))
        & (df["upset_prob"] <= float(cfg["max_upset"]))
        & (df["high10000_prob"] >= float(cfg["min_high10000"]))
        & (ev >= float(cfg["min_ev"]))
    )
    selected = df.loc[mask].copy()
    if len(selected) < int(cfg["min_bets"]):
        return None
    selected["ev"] = ev[mask]
    stake = len(selected) * 100.0
    ret = float(selected.loc[selected["hit"], "payout_3t"].sum())
    group_rows = []
    min_group_roi = 999.0
    zero_groups = 0
    for key, group in selected.groupby(["sample", "valid_year"]):
        group_stake = len(group) * 100.0
        group_ret = float(group.loc[group["hit"], "payout_3t"].sum())
        group_roi = group_ret / group_stake if group_stake else 0.0
        min_group_roi = min(min_group_roi, group_roi)
        zero_groups += 1 if group_ret <= 0 else 0
        group_rows.append(f"{key[0]}/{key[1]}:{group_roi:.3f}({len(group)})")
    if len(group_rows) < 6:
        return None
    roi = ret / stake if stake else 0.0
    # Penalize strategies that win by one narrow pocket but collapse elsewhere.
    score = roi * min(1.0, min_group_roi + 0.25) * (1.0 - zero_groups / 8.0)
    return {
        **cfg,
        "bets": len(selected),
        "races": selected[["sample", "valid_year", "date", "jcd", "rno"]].drop_duplicates().shape[0],
        "hits": int(selected["hit"].sum()),
        "stake": stake,
        "ret": ret,
        "roi": roi,
        "profit": ret - stake,
        "hit_rate": float(selected["hit"].mean()),
        "min_group_roi": min_group_roi,
        "zero_groups": zero_groups,
        "score": score,
        "avg_odds": float(selected["odds"].mean()),
        "avg_raw_prob": float(selected["raw_prob"].mean()),
        "avg_ev": float(selected["ev"].mean()),
        "group_rois": ";".join(group_rows),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--candidates", required=True)
    parser.add_argument("--out", required=True)
    parser.add_argument("--iterations", type=int, default=3000)
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument("--min-bets", type=int, default=400)
    args = parser.parse_args()

    df = pd.read_csv(args.candidates)
    rng = np.random.default_rng(args.seed)
    rows = []
    for _ in range(args.iterations):
        min_odds = float(rng.choice([1, 5, 10, 20, 30, 50, 80, 100]))
        max_odds = float(rng.choice([80, 120, 150, 200, 300, 400, 500, 800, 1200]))
        if min_odds > max_odds:
            min_odds, max_odds = max_odds, min_odds
        min_upset = float(rng.choice([0.0, 0.15, 0.2, 0.25, 0.3, 0.35, 0.4, 0.45, 0.5]))
        max_upset = float(rng.choice([0.45, 0.55, 0.6, 0.7, 0.8, 1.0]))
        if min_upset > max_upset:
            min_upset, max_upset = max_upset, min_upset
        cfg = {
            "prob_scale": float(rng.choice([0.04, 0.05, 0.06, 0.08, 0.1, 0.12, 0.15, 0.18, 0.2, 0.25, 0.3])),
            "min_ev": float(rng.choice([-0.4, -0.3, -0.2, -0.1, -0.05, 0.0, 0.05, 0.1, 0.2, 0.35])),
            "top_rank": int(rng.choice([1, 2, 3, 4, 5, 8, 10, 15, 20, 30])),
            "min_odds": min_odds,
            "max_odds": max_odds,
            "min_upset": min_upset,
            "max_upset": max_upset,
            "min_high10000": float(rng.choice([0.0, 0.04, 0.06, 0.08, 0.1, 0.12, 0.15, 0.2])),
            "min_bets": args.min_bets,
        }
        row = evaluate(df, cfg)
        if row is not None:
            rows.append(row)

    out = pd.DataFrame(rows)
    if not out.empty:
        out = out.sort_values(["score", "roi", "bets"], ascending=[False, False, False])
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(out_path, index=False, encoding="utf-8")
    print(out.head(40).to_string(index=False) if not out.empty else "no results")
    print(f"out={out_path}")


if __name__ == "__main__":
    main()
