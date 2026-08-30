from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd


def parse_floats(text: str) -> list[float]:
    return [float(item) for item in text.split(",") if item]


def parse_ints(text: str) -> list[int]:
    return [int(item) for item in text.split(",") if item]


def summarize(selected: pd.DataFrame, config: dict[str, float | int]) -> dict[str, float | int]:
    if selected.empty:
        return {**config, "races": 0, "bets": 0, "hits": 0, "stake": 0, "ret": 0, "roi": 0, "profit": 0}
    stake = len(selected) * 100
    ret = float((selected.loc[selected["hit"], "payout_3t"]).sum())
    races = selected[["valid_year", "date", "jcd", "rno"]].drop_duplicates().shape[0]
    return {
        **config,
        "races": races,
        "bets": len(selected),
        "hits": int(selected["hit"].sum()),
        "hit_rate": float(selected["hit"].mean()),
        "stake": stake,
        "ret": ret,
        "roi": ret / stake if stake else 0.0,
        "profit": ret - stake,
        "avg_odds": float(selected["odds"].mean()),
        "avg_raw_prob": float(selected["raw_prob"].mean()),
        "avg_ev": float(selected["ev"].mean()),
    }


def grouped_roi(selected: pd.DataFrame, group_cols: list[str]) -> tuple[float, str]:
    if selected.empty or not group_cols:
        return 0.0, ""
    parts = []
    rois = []
    for key, group in selected.groupby(group_cols):
        stake = len(group) * 100
        ret = float(group.loc[group["hit"], "payout_3t"].sum())
        roi = ret / stake if stake else 0.0
        rois.append(roi)
        if not isinstance(key, tuple):
            key = (key,)
        parts.append("/".join(map(str, key)) + f":{roi:.3f}")
    return (min(rois) if rois else 0.0), ";".join(parts)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--candidates", required=True)
    parser.add_argument("--out", required=True)
    parser.add_argument("--prob-scales", default="0.05,0.08,0.1,0.12,0.15,0.2,0.25,0.3,0.35")
    parser.add_argument("--min-evs", default="-0.2,-0.1,0,0.05,0.1,0.15,0.25,0.4")
    parser.add_argument("--top-ranks", default="1,2,3,5,10")
    parser.add_argument("--min-odds", default="1,10,20,50,100")
    parser.add_argument("--max-odds", default="150,300,500,800,1200,999999")
    parser.add_argument("--min-upsets", default="0,0.25,0.35,0.45,0.55")
    parser.add_argument("--max-upsets", default="0.45,0.6,0.75,1")
    parser.add_argument("--min-high10000s", default="0,0.08,0.12,0.16,0.22")
    parser.add_argument("--min-bets", type=int, default=100)
    parser.add_argument("--group-cols", default="")
    parser.add_argument("--min-group-roi", type=float, default=None)
    args = parser.parse_args()

    df = pd.read_csv(args.candidates)
    rows = []
    for prob_scale in parse_floats(args.prob_scales):
        scaled_prob = df["raw_prob"] * prob_scale
        ev = scaled_prob * df["odds"] - 1.0
        base = df.assign(ev=ev, scaled_prob=scaled_prob)
        for min_ev in parse_floats(args.min_evs):
            ev_df = base[base["ev"] >= min_ev]
            if ev_df.empty:
                continue
            for top_rank in parse_ints(args.top_ranks):
                rank_df = ev_df[ev_df["candidate_rank"] <= top_rank]
                if rank_df.empty:
                    continue
                for min_odds in parse_floats(args.min_odds):
                    for max_odds in parse_floats(args.max_odds):
                        if min_odds > max_odds:
                            continue
                        odds_df = rank_df[(rank_df["odds"] >= min_odds) & (rank_df["odds"] <= max_odds)]
                        if len(odds_df) < args.min_bets:
                            continue
                        for min_upset in parse_floats(args.min_upsets):
                            for max_upset in parse_floats(args.max_upsets):
                                if min_upset > max_upset:
                                    continue
                                upset_df = odds_df[(odds_df["upset_prob"] >= min_upset) & (odds_df["upset_prob"] <= max_upset)]
                                if len(upset_df) < args.min_bets:
                                    continue
                                for min_high in parse_floats(args.min_high10000s):
                                    selected = upset_df[upset_df["high10000_prob"] >= min_high]
                                    if len(selected) < args.min_bets:
                                        continue
                                    group_cols = [col for col in args.group_cols.split(",") if col]
                                    min_group_roi, group_rois = grouped_roi(selected, group_cols)
                                    if args.min_group_roi is not None and min_group_roi < args.min_group_roi:
                                        continue
                                    summary = summarize(
                                        selected,
                                        {
                                            "prob_scale": prob_scale,
                                            "min_ev": min_ev,
                                            "top_rank": top_rank,
                                            "min_odds": min_odds,
                                            "max_odds": max_odds,
                                            "min_upset": min_upset,
                                            "max_upset": max_upset,
                                            "min_high10000": min_high,
                                        },
                                    )
                                    summary["min_group_roi"] = min_group_roi
                                    summary["group_rois"] = group_rois
                                    rows.append(summary)
    out = pd.DataFrame(rows)
    out = out.sort_values(["roi", "bets"], ascending=[False, False])
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(out_path, index=False, encoding="utf-8")
    print(out.head(40).to_string(index=False))
    print(f"out={out_path}")


if __name__ == "__main__":
    main()
