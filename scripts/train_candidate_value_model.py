from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd


def add_features(df: pd.DataFrame, prob_scale: float) -> pd.DataFrame:
    out = df.copy()
    out["odds"] = out["odds"].astype(float)
    out["raw_prob"] = out["raw_prob"].astype(float)
    out["log_odds"] = np.log1p(out["odds"])
    out["implied_prob"] = 1.0 / out["odds"].clip(lower=1.01)
    out["raw_ev"] = out["raw_prob"] * out["odds"] - 1.0
    out["scaled_ev"] = out["raw_prob"] * prob_scale * out["odds"] - 1.0
    out["prob_over_implied"] = out["raw_prob"] / out["implied_prob"]
    out["rank_inv"] = 1.0 / out["candidate_rank"].astype(float)
    out["upset_x_odds"] = out["upset_prob"] * out["log_odds"]
    out["high10000_x_odds"] = out["high10000_prob"] * out["log_odds"]
    out["prob_x_upset"] = out["raw_prob"] * out["upset_prob"]
    out["prob_x_high10000"] = out["raw_prob"] * out["high10000_prob"]
    for boat in range(1, 7):
        out[f"first_{boat}"] = (out["first"] == boat).astype(float)
        out[f"second_{boat}"] = (out["second"] == boat).astype(float)
        out[f"third_{boat}"] = (out["third"] == boat).astype(float)
    return out


FEATURE_COLUMNS = [
    "candidate_rank",
    "rank_inv",
    "raw_prob",
    "odds",
    "log_odds",
    "implied_prob",
    "raw_ev",
    "scaled_ev",
    "prob_over_implied",
    "upset_prob",
    "high10000_prob",
    "upset_x_odds",
    "high10000_x_odds",
    "prob_x_upset",
    "prob_x_high10000",
] + [f"{pos}_{boat}" for pos in ["first", "second", "third"] for boat in range(1, 7)]


def standardize(train_x: np.ndarray, valid_x: np.ndarray) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    mean = np.nanmean(train_x, axis=0)
    scale = np.nanstd(train_x, axis=0)
    scale[scale == 0] = 1.0
    train_x = np.where(np.isnan(train_x), mean, train_x)
    valid_x = np.where(np.isnan(valid_x), mean, valid_x)
    return (train_x - mean) / scale, (valid_x - mean) / scale, mean, scale


def sigmoid(z: np.ndarray) -> np.ndarray:
    return 1.0 / (1.0 + np.exp(-np.clip(z, -40, 40)))


def fit_logistic(
    train_x: np.ndarray,
    y: np.ndarray,
    *,
    epochs: int,
    lr: float,
    l2: float,
    pos_weight: float,
) -> tuple[np.ndarray, float]:
    n, d = train_x.shape
    w = np.zeros(d, dtype=float)
    b = 0.0
    weights = np.where(y > 0, pos_weight, 1.0)
    weight_sum = weights.sum()
    for _ in range(epochs):
        p = sigmoid(train_x @ w + b)
        grad = ((p - y) * weights) / weight_sum
        w -= lr * (train_x.T @ grad + l2 * w)
        b -= lr * float(grad.sum())
    return w, b


def predict(x: np.ndarray, w: np.ndarray, b: float) -> np.ndarray:
    return sigmoid(x @ w + b)


def evaluate_strategy(
    df: pd.DataFrame,
    score_col: str,
    *,
    top_per_race: int,
    min_score_quantile: float,
    min_odds: float,
    max_odds: float,
) -> dict[str, float | int]:
    scored = df[(df["odds"] >= min_odds) & (df["odds"] <= max_odds)].copy()
    if scored.empty:
        return {"bets": 0, "hits": 0, "roi": 0.0, "profit": 0.0}
    threshold = float(scored[score_col].quantile(min_score_quantile))
    scored = scored[scored[score_col] >= threshold]
    if scored.empty:
        return {"bets": 0, "hits": 0, "roi": 0.0, "profit": 0.0}
    scored = scored.sort_values(["valid_year", "date", "jcd", "rno", score_col], ascending=[True, True, True, True, False])
    picked = scored.groupby(["valid_year", "date", "jcd", "rno"], group_keys=False).head(top_per_race)
    stake = len(picked) * 100.0
    ret = float(picked.loc[picked["hit"], "payout_3t"].sum())
    return {
        "top_per_race": top_per_race,
        "min_score_quantile": min_score_quantile,
        "min_odds": min_odds,
        "max_odds": max_odds,
        "races": picked[["valid_year", "date", "jcd", "rno"]].drop_duplicates().shape[0],
        "bets": len(picked),
        "hits": int(picked["hit"].sum()),
        "hit_rate": float(picked["hit"].mean()) if len(picked) else 0.0,
        "stake": stake,
        "ret": ret,
        "roi": ret / stake if stake else 0.0,
        "profit": ret - stake,
        "avg_odds": float(picked["odds"].mean()) if len(picked) else 0.0,
        "avg_score": float(picked[score_col].mean()) if len(picked) else 0.0,
    }


def grouped_summary(df: pd.DataFrame, score_col: str, cfg: dict[str, float | int]) -> str:
    parts = []
    for key, group in df.groupby(["sample", "valid_year"]):
        result = evaluate_strategy(
            group,
            score_col,
            top_per_race=int(cfg["top_per_race"]),
            min_score_quantile=float(cfg["min_score_quantile"]),
            min_odds=float(cfg["min_odds"]),
            max_odds=float(cfg["max_odds"]),
        )
        parts.append(f"{key[0]}/{key[1]}:{result['roi']:.3f}({result['bets']})")
    return ";".join(parts)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--candidates", required=True)
    parser.add_argument("--out-dir", required=True)
    parser.add_argument("--train-sample", default="head")
    parser.add_argument("--valid-sample", default="tail")
    parser.add_argument("--train-years", nargs="+", type=int, default=None)
    parser.add_argument("--valid-years", nargs="+", type=int, default=None)
    parser.add_argument("--prob-scale", type=float, default=0.2)
    parser.add_argument("--epochs", type=int, default=2500)
    parser.add_argument("--lr", type=float, default=0.05)
    parser.add_argument("--l2", type=float, default=0.001)
    parser.add_argument("--pos-weight", type=float, default=250.0)
    args = parser.parse_args()

    df = add_features(pd.read_csv(args.candidates), args.prob_scale)
    df["hit"] = df["hit"].astype(bool)
    if args.train_years or args.valid_years:
        train = df[df["valid_year"].isin(args.train_years or [])].copy()
        valid = df[df["valid_year"].isin(args.valid_years or [])].copy()
    else:
        train = df[df["sample"] == args.train_sample].copy()
        valid = df[df["sample"] == args.valid_sample].copy()
    if train.empty or valid.empty:
        raise SystemExit(f"Bad split: train={len(train)} valid={len(valid)}")

    train_x_raw = train[FEATURE_COLUMNS].to_numpy(dtype=float)
    valid_x_raw = valid[FEATURE_COLUMNS].to_numpy(dtype=float)
    train_x, valid_x, mean, scale = standardize(train_x_raw, valid_x_raw)
    y = train["hit"].to_numpy(dtype=float)

    w, b = fit_logistic(train_x, y, epochs=args.epochs, lr=args.lr, l2=args.l2, pos_weight=args.pos_weight)
    train["value_prob"] = predict(train_x, w, b)
    valid["value_prob"] = predict(valid_x, w, b)
    train["value_ev"] = train["value_prob"] * train["odds"] - 1.0
    valid["value_ev"] = valid["value_prob"] * valid["odds"] - 1.0

    strategy_rows = []
    for score_col in ["value_ev", "value_prob", "scaled_ev"]:
        for top_per_race in [1, 2, 3, 5]:
            for quantile in [0.75, 0.85, 0.9, 0.95, 0.98]:
                for min_odds, max_odds in [(1, 150), (20, 150), (50, 150), (20, 300), (50, 300), (50, 500)]:
                    result = evaluate_strategy(
                        valid,
                        score_col,
                        top_per_race=top_per_race,
                        min_score_quantile=quantile,
                        min_odds=min_odds,
                        max_odds=max_odds,
                    )
                    if result["bets"] < 50:
                        continue
                    result["score_col"] = score_col
                    strategy_rows.append(result)

    strategies = pd.DataFrame(strategy_rows).sort_values(["roi", "bets"], ascending=[False, False])
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    np.savez_compressed(
        out_dir / "candidate_value_model.npz",
        weights=w,
        bias=np.array([b]),
        mean=mean,
        scale=scale,
        feature_columns=np.array(FEATURE_COLUMNS, dtype=object),
        prob_scale=np.array([args.prob_scale]),
    )
    strategies.to_csv(out_dir / "strategy_search.csv", index=False, encoding="utf-8")
    valid.sort_values("value_ev", ascending=False).head(50000).to_csv(
        out_dir / "valid_scored_top.csv", index=False, encoding="utf-8"
    )

    metrics = {
        "train_rows": int(len(train)),
        "valid_rows": int(len(valid)),
        "train_hit_rate": float(train["hit"].mean()),
        "valid_hit_rate": float(valid["hit"].mean()),
        "pos_weight": args.pos_weight,
        "prob_scale": args.prob_scale,
    }
    (out_dir / "metrics.json").write_text(json.dumps(metrics, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(metrics, ensure_ascii=False, indent=2))
    print(strategies.head(30).to_string(index=False))
    print(f"out_dir={out_dir}")


if __name__ == "__main__":
    main()
