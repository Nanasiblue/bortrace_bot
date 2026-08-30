from __future__ import annotations

import argparse
import json
import pickle
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.ensemble import ExtraTreesClassifier, RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import average_precision_score, log_loss, roc_auc_score
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler

from train_candidate_value_model import FEATURE_COLUMNS, add_features, evaluate_strategy


def make_models(pos_weight: float, seed: int) -> dict[str, object]:
    return {
        "logistic_balanced": make_pipeline(
            StandardScaler(),
            LogisticRegression(class_weight="balanced", max_iter=2000, C=0.3, n_jobs=-1, random_state=seed),
        ),
        "random_forest": RandomForestClassifier(
            n_estimators=240,
            max_depth=8,
            min_samples_leaf=80,
            class_weight={False: 1.0, True: pos_weight},
            n_jobs=-1,
            random_state=seed,
        ),
        "extra_trees": ExtraTreesClassifier(
            n_estimators=300,
            max_depth=8,
            min_samples_leaf=80,
            class_weight={False: 1.0, True: pos_weight},
            n_jobs=-1,
            random_state=seed,
        ),
    }


def add_optional_models(models: dict[str, object], pos_weight: float, seed: int) -> None:
    try:
        import lightgbm as lgb

        models["lightgbm"] = lgb.LGBMClassifier(
            objective="binary",
            n_estimators=700,
            learning_rate=0.035,
            num_leaves=15,
            max_depth=5,
            min_child_samples=80,
            subsample=0.85,
            colsample_bytree=0.85,
            reg_alpha=0.1,
            reg_lambda=2.0,
            scale_pos_weight=pos_weight,
            random_state=seed,
            n_jobs=-1,
            verbosity=-1,
        )
    except Exception as exc:
        print(f"skip lightgbm: {exc}", flush=True)

    try:
        from xgboost import XGBClassifier

        models["xgboost"] = XGBClassifier(
            objective="binary:logistic",
            n_estimators=500,
            learning_rate=0.035,
            max_depth=4,
            min_child_weight=80,
            subsample=0.85,
            colsample_bytree=0.85,
            reg_alpha=0.1,
            reg_lambda=2.0,
            scale_pos_weight=pos_weight,
            tree_method="hist",
            random_state=seed,
            n_jobs=-1,
            eval_metric="logloss",
        )
    except Exception as exc:
        print(f"skip xgboost: {exc}", flush=True)

    try:
        from catboost import CatBoostClassifier

        models["catboost"] = CatBoostClassifier(
            iterations=700,
            learning_rate=0.035,
            depth=5,
            l2_leaf_reg=8,
            loss_function="Logloss",
            class_weights=[1.0, pos_weight],
            random_seed=seed,
            verbose=False,
            allow_writing_files=False,
            thread_count=-1,
        )
    except Exception as exc:
        print(f"skip catboost: {exc}", flush=True)


def evaluate_model(name: str, model: object, train: pd.DataFrame, valid: pd.DataFrame) -> tuple[dict[str, object], pd.DataFrame]:
    x_train = train[FEATURE_COLUMNS].to_numpy(dtype=float)
    y_train = train["hit"].astype(int).to_numpy()
    x_valid = valid[FEATURE_COLUMNS].to_numpy(dtype=float)
    y_valid = valid["hit"].astype(int).to_numpy()
    model.fit(x_train, y_train)
    prob = model.predict_proba(x_valid)[:, 1]
    scored = valid.copy()
    scored["model"] = name
    scored["model_prob"] = prob
    scored["model_ev"] = prob * scored["odds"] - 1.0

    metrics: dict[str, object] = {
        "model": name,
        "roc_auc": roc_auc_score(y_valid, prob) if len(np.unique(y_valid)) > 1 else 0.0,
        "avg_precision": average_precision_score(y_valid, prob) if len(np.unique(y_valid)) > 1 else 0.0,
        "log_loss": log_loss(y_valid, np.clip(prob, 1e-8, 1 - 1e-8)),
        "valid_hit_rate": float(y_valid.mean()),
    }
    strategy_rows = []
    for score_col in ["model_ev", "model_prob", "scaled_ev"]:
        for top_per_race in [1, 2, 3, 5]:
            for quantile in [0.75, 0.85, 0.9, 0.95, 0.98]:
                for min_odds, max_odds in [(1, 80), (1, 150), (20, 150), (50, 150), (20, 300), (50, 300), (50, 500)]:
                    result = evaluate_strategy(
                        scored,
                        score_col,
                        top_per_race=top_per_race,
                        min_score_quantile=quantile,
                        min_odds=min_odds,
                        max_odds=max_odds,
                    )
                    if result["bets"] < 50:
                        continue
                    result["model"] = name
                    result["score_col"] = score_col
                    strategy_rows.append(result)
    strategies = pd.DataFrame(strategy_rows)
    if not strategies.empty:
        best = strategies.sort_values(["roi", "bets"], ascending=[False, False]).iloc[0].to_dict()
        metrics["best_strategy"] = best
    return metrics, scored


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--candidates", required=True)
    parser.add_argument("--valid-candidates", default=None)
    parser.add_argument("--out-dir", required=True)
    parser.add_argument("--train-sample", default="head")
    parser.add_argument("--valid-sample", default="tail")
    parser.add_argument("--train-years", nargs="+", type=int, default=None)
    parser.add_argument("--valid-years", nargs="+", type=int, default=None)
    parser.add_argument("--prob-scale", type=float, default=0.2)
    parser.add_argument("--pos-weight", type=float, default=250.0)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--include-heavy", action="store_true")
    parser.add_argument("--only-models", default=None)
    args = parser.parse_args()

    train_source = add_features(pd.read_csv(args.candidates), args.prob_scale)
    train_source["hit"] = train_source["hit"].astype(bool)
    if args.valid_candidates:
        valid_source = add_features(pd.read_csv(args.valid_candidates), args.prob_scale)
        valid_source["hit"] = valid_source["hit"].astype(bool)
    else:
        valid_source = train_source
    if args.train_years or args.valid_years:
        train = train_source[train_source["valid_year"].isin(args.train_years or [])].copy()
        valid = valid_source[valid_source["valid_year"].isin(args.valid_years or [])].copy()
    else:
        train = train_source[train_source["sample"] == args.train_sample].copy()
        valid = valid_source[valid_source["sample"] == args.valid_sample].copy()
    if train.empty or valid.empty:
        raise SystemExit(f"Bad split: train={len(train)} valid={len(valid)}")

    models = make_models(args.pos_weight, args.seed)
    add_optional_models(models, args.pos_weight, args.seed)
    if not args.include_heavy:
        models = {key: value for key, value in models.items() if key not in {"catboost", "xgboost"}}
    if args.only_models:
        wanted = {name.strip() for name in args.only_models.split(",") if name.strip()}
        models = {key: value for key, value in models.items() if key in wanted}
        if not models:
            raise SystemExit(f"No matching models selected: {sorted(wanted)}")

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    metrics = []
    scored_parts = []
    for name, model in models.items():
        print(f"train {name}", flush=True)
        model_metrics, scored = evaluate_model(name, model, train, valid)
        metrics.append(model_metrics)
        scored_parts.append(scored.nlargest(50000, "model_ev"))
        with (out_dir / f"{name}.pkl").open("wb") as f:
            pickle.dump(model, f)

    rows = []
    for item in metrics:
        best = item.get("best_strategy", {})
        rows.append(
            {
                "model": item["model"],
                "roc_auc": item["roc_auc"],
                "avg_precision": item["avg_precision"],
                "log_loss": item["log_loss"],
                "best_roi": best.get("roi"),
                "best_bets": best.get("bets"),
                "best_hits": best.get("hits"),
                "best_profit": best.get("profit"),
                "best_score_col": best.get("score_col"),
                "best_top_per_race": best.get("top_per_race"),
                "best_quantile": best.get("min_score_quantile"),
                "best_min_odds": best.get("min_odds"),
                "best_max_odds": best.get("max_odds"),
            }
        )
    summary = pd.DataFrame(rows).sort_values(["best_roi", "avg_precision"], ascending=[False, False])
    summary.to_csv(out_dir / "model_compare_summary.csv", index=False, encoding="utf-8")
    pd.concat(scored_parts, ignore_index=True).to_csv(out_dir / "valid_scored_top.csv", index=False, encoding="utf-8")
    (out_dir / "metrics.json").write_text(json.dumps(metrics, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    print(summary.to_string(index=False))
    print(f"out_dir={out_dir}")


if __name__ == "__main__":
    main()
