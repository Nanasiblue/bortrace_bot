from __future__ import annotations

import argparse
import json
import os
import time
import urllib.request
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import numpy as np

from bortrace.official_parser import parse_raceresult
from bortrace.paths import OUTPUT_DIR, WORK_DIR
from live_thread_predict import (
    COURSE_NAMES,
    JST,
    OfficialClient,
    RaceSchedule,
    build_live_row,
    format_countdown,
    level_bar,
    load_candidate_model,
    load_position_models,
    load_thread_predictions,
    pick_bets,
    result_dir_for,
    scheduled_races,
    score_candidates,
    upset_level,
)


PREDICTION_WEBHOOK_ENV = "DISCORD_PREDICTION_WEBHOOK_URL"
RESULTS_WEBHOOK_ENV = "DISCORD_RESULTS_WEBHOOK_URL"
PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OFFICIAL_MODELS_DIR = (
    OUTPUT_DIR / "official_models_final_2021_2025"
    if (OUTPUT_DIR / "official_models_final_2021_2025").exists()
    else PROJECT_ROOT / "models" / "official_models_final_2021_2025"
)
DEFAULT_CANDIDATE_MODEL = (
    OUTPUT_DIR / "candidate_lightgbm_2023_2025_valid_202607" / "lightgbm.pkl"
    if (OUTPUT_DIR / "candidate_lightgbm_2023_2025_valid_202607" / "lightgbm.pkl").exists()
    else PROJECT_ROOT / "models" / "candidate_lightgbm" / "lightgbm.pkl"
)


def post_webhook(webhook_url: str, payload: dict[str, Any]) -> None:
    data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(
        webhook_url,
        data=data,
        headers={"Content-Type": "application/json", "User-Agent": "Mozilla/5.0"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=20) as res:
        if res.status >= 400:
            raise RuntimeError(f"Discord webhook failed: HTTP {res.status}")


def fmt(value: Any, digits: int = 2) -> str:
    try:
        if value is None or (isinstance(value, float) and np.isnan(value)):
            return "-"
        return f"{float(value):.{digits}f}"
    except Exception:
        return "-"


def prediction_payload(race: RaceSchedule, candidates, picks, now: datetime) -> dict[str, Any]:
    pred_order = candidates["pred_order"].iloc[0] if "pred_order" in candidates.columns and not candidates.empty else ""
    winner_probs = candidates.attrs.get("winner_probs", {})
    upset_prob = float(candidates["upset_prob"].iloc[0]) if not candidates.empty else 0.0
    high_prob = float(candidates["high10000_prob"].iloc[0]) if not candidates.empty else 0.0
    level, level_name = upset_level(upset_prob, high_prob)
    source_row = candidates.attrs.get("source_row", {})

    ex_lines = []
    for boat in range(1, 7):
        ex_lines.append(
            f"{boat}号艇 展示 {fmt(source_row.get(f'ex_time_{boat}'))}"
            f" / 順位 {source_row.get(f'ex_time_rank_low_{boat}', '-')}"
            f" / 体重 {fmt(source_row.get(f'weight_{boat}'), 1)}"
            f" / チルト {fmt(source_row.get(f'tilt_{boat}'), 1)}"
            f" / ST {fmt(source_row.get(f'start_ex_st_by_course_{boat}'))}"
        )

    if picks.empty:
        pick_lines = ["見送り"]
    else:
        pick_lines = []
        for _, row in picks.iterrows():
            stake = int(row.get("stake_yen", 0))
            stake_text = f"{stake:,}円" if stake > 0 else "紙検証"
            pick_lines.append(
                f"{row['combination']} / odds {row['odds']:.1f} / scoreEV {row['model_ev']:.2f} "
                f"/ Kelly {row['used_kelly']:.3%} / {stake_text}"
            )

    prob_line = " / ".join(f"{boat}号艇 {prob:.1%}" for boat, prob in sorted(winner_probs.items()))
    description = (
        f"対象レース締切: {race.deadline}（あと {format_countdown(race.deadline_dt, now)}）\n"
        f"発走目安: {race.start_time}\n"
        f"荒れ判定: {level_bar(level)} Lv.{level} {level_name} / イン飛び {upset_prob:.1%} / 万舟 {high_prob:.1%}\n"
        f"AI予想順位: {pred_order}\n"
        f"1着確率: {prob_line}"
    )
    return {
        "content": f"競艇予想: {race.course} {race.rno}R / 締切 {race.deadline}",
        "embeds": [
            {
                "title": f"{race.course} {race.rno}R 予測",
                "url": race.url,
                "description": description[:4000],
                "color": 0xE67E22 if level >= 4 else 0x3498DB,
                "fields": [
                    {"name": "展示航走", "value": "\n".join(ex_lines)[:1024], "inline": False},
                    {"name": "AI推奨買い目", "value": "\n".join(pick_lines)[:1024], "inline": False},
                ],
                "footer": {"text": f"race_id={race.race_id}"},
                "timestamp": datetime.now(JST).isoformat(),
            }
        ],
    }


def make_prediction_record(race: RaceSchedule, candidates, picks) -> dict[str, Any]:
    upset_prob = float(candidates["upset_prob"].iloc[0]) if not candidates.empty else 0.0
    high_prob = float(candidates["high10000_prob"].iloc[0]) if not candidates.empty else 0.0
    level, level_name = upset_level(upset_prob, high_prob)
    source_row = candidates.attrs.get("source_row", {})
    return {
        "race": race.__dict__,
        "deadline": race.deadline,
        "pred_order": candidates.attrs.get("pred_order", ""),
        "winner_probs": candidates.attrs.get("winner_probs", {}),
        "upset_level": {
            "level": level,
            "name": level_name,
            "bar": level_bar(level),
            "upset_prob": upset_prob,
            "high10000_prob": high_prob,
        },
        "exhibition": {
            str(boat): {
                "ex_time": source_row.get(f"ex_time_{boat}"),
                "ex_rank": source_row.get(f"ex_time_rank_low_{boat}"),
                "weight": source_row.get(f"weight_{boat}"),
                "tilt": source_row.get(f"tilt_{boat}"),
                "start_ex_st": source_row.get(f"start_ex_st_by_course_{boat}"),
            }
            for boat in range(1, 7)
        },
        "picks": picks.to_dict(orient="records"),
        "top_candidates": candidates.head(10).to_dict(orient="records"),
        "predicted_at": datetime.now(JST).isoformat(),
        "discord_prediction_sent": True,
    }


def save_prediction(date_str: str, record: dict[str, Any]) -> None:
    out_dir = OUTPUT_DIR / "live_thread_predictions"
    out_dir.mkdir(parents=True, exist_ok=True)
    with (out_dir / f"predictions_{date_str}.jsonl").open("a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False, default=str) + "\n")


def predict_and_post(args: argparse.Namespace, client: OfficialClient, race: RaceSchedule, models, candidate_model) -> None:
    now = datetime.now(JST)
    race_dir = client.fetch_race_pages(race)
    row = build_live_row(race, race_dir)
    pos_models, binary_models = models
    candidates = score_candidates(
        race,
        row,
        race_dir,
        pos_models,
        binary_models,
        candidate_model,
        top_candidates=args.top_candidates,
        prob_scale=args.prob_scale,
        kelly_scale=args.kelly_scale,
        max_kelly_fraction=args.max_kelly_fraction,
        bankroll=args.bankroll,
        base_stake=args.base_stake,
    )
    picks = pick_bets(
        candidates,
        min_odds=args.min_odds,
        max_odds=args.max_odds,
        min_model_ev=args.min_model_ev,
        top_per_race=args.top_per_race,
    )
    save_prediction(race.date, make_prediction_record(race, candidates, picks))
    webhook = os.environ.get(PREDICTION_WEBHOOK_ENV) or os.environ.get("DISCORD_WEBHOOK_URL")
    if not webhook:
        print(f"{PREDICTION_WEBHOOK_ENV} is not set")
        return
    post_webhook(webhook, prediction_payload(race, candidates, picks, now))
    print(f"prediction sent: {race.course} {race.rno}R {race.deadline}")


def result_payload(pred: dict[str, Any], result: dict[str, Any]) -> dict[str, Any]:
    race_data = pred["race"]
    jcd = str(race_data["jcd"]).zfill(2)
    course = COURSE_NAMES.get(jcd, jcd)
    rno = int(race_data["rno"])
    finish_order = [int(result.get(f"target_pos{i}", -1)) + 1 for i in range(1, 7) if result.get(f"target_pos{i}") is not None]
    actual_order = "-".join(map(str, finish_order))
    actual_top3 = "-".join(map(str, finish_order[:3]))
    trifecta = str(result.get("trifecta", ""))
    payout = int(result.get("payout_3t") or 0)
    pred_order = pred.get("pred_order", "")
    pred_top3 = pred_order.split("-")[:3] if pred_order else []
    winner_hit = bool(pred_top3 and finish_order and pred_top3[0] == str(finish_order[0]))
    top3_exact = "-".join(pred_top3) == actual_top3 if pred_top3 else False
    hit_picks = [pick for pick in pred.get("picks", []) if str(pick.get("combination")) == trifecta]
    actual_upset = bool(finish_order and finish_order[0] != 1)
    level = pred.get("upset_level", {})
    pred_upset = int(level.get("level", 0) or 0) >= 4

    pick_status = "見送り"
    pick_lines = []
    if pred.get("picks"):
        pick_status = "的中" if hit_picks else "外れ"
        for pick in pred["picks"][:5]:
            mark = "的中 " if str(pick.get("combination")) == trifecta else ""
            pick_lines.append(f"{mark}{pick.get('combination')} / odds {float(pick.get('odds', 0)):.1f}")

    description = (
        f"対象レース締切: {pred.get('deadline', race_data.get('deadline', ''))}\n"
        f"結果: {actual_order}\n"
        f"3連単: {trifecta} / 払戻 {payout:,}円\n"
        f"AI予想順位: {pred_order}\n"
        f"順位判定: 1着 {'的中' if winner_hit else '外れ'} / 3連単順序 {'的中' if top3_exact else '外れ'}\n"
        f"荒れ判定: 予想Lv.{level.get('level')} {level.get('name')} / 実際 {'荒れ' if actual_upset else 'イン逃げ'} "
        f"/ {'一致' if pred_upset == actual_upset else '不一致'}\n"
        f"買い目: {pick_status}"
    )
    fields = []
    if pick_lines:
        fields.append({"name": "AI推奨買い目", "value": "\n".join(pick_lines)[:1024], "inline": False})
    return {
        "content": f"競艇成績: {course} {rno}R / {pick_status}",
        "embeds": [
            {
                "title": f"{course} {rno}R 結果",
                "description": description[:4000],
                "color": 0x2ECC71 if hit_picks else 0x95A5A6,
                "fields": fields,
                "footer": {"text": f"race_id={race_data.get('date')}_{jcd}_{rno}"},
                "timestamp": datetime.now(JST).isoformat(),
            }
        ],
    }


def update_results_and_post(args: argparse.Namespace, client: OfficialClient) -> None:
    date_str = args.date or datetime.now(JST).strftime("%Y%m%d")
    preds = load_thread_predictions(date_str)
    latest: dict[str, dict[str, Any]] = {}
    for pred in preds:
        race_data = pred.get("race", {})
        race_id = f"{race_data.get('date')}_{str(race_data.get('jcd')).zfill(2)}_{race_data.get('rno')}"
        latest[race_id] = pred
    webhook = os.environ.get(RESULTS_WEBHOOK_ENV)
    if not webhook:
        print(f"{RESULTS_WEBHOOK_ENV} is not set")
        return
    sent = 0
    for pred in latest.values():
        race_data = pred["race"]
        race = RaceSchedule(
            date=str(race_data["date"]),
            jcd=str(race_data["jcd"]).zfill(2),
            rno=int(race_data["rno"]),
            deadline=str(race_data.get("deadline") or pred.get("deadline") or "00:00"),
            start_time=str(race_data.get("start_time") or "00:00"),
            url=str(race_data.get("url") or ""),
        )
        if args.jcd and race.jcd != args.jcd.zfill(2):
            continue
        if args.rno and race.rno != args.rno:
            continue
        race_dir = result_dir_for(client, race)
        result = parse_raceresult(race_dir / "raceresult.html")
        if "winner" not in result:
            continue
        post_webhook(webhook, result_payload(pred, result))
        sent += 1
        time.sleep(args.sleep_sec)
    print(f"results sent: {sent}")


def run_targets(args: argparse.Namespace) -> None:
    date_str = args.date or datetime.now(JST).strftime("%Y%m%d")
    client = OfficialClient(
        sleep_sec=args.sleep_sec,
        cache_dir=Path(args.cache_dir),
        offline_root=Path(args.offline_root) if args.offline_root else None,
    )
    if args.update_results:
        update_results_and_post(args, client)
        return

    races = scheduled_races(args, client, date_str)
    now = datetime.now(JST)
    models = load_position_models(Path(args.official_models_dir))
    candidate_model = load_candidate_model(Path(args.candidate_model))
    targets = []
    for race in races:
        minutes = (race.deadline_dt - now).total_seconds() / 60.0
        if args.rno or args.force:
            targets.append(race)
        elif args.from_min <= minutes <= args.to_min:
            targets.append(race)
    for race in targets:
        try:
            predict_and_post(args, client, race, models, candidate_model)
        except Exception as exc:
            print(f"skip {race.course} {race.rno}R: {exc}")


def run_scheduled(args: argparse.Namespace) -> None:
    date_str = args.date or datetime.now(JST).strftime("%Y%m%d")
    client = OfficialClient(
        sleep_sec=args.sleep_sec,
        cache_dir=Path(args.cache_dir),
        offline_root=Path(args.offline_root) if args.offline_root else None,
    )
    races = scheduled_races(args, client, date_str)
    events: list[tuple[datetime, str, RaceSchedule]] = []
    for race in races:
        events.append((race.deadline_dt - timedelta(minutes=args.predict_before_deadline_min), "predict", race))
        events.append((race.deadline_dt + timedelta(minutes=args.result_after_deadline_min), "result", race))
    events.sort(key=lambda item: item[0])
    models = load_position_models(Path(args.official_models_dir))
    candidate_model = load_candidate_model(Path(args.candidate_model))
    for event_at, kind, race in events:
        now = datetime.now(JST)
        if args.skip_past and event_at < now:
            continue
        wait_sec = (event_at - now).total_seconds()
        if wait_sec > 0:
            print(f"sleep until {event_at.strftime('%H:%M:%S')} / {race.course} {race.rno}R {kind}")
            time.sleep(wait_sec)
        try:
            if kind == "predict":
                predict_and_post(args, client, race, models, candidate_model)
            else:
                update_args = argparse.Namespace(**vars(args))
                update_args.jcd = race.jcd
                update_args.rno = race.rno
                update_results_and_post(update_args, client)
        except Exception as exc:
            print(f"skip {kind} {race.course} {race.rno}R: {exc}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", default=None)
    parser.add_argument("--jcd", default=None)
    parser.add_argument("--rno", type=int, default=None)
    parser.add_argument("--from-min", type=float, default=5)
    parser.add_argument("--to-min", type=float, default=35)
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--schedule-run", action="store_true")
    parser.add_argument("--update-results", action="store_true")
    parser.add_argument("--predict-before-deadline-min", type=float, default=15)
    parser.add_argument("--result-after-deadline-min", type=float, default=7)
    parser.add_argument("--skip-past", action="store_true")
    parser.add_argument("--sleep-sec", type=float, default=0.5)
    parser.add_argument("--start-offset-min", type=int, default=2)
    parser.add_argument("--cache-dir", default=str(WORK_DIR / "live_pages"))
    parser.add_argument("--offline-root", default=None)
    parser.add_argument("--official-models-dir", default=str(DEFAULT_OFFICIAL_MODELS_DIR))
    parser.add_argument(
        "--candidate-model",
        default=str(DEFAULT_CANDIDATE_MODEL),
    )
    parser.add_argument("--top-candidates", type=int, default=10)
    parser.add_argument("--top-per-race", type=int, default=3)
    parser.add_argument("--min-odds", type=float, default=50)
    parser.add_argument("--max-odds", type=float, default=150)
    parser.add_argument("--min-model-ev", type=float, default=84.24)
    parser.add_argument("--prob-scale", type=float, default=0.2)
    parser.add_argument("--kelly-scale", type=float, default=0.25)
    parser.add_argument("--max-kelly-fraction", type=float, default=0.003)
    parser.add_argument("--bankroll", type=float, default=10000)
    parser.add_argument("--base-stake", type=int, default=100)
    args = parser.parse_args()
    if args.schedule_run:
        run_scheduled(args)
    else:
        run_targets(args)


if __name__ == "__main__":
    main()
