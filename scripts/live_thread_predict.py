from __future__ import annotations

import argparse
import json
import pickle
import re
import sys
import time
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from functools import lru_cache
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from lxml import html

try:
    import requests
except ModuleNotFoundError:
    requests = None

from bortrace.baseline_model import BinaryLogisticRegression, SoftmaxRegression
from bortrace.official_parser import add_engineered_features, parse_beforeinfo, parse_raceresult, parse_racelist
from bortrace.kibetsu import enrich_race
from bortrace.ranking_order_model import RaceRankingModel
from bortrace.paths import OUTPUT_DIR, WORK_DIR
from build_odds3t_dataset import parse_odds3t
from predict_trifecta_ev import combo_probabilities, kelly_fraction
from train_candidate_value_model import FEATURE_COLUMNS as CANDIDATE_FEATURE_COLUMNS
from train_candidate_value_model import add_features as add_candidate_features
from train_official_models import greedy_order_from_position_probs


JST = timezone(timedelta(hours=9), "JST")
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8")
BASE_URL = "https://www.boatrace.jp/owpc/pc/race"
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
DEFAULT_RANKING_MODEL = PROJECT_ROOT / "models" / "ranking_order_kibetsu" / "ranking_model_selected.pkl"
DEFAULT_KIBETSU_DIR = PROJECT_ROOT / "models" / "kibetsu"
DEFAULT_UPSET_MODEL = PROJECT_ROOT / "models" / "upset_model" / "upset_model.pkl"
COURSE_NAMES = {
    "01": "桐生",
    "02": "戸田",
    "03": "江戸川",
    "04": "平和島",
    "05": "多摩川",
    "06": "浜名湖",
    "07": "蒲郡",
    "08": "常滑",
    "09": "津",
    "10": "三国",
    "11": "びわこ",
    "12": "住之江",
    "13": "尼崎",
    "14": "鳴門",
    "15": "丸亀",
    "16": "児島",
    "17": "宮島",
    "18": "徳山",
    "19": "下関",
    "20": "若松",
    "21": "芦屋",
    "22": "福岡",
    "23": "唐津",
    "24": "大村",
}


@dataclass(frozen=True)
class RaceSchedule:
    date: str
    jcd: str
    rno: int
    deadline: str
    start_time: str
    url: str

    @property
    def course(self) -> str:
        return COURSE_NAMES.get(self.jcd, self.jcd)

    @property
    def race_id(self) -> str:
        return f"{self.date}_{self.jcd}_{self.rno}"

    @property
    def deadline_dt(self) -> datetime:
        return datetime.strptime(f"{self.date} {self.deadline}", "%Y%m%d %H:%M").replace(tzinfo=JST)

    @property
    def start_dt(self) -> datetime:
        return datetime.strptime(f"{self.date} {self.start_time}", "%Y%m%d %H:%M").replace(tzinfo=JST)


def text_of(node: Any) -> str:
    return " ".join(" ".join(node.xpath(".//text()")).split())


def format_countdown(target: datetime, now: datetime) -> str:
    seconds = int((target - now).total_seconds())
    sign = "" if seconds >= 0 else "-"
    seconds = abs(seconds)
    minutes, sec = divmod(seconds, 60)
    hours, minutes = divmod(minutes, 60)
    if hours:
        return f"{sign}{hours}時間{minutes:02d}分"
    return f"{sign}{minutes}分{sec:02d}秒"


def to_int_money(value: str | None) -> int | None:
    if not value:
        return None
    match = re.search(r"([\d,]+)", value)
    if not match:
        return None
    return int(match.group(1).replace(",", ""))


def upset_level(upset_prob: float, high10000_prob: float) -> tuple[int, str]:
    score = 0.75 * float(upset_prob) + 0.25 * min(float(high10000_prob) / 0.35, 1.0)
    if score >= 0.72:
        return 5, "大荒れ警戒"
    if score >= 0.58:
        return 4, "荒れ寄り"
    if score >= 0.44:
        return 3, "中間"
    if score >= 0.30:
        return 2, "堅め"
    return 1, "かなり堅め"


def level_bar(level: int) -> str:
    return "■" * level + "□" * (5 - level)


def safe_float(value: Any) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    if not np.isfinite(result):
        return None
    return result


def fmt_pct(value: float | None) -> str:
    return "-" if value is None else f"{value:.1%}"


def fmt_num(value: float | None, digits: int = 2) -> str:
    return "-" if value is None else f"{value:.{digits}f}"


def race_explanation(source_row: dict[str, Any], winner_probs: dict[str, float], upset_prob: float) -> list[str]:
    if not winner_probs:
        return []
    probs = {int(boat): float(prob) for boat, prob in winner_probs.items()}
    pred_winner = max(probs, key=probs.get)
    top_challenger = max((boat for boat in range(2, 7)), key=lambda boat: probs.get(boat, 0.0))
    boat1_prob = probs.get(1, 0.0)
    challenger_prob = probs.get(top_challenger, 0.0)
    margin = boat1_prob - challenger_prob

    lines = [
        f"1号艇1着 {fmt_pct(boat1_prob)} / 最有力対抗 {top_challenger}号艇 {fmt_pct(challenger_prob)} / 差 {margin:+.1%}",
    ]
    if pred_winner != 1:
        lines.append(f"着順モデルは {pred_winner}号艇の1着を最上位評価")
    elif margin < 0.08:
        lines.append("1号艇は最上位だが、対抗との差が小さい")
    elif boat1_prob >= 0.62 and upset_prob < 0.42:
        lines.append("1号艇優勢で、荒れ確率も低め")

    boat1_ex_rank = safe_float(source_row.get("ex_time_rank_low_1"))
    boat1_ex = safe_float(source_row.get("ex_time_1"))
    best_ex_boat = None
    best_ex_time = None
    best_ex_rank = None
    ex_candidates = []
    for boat in range(1, 7):
        rank = safe_float(source_row.get(f"ex_time_rank_low_{boat}"))
        ex_time = safe_float(source_row.get(f"ex_time_{boat}"))
        if rank is not None and ex_time is not None:
            ex_candidates.append((rank, ex_time, boat))
    if ex_candidates:
        best_ex_rank, best_ex_time, best_ex_boat = min(ex_candidates)
        if best_ex_boat != 1 and (boat1_ex_rank is None or boat1_ex_rank >= 3):
            lines.append(
                f"展示は{best_ex_boat}号艇が上位（{fmt_num(best_ex_time)}）、1号艇は{fmt_num(boat1_ex)}で{fmt_num(boat1_ex_rank, 0)}位"
            )

    for metric, label, higher_is_better in [
        ("national_win_rate", "全国勝率", True),
        ("local_win_rate", "当地勝率", True),
        ("motor_quinella_rate", "モーター2連率", True),
        ("avg_st", "平均ST", False),
    ]:
        boat1 = safe_float(source_row.get(f"{metric}_1"))
        challenger = safe_float(source_row.get(f"{metric}_{top_challenger}"))
        if boat1 is None or challenger is None:
            continue
        if higher_is_better:
            diff = boat1 - challenger
            if diff <= -0.25:
                lines.append(f"{label}は{top_challenger}号艇が優勢（1号艇 {fmt_num(boat1)} / {top_challenger}号艇 {fmt_num(challenger)}）")
        else:
            diff = boat1 - challenger
            if diff >= 0.03:
                lines.append(f"{label}は{top_challenger}号艇が先行気味（1号艇 {fmt_num(boat1)} / {top_challenger}号艇 {fmt_num(challenger)}）")
    return lines[:4]


def parse_hhmm(text: str) -> str | None:
    match = re.search(r"(\d{1,2}):(\d{2})", text)
    if not match:
        return None
    return f"{int(match.group(1)):02d}:{match.group(2)}"


def estimate_start_time(deadline: str, offset_min: int) -> str:
    base = datetime.strptime(deadline, "%H:%M")
    return (base + timedelta(minutes=offset_min)).strftime("%H:%M")


class OfficialClient:
    def __init__(self, *, sleep_sec: float = 0.3, cache_dir: Path | None = None, offline_root: Path | None = None) -> None:
        self.sleep_sec = sleep_sec
        self.cache_dir = cache_dir or (WORK_DIR / "live_pages")
        self.offline_root = offline_root
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
            "Accept-Language": "ja,en-US;q=0.9,en;q=0.8",
        }
        self.session = requests.Session() if requests else None
        if self.session:
            self.session.headers.update(self.headers)

    def fetch_bytes(self, url: str, referer: str | None = None) -> bytes:
        if self.sleep_sec:
            time.sleep(self.sleep_sec)
        if self.session:
            headers = {"Referer": referer} if referer else None
            res = self.session.get(url, headers=headers, timeout=20)
            res.raise_for_status()
            return res.content
        headers = dict(self.headers)
        if referer:
            headers["Referer"] = referer
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req, timeout=20) as res:
            return res.read()

    def doc_from_url(self, url: str, referer: str | None = None):
        return html.fromstring(self.fetch_bytes(url, referer=referer))

    def fetch_schedules(self, date_str: str, *, start_offset_min: int = 2) -> list[RaceSchedule]:
        if self.offline_root and (self.offline_root / date_str).exists():
            return self.fetch_offline_schedules(date_str, start_offset_min=start_offset_min)
        index_url = f"{BASE_URL}/index?hd={date_str}"
        index_doc = self.doc_from_url(index_url)
        active_jcds = sorted(
            {
                match.group(1)
                for href in index_doc.xpath("//a/@href")
                for match in [re.search(r"jcd=(\d{2})", href)]
                if match and match.group(1) in COURSE_NAMES
            }
        )
        schedules: dict[tuple[str, int], RaceSchedule] = {}
        for jcd in active_jcds:
            raceindex_url = f"{BASE_URL}/raceindex?jcd={jcd}&hd={date_str}"
            doc = self.doc_from_url(raceindex_url, referer=index_url)
            for link in doc.xpath("//a[contains(@href, 'racelist') and contains(@href, 'rno=')]"):
                href = link.get("href") or ""
                rno_match = re.search(r"rno=(\d{1,2})", href)
                if not rno_match:
                    continue
                rno = int(rno_match.group(1))
                row = link.xpath("ancestor::tr[1]")
                row_text = text_of(row[0]) if row else text_of(link)
                deadline = parse_hhmm(row_text)
                if not deadline:
                    continue
                url = "https://www.boatrace.jp" + href if href.startswith("/") else href
                schedules[(jcd, rno)] = RaceSchedule(
                    date=date_str,
                    jcd=jcd,
                    rno=rno,
                    deadline=deadline,
                    start_time=estimate_start_time(deadline, start_offset_min),
                    url=url,
                )
        return sorted(schedules.values(), key=lambda item: (item.deadline, item.jcd, item.rno))

    def fetch_course_schedules(self, date_str: str, jcd: str, *, start_offset_min: int = 2) -> list[RaceSchedule]:
        if self.offline_root and (self.offline_root / date_str).exists():
            return [race for race in self.fetch_offline_schedules(date_str, start_offset_min=start_offset_min) if race.jcd == jcd]
        raceindex_url = f"{BASE_URL}/raceindex?jcd={jcd}&hd={date_str}"
        doc = self.doc_from_url(raceindex_url, referer=f"{BASE_URL}/index?hd={date_str}")
        schedules: dict[int, RaceSchedule] = {}
        for link in doc.xpath("//a[contains(@href, 'racelist') and contains(@href, 'rno=')]"):
            href = link.get("href") or ""
            rno_match = re.search(r"rno=(\d{1,2})", href)
            if not rno_match:
                continue
            rno = int(rno_match.group(1))
            row = link.xpath("ancestor::tr[1]")
            row_text = text_of(row[0]) if row else text_of(link)
            deadline = parse_hhmm(row_text)
            if not deadline:
                continue
            url = "https://www.boatrace.jp" + href if href.startswith("/") else href
            schedules[rno] = RaceSchedule(
                date=date_str,
                jcd=jcd,
                rno=rno,
                deadline=deadline,
                start_time=estimate_start_time(deadline, start_offset_min),
                url=url,
            )
        return sorted(schedules.values(), key=lambda item: item.rno)

    def fetch_offline_schedules(self, date_str: str, *, start_offset_min: int = 2) -> list[RaceSchedule]:
        schedules: dict[tuple[str, int], RaceSchedule] = {}
        date_root = self.offline_root / date_str if self.offline_root else None
        if date_root is None:
            return []
        for raceindex in sorted(date_root.glob("*/raceindex.html")):
            jcd = raceindex.parent.name
            if jcd not in COURSE_NAMES:
                continue
            doc = html.fromstring(raceindex.read_bytes())
            for link in doc.xpath("//a[contains(@href, 'racelist') and contains(@href, 'rno=')]"):
                href = link.get("href") or ""
                rno_match = re.search(r"rno=(\d{1,2})", href)
                if not rno_match:
                    continue
                rno = int(rno_match.group(1))
                row = link.xpath("ancestor::tr[1]")
                row_text = text_of(row[0]) if row else text_of(link)
                deadline = parse_hhmm(row_text)
                if not deadline:
                    continue
                url = "https://www.boatrace.jp" + href if href.startswith("/") else href
                schedules[(jcd, rno)] = RaceSchedule(
                    date=date_str,
                    jcd=jcd,
                    rno=rno,
                    deadline=deadline,
                    start_time=estimate_start_time(deadline, start_offset_min),
                    url=url,
                )
        return sorted(schedules.values(), key=lambda item: (item.deadline, item.jcd, item.rno))

    def live_race_dir(self, race: RaceSchedule) -> Path:
        return self.cache_dir / race.date / race.jcd / str(race.rno)

    def offline_race_dir(self, race: RaceSchedule) -> Path | None:
        if not self.offline_root:
            return None
        path = self.offline_root / race.date / race.jcd / str(race.rno)
        return path if path.exists() else None

    def fetch_race_pages(self, race: RaceSchedule) -> Path:
        offline = self.offline_race_dir(race)
        if offline:
            return offline
        race_dir = self.live_race_dir(race)
        race_dir.mkdir(parents=True, exist_ok=True)
        pages = {
            "racelist": f"{BASE_URL}/racelist?rno={race.rno}&jcd={race.jcd}&hd={race.date}",
            "beforeinfo": f"{BASE_URL}/beforeinfo?rno={race.rno}&jcd={race.jcd}&hd={race.date}",
            "odds3t": f"{BASE_URL}/odds3t?rno={race.rno}&jcd={race.jcd}&hd={race.date}",
        }
        referer = f"{BASE_URL}/raceindex?jcd={race.jcd}&hd={race.date}"
        for name, url in pages.items():
            tmp = race_dir / f"{name}.tmp"
            out = race_dir / f"{name}.html"
            tmp.write_bytes(self.fetch_bytes(url, referer=referer))
            tmp.replace(out)
            referer = url
        return race_dir


def load_position_models(models_dir: Path) -> tuple[dict[str, SoftmaxRegression], dict[str, BinaryLogisticRegression]]:
    pos_models = {f"pos{pos}": SoftmaxRegression.load(models_dir / f"finish_pos{pos}_softmax.npz") for pos in range(1, 7)}
    binary_models: dict[str, BinaryLogisticRegression] = {}
    for name in ["upset", "is_high_payout_10000"]:
        path = models_dir / f"{name}_logistic.npz"
        if path.exists():
            binary_models[name] = BinaryLogisticRegression.load(path)
    return pos_models, binary_models


def load_candidate_model(path: Path) -> Any:
    with path.open("rb") as f:
        return pickle.load(f)


@lru_cache(maxsize=1)
def load_ranking_model(path: str = str(DEFAULT_RANKING_MODEL)) -> RaceRankingModel | None:
    model_path = Path(path)
    if not model_path.exists():
        return None
    with model_path.open("rb") as f:
        model = pickle.load(f)
    if not isinstance(model, RaceRankingModel):
        raise TypeError(f"Unexpected ranking model: {type(model)!r}")
    return model


def load_pickle_model(path: Path) -> Any:
    with path.open("rb") as f:
        return pickle.load(f)


def candidate_model_is_calibrated(candidate_model: Any) -> bool:
    return bool(getattr(candidate_model, "is_calibrated_probability_model", False))


def build_live_row(race: RaceSchedule, race_dir: Path) -> pd.DataFrame:
    row: dict[str, Any] = {
        "date": int(race.date),
        "year": int(race.date[:4]),
        "month": int(race.date[4:6]),
        "jcd": int(race.jcd),
        "rno": race.rno,
    }
    row.update(parse_racelist(race_dir / "racelist.html", rno=race.rno))
    before = parse_beforeinfo(race_dir / "beforeinfo.html")
    row.update(before)
    enrich_race(row, race.date, root=str(DEFAULT_KIBETSU_DIR))
    missing = [f"ex_time_{boat}" for boat in range(1, 7) if row.get(f"ex_time_{boat}") is None]
    if missing:
        raise RuntimeError(f"展示航走データ未取得: {', '.join(missing)}")
    row = add_engineered_features(row)
    return pd.DataFrame([row])


def ensure_model_features(frame: pd.DataFrame, feature_columns: list[str]) -> pd.DataFrame:
    out = frame.copy()
    for col in feature_columns:
        if col not in out.columns:
            out[col] = np.nan
    return out[feature_columns]


def score_candidates(
    race: RaceSchedule,
    row: pd.DataFrame,
    race_dir: Path,
    pos_models: dict[str, SoftmaxRegression],
    binary_models: dict[str, BinaryLogisticRegression],
    candidate_model: Any,
    upset_model: Any | None = None,
    *,
    top_candidates: int,
    prob_scale: float,
    kelly_scale: float,
    max_kelly_fraction: float,
    bankroll: float,
    base_stake: int,
) -> pd.DataFrame:
    model_frame = row.copy()
    probs = {
        name: model.predict_proba(ensure_model_features(model_frame, model.feature_columns))[0]
        for name, model in pos_models.items()
    }
    order = greedy_order_from_position_probs({name: values.reshape(1, -1) for name, values in probs.items()})[0] + 1
    order_model = "position_softmax"
    ranking_model = load_ranking_model()
    if ranking_model is not None:
        try:
            order = ranking_model.predict_order(model_frame)[0]
            order_model = ranking_model.label
        except Exception as exc:
            print(f"ranking model fallback: {exc}", flush=True)
    pred_order = "-".join(str(int(boat)) for boat in order)
    binary = {
        name: float(model.predict_proba(ensure_model_features(model_frame, model.feature_columns))[0])
        for name, model in binary_models.items()
    }
    if upset_model is not None:
        binary["upset"] = float(upset_model.predict_proba(model_frame)[0])
    odds_rows = parse_odds3t(race_dir / "odds3t.html")
    if not odds_rows:
        raise RuntimeError("三連単オッズが未取得")
    odds_by_combo = {str(item["combination"]): float(item["odds"]) for item in odds_rows}

    rows: list[dict[str, Any]] = []
    combos = []
    for combo, first, second, third, raw_prob in combo_probabilities(probs["pos1"], probs["pos2"], probs["pos3"]):
        odds = odds_by_combo.get(combo)
        if odds is None:
            continue
        combos.append((raw_prob * odds, raw_prob, odds, combo, first, second, third))
    combos.sort(reverse=True)
    for rank, (_, raw_prob, odds, combo, first, second, third) in enumerate(combos[:top_candidates], start=1):
        kelly_prob = raw_prob * prob_scale
        full_kelly = kelly_fraction(kelly_prob, odds)
        used_kelly = min(max_kelly_fraction, full_kelly * kelly_scale)
        kelly_stake = int((bankroll * used_kelly) // 100 * 100) if bankroll > 0 else 0
        stake = max(base_stake, kelly_stake) if used_kelly > 0 else 0
        rows.append(
            {
                "valid_year": int(race.date[:4]),
                "sample": "live",
                "date": race.date,
                "jcd": int(race.jcd),
                "rno": race.rno,
                "candidate_rank": rank,
                "combination": combo,
                "first": first,
                "second": second,
                "third": third,
                "raw_prob": raw_prob,
                "odds": odds,
                "actual": "",
                "hit": False,
                "payout_3t": 0,
                "upset_prob": binary.get("upset", 0.0),
                "high10000_prob": binary.get("is_high_payout_10000", 0.0),
                "kelly_prob": kelly_prob,
                "full_kelly": full_kelly,
                "used_kelly": used_kelly,
                "stake_yen": stake,
                "pred_order": pred_order,
            }
        )
    candidates = add_candidate_features(pd.DataFrame(rows), prob_scale)
    model_x = candidates[CANDIDATE_FEATURE_COLUMNS].to_numpy(dtype=float)
    candidates["model_prob"] = candidate_model.predict_proba(model_x)[:, 1]
    candidates["model_ev"] = candidates["model_prob"] * candidates["odds"] - 1.0
    if candidate_model_is_calibrated(candidate_model):
        candidates["kelly_prob"] = candidates["model_prob"].clip(lower=0.0, upper=1.0)
        candidates["full_kelly"] = [
            kelly_fraction(float(prob), float(odds))
            for prob, odds in zip(candidates["kelly_prob"], candidates["odds"])
        ]
        candidates["used_kelly"] = (candidates["full_kelly"] * kelly_scale).clip(lower=0.0, upper=max_kelly_fraction)
        candidates["stake_yen"] = [
            max(base_stake, int((bankroll * used) // 100 * 100)) if used > 0 else 0
            for used in candidates["used_kelly"]
        ]
    sorted_candidates = candidates.sort_values(["model_ev", "raw_ev"], ascending=[False, False]).reset_index(drop=True)
    sorted_candidates.attrs["pred_order"] = pred_order
    sorted_candidates.attrs["order_model"] = order_model
    sorted_candidates.attrs["winner_probs"] = {str(i + 1): float(prob) for i, prob in enumerate(probs["pos1"])}
    sorted_candidates.attrs["source_row"] = row.iloc[0].to_dict()
    sorted_candidates.attrs["explanation"] = race_explanation(
        sorted_candidates.attrs["source_row"],
        sorted_candidates.attrs["winner_probs"],
        binary.get("upset", 0.0),
    )
    return sorted_candidates


def pick_bets(
    candidates: pd.DataFrame,
    *,
    min_odds: float,
    max_odds: float,
    min_model_ev: float,
    top_per_race: int,
) -> pd.DataFrame:
    picked = candidates[
        (candidates["odds"] >= min_odds)
        & (candidates["odds"] <= max_odds)
        & (candidates["model_ev"] >= min_model_ev)
        & (candidates["stake_yen"] > 0)
    ].copy()
    return picked.head(top_per_race)


def render_schedule(races: list[RaceSchedule], now: datetime) -> str:
    lines = ["# 本日の開催スケジュール", ""]
    if not races:
        return "# 本日の開催スケジュール\n\n開催レースが見つかりません。"
    courses = sorted({race.jcd for race in races})
    lines.append("開催場: " + " / ".join(COURSE_NAMES.get(jcd, jcd) for jcd in courses))
    for jcd in courses:
        course_races = [race for race in races if race.jcd == jcd]
        if not course_races:
            continue
        lines.append("")
        lines.append(f"## {COURSE_NAMES.get(jcd, jcd)}")
        for race in course_races:
            lines.append(
            f"- {race.rno}R 締切 {race.deadline} / 発走目安 {race.start_time} / 締切まで {format_countdown(race.deadline_dt, now)}"
            )
    return "\n".join(lines)


def render_prediction(race: RaceSchedule, candidates: pd.DataFrame, picks: pd.DataFrame, now: datetime) -> str:
    pred_order = candidates["pred_order"].iloc[0] if "pred_order" in candidates.columns and not candidates.empty else ""
    order_model = candidates.attrs.get("order_model", "position_softmax")
    winner_probs = candidates.attrs.get("winner_probs", {})
    winner_line = " / ".join(f"{boat}号艇 {prob:.1%}" for boat, prob in sorted(winner_probs.items()))
    upset_prob = float(candidates["upset_prob"].iloc[0]) if "upset_prob" in candidates.columns and not candidates.empty else 0.0
    high_prob = float(candidates["high10000_prob"].iloc[0]) if "high10000_prob" in candidates.columns and not candidates.empty else 0.0
    level, level_name = upset_level(upset_prob, high_prob)
    source_row = candidates.attrs.get("source_row", {})
    explanation = candidates.attrs.get("explanation", [])
    lines = [
        f"## {race.course} {race.rno}R",
        f"- 締切: {race.deadline}（あと {format_countdown(race.deadline_dt, now)}）",
        f"- 発走目安: {race.start_time}",
        f"- 荒れやすさ: {level_bar(level)} Lv.{level} {level_name} / 1号艇以外の1着 {upset_prob:.1%} / 万舟級 {high_prob:.1%}",
        f"- 予想順位: {pred_order}",
        f"- 順位モデル: {order_model}",
        f"- 1着確率: {winner_line}",
        "",
        "読み筋:",
    ]
    if explanation:
        for line in explanation:
            lines.append(f"- {line}")
    else:
        lines.append("- 説明材料なし")
    lines.extend([
        "展示航走:",
        "| 艇 | 展示 | 展示順位 | 体重 | チルト | ST展示 |",
        "|---:|---:|---:|---:|---:|---:|",
    ])
    for boat in range(1, 7):
        lines.append(
            f"| {boat} | {source_row.get(f'ex_time_{boat}', np.nan):.2f} | "
            f"{source_row.get(f'ex_time_rank_low_{boat}', '')} | "
            f"{source_row.get(f'weight_{boat}', np.nan):.1f} | "
            f"{source_row.get(f'tilt_{boat}', np.nan):.1f} | "
            f"{source_row.get(f'start_ex_st_by_course_{boat}', np.nan):.2f} |"
        )
    lines.append("")
    if picks.empty:
        lines.append("買い目提案: 見送り")
        lines.append("")
        lines.append("上位候補:")
    else:
        lines.append("買い目提案:")
        for _, row in picks.iterrows():
            stake = int(row["stake_yen"])
            stake_text = f"購入候補 {stake:,}円" if stake > 0 else "参考候補（購入なし）"
            lines.append(
                f"- {row['combination']} / オッズ {row['odds']:.1f} / AI的中確率 {row['model_prob']:.3%} "
                f"/ 期待値 {row['model_ev']:.2f} / Kelly {row['used_kelly']:.3%} / {stake_text}"
            )
        lines.append("")
        lines.append("参考上位:")
    for _, row in candidates.head(5).iterrows():
        lines.append(
            f"- {row['combination']} / オッズ {row['odds']:.1f} / AI的中確率 {row['model_prob']:.3%} / "
            f"期待値 {row['model_ev']:.2f} / Kelly {row['used_kelly']:.3%}"
        )
    return "\n".join(lines)


def save_prediction(date_str: str, record: dict[str, Any]) -> None:
    out_dir = OUTPUT_DIR / "live_thread_predictions"
    out_dir.mkdir(parents=True, exist_ok=True)
    with (out_dir / f"predictions_{date_str}.jsonl").open("a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False, default=str) + "\n")


def predict_and_save_race(
    args: argparse.Namespace,
    date_str: str,
    now: datetime,
    client: OfficialClient,
    race: RaceSchedule,
    pos_models: dict[str, SoftmaxRegression],
    binary_models: dict[str, BinaryLogisticRegression],
    candidate_model: Any,
    upset_model: Any | None = None,
) -> str:
    race_dir = client.fetch_race_pages(race)
    row = build_live_row(race, race_dir)
    candidates = score_candidates(
        race,
        row,
        race_dir,
        pos_models,
        binary_models,
        candidate_model,
        upset_model,
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
    upset_prob = float(candidates["upset_prob"].iloc[0]) if not candidates.empty else 0.0
    high_prob = float(candidates["high10000_prob"].iloc[0]) if not candidates.empty else 0.0
    level, level_name = upset_level(upset_prob, high_prob)
    source_row = candidates.attrs.get("source_row", {})
    save_prediction(
        date_str,
        {
            "race": race.__dict__,
            "deadline": race.deadline,
        "pred_order": candidates.attrs.get("pred_order", ""),
        "order_model": candidates.attrs.get("order_model", "position_softmax"),
            "winner_probs": candidates.attrs.get("winner_probs", {}),
            "explanation": candidates.attrs.get("explanation", []),
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
        },
    )
    return render_prediction(race, candidates, picks, now)


def load_thread_predictions(date_str: str) -> list[dict[str, Any]]:
    path = OUTPUT_DIR / "live_thread_predictions" / f"predictions_{date_str}.jsonl"
    if not path.exists():
        return []
    rows = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.strip():
            rows.append(json.loads(line))
    return rows


def result_dir_for(client: OfficialClient, race: RaceSchedule) -> Path:
    offline = client.offline_race_dir(race)
    if offline:
        return offline
    race_dir = client.live_race_dir(race)
    race_dir.mkdir(parents=True, exist_ok=True)
    url = f"{BASE_URL}/raceresult?rno={race.rno}&jcd={race.jcd}&hd={race.date}"
    tmp = race_dir / "raceresult.tmp"
    out = race_dir / "raceresult.html"
    tmp.write_bytes(client.fetch_bytes(url, referer=race.url))
    tmp.replace(out)
    return race_dir


def render_result_update(pred: dict[str, Any], result: dict[str, Any]) -> str:
    race_data = pred["race"]
    course = COURSE_NAMES.get(str(race_data["jcd"]).zfill(2), str(race_data["jcd"]))
    rno = int(race_data["rno"])
    deadline = race_data.get("deadline", "")
    pred_order = pred.get("pred_order", "")
    finish_order = [int(result.get(f"target_pos{i}", -1)) + 1 for i in range(1, 7) if result.get(f"target_pos{i}") is not None]
    actual_order = "-".join(map(str, finish_order))
    actual_top3 = "-".join(map(str, finish_order[:3]))
    trifecta = str(result.get("trifecta", ""))
    payout = result.get("payout_3t")
    picks = pred.get("picks", [])
    hit_picks = [pick for pick in picks if str(pick.get("combination")) == trifecta]
    pred_top3 = pred_order.split("-")[:3] if pred_order else []
    top3_exact = "-".join(pred_top3) == actual_top3 if pred_top3 else False
    winner_hit = bool(pred_top3 and pred_top3[0] == str(finish_order[0])) if finish_order else False
    actual_upset = bool(finish_order and finish_order[0] != 1)
    pred_upset_level = pred.get("upset_level", {})
    pred_upset = int(pred_upset_level.get("level", 0)) >= 4
    lines = [
        f"## 結果更新: {course} {rno}R",
        f"- 締切: {deadline}",
        f"- 結果: {actual_order}",
        f"- 3連単: {trifecta} / 払戻 {int(payout or 0):,}円",
        f"- AI予想順位: {pred_order}",
        f"- 順位判定: 1着 {'的中' if winner_hit else '外れ'} / 3連単順序 {'的中' if top3_exact else '外れ'}",
        f"- 荒れ判定: 予想Lv.{pred_upset_level.get('level')} {pred_upset_level.get('name')} / 実際 {'荒れ' if actual_upset else 'イン逃げ'} / {'一致' if pred_upset == actual_upset else '不一致'}",
    ]
    if not picks:
        lines.append("- 買い目: 見送り")
    elif hit_picks:
        lines.append("- 買い目: 的中")
        for pick in hit_picks:
            lines.append(f"  - {pick.get('combination')} / オッズ {float(pick.get('odds', 0)):.1f}")
    else:
        lines.append("- 買い目: 外れ")
        for pick in picks[:5]:
            lines.append(f"  - {pick.get('combination')} / オッズ {float(pick.get('odds', 0)):.1f}")
    return "\n".join(lines)


def run_once(args: argparse.Namespace) -> None:
    date_str = args.date or datetime.now(JST).strftime("%Y%m%d")
    now = datetime.now(JST)
    client = OfficialClient(
        sleep_sec=args.sleep_sec,
        cache_dir=Path(args.cache_dir),
        offline_root=Path(args.offline_root) if args.offline_root else None,
    )
    if args.jcd:
        races = client.fetch_course_schedules(date_str, args.jcd.zfill(2), start_offset_min=args.start_offset_min)
    else:
        races = client.fetch_schedules(date_str, start_offset_min=args.start_offset_min)
    if args.list_schedule:
        print(render_schedule(races, now))
        return

    pos_models, binary_models = load_position_models(Path(args.official_models_dir))
    candidate_model = load_candidate_model(Path(args.candidate_model))
    upset_model_path = Path(args.upset_model) if args.upset_model else None
    upset_model = load_pickle_model(upset_model_path) if upset_model_path and upset_model_path.exists() else None
    targets = []
    for race in races:
        minutes_to_deadline = (race.deadline_dt - now).total_seconds() / 60.0
        if args.jcd and race.jcd != args.jcd.zfill(2):
            continue
        if args.rno and race.rno != args.rno:
            continue
        if not args.rno and not (args.from_min <= minutes_to_deadline <= args.to_min):
            continue
        targets.append(race)

    if not targets:
        print("対象レースなし")
        return

    rendered = []
    for race in targets:
        try:
            rendered.append(
                predict_and_save_race(args, date_str, now, client, race, pos_models, binary_models, candidate_model, upset_model)
            )
        except Exception as exc:
            rendered.append(f"## {race.course} {race.rno}R\n- skip: {exc}")
    print("\n\n".join(rendered))


def run_watch(args: argparse.Namespace) -> None:
    seen: set[str] = set()
    print("watch start", flush=True)
    while True:
        date_str = args.date or datetime.now(JST).strftime("%Y%m%d")
        now = datetime.now(JST)
        if args.watch_start:
            start_dt = datetime.strptime(f"{date_str} {args.watch_start}", "%Y%m%d %H:%M").replace(tzinfo=JST)
            if now < start_dt:
                print(f"waiting until {args.watch_start} / now {now.strftime('%H:%M:%S')}", flush=True)
                time.sleep(min(args.interval_sec, max(1, int((start_dt - now).total_seconds()))))
                continue
        if args.watch_end:
            end_dt = datetime.strptime(f"{date_str} {args.watch_end}", "%Y%m%d %H:%M").replace(tzinfo=JST)
            if now > end_dt:
                print(f"watch done: past {args.watch_end}", flush=True)
                return
        client = OfficialClient(
            sleep_sec=args.sleep_sec,
            cache_dir=Path(args.cache_dir),
            offline_root=Path(args.offline_root) if args.offline_root else None,
        )
        try:
            if args.jcd:
                races = client.fetch_course_schedules(date_str, args.jcd.zfill(2), start_offset_min=args.start_offset_min)
            else:
                races = client.fetch_schedules(date_str, start_offset_min=args.start_offset_min)
        except Exception as exc:
            print(f"schedule fetch failed: {exc}", flush=True)
            time.sleep(args.interval_sec)
            continue

        remaining = 0
        for race in races:
            minutes_to_deadline = (race.deadline_dt - now).total_seconds() / 60.0
            if minutes_to_deadline > args.to_min:
                remaining += 1
                continue
            if minutes_to_deadline < args.from_min:
                continue
            if args.rno and race.rno != args.rno:
                continue
            if race.race_id in seen:
                continue
            run_args = argparse.Namespace(**vars(args))
            run_args.date = date_str
            run_args.jcd = race.jcd
            run_args.rno = race.rno
            run_args.list_schedule = False
            run_once(run_args)
            seen.add(race.race_id)

        if not remaining and all((race.deadline_dt - now).total_seconds() / 60.0 < args.from_min for race in races):
            print("watch done", flush=True)
            return
        time.sleep(args.interval_sec)


def run_update_results(args: argparse.Namespace) -> None:
    date_str = args.date or datetime.now(JST).strftime("%Y%m%d")
    preds = load_thread_predictions(date_str)
    if not preds:
        print(f"予測ログなし: {date_str}")
        return
    latest_by_race: dict[str, dict[str, Any]] = {}
    for pred in preds:
        race_data = pred.get("race", {})
        race_id = f"{race_data.get('date')}_{str(race_data.get('jcd')).zfill(2)}_{race_data.get('rno')}"
        latest_by_race[race_id] = pred
    preds = list(latest_by_race.values())
    client = OfficialClient(
        sleep_sec=args.sleep_sec,
        cache_dir=Path(args.cache_dir),
        offline_root=Path(args.offline_root) if args.offline_root else None,
    )
    rendered = []
    matched = 0
    for pred in preds:
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
        try:
            race_dir = result_dir_for(client, race)
            result = parse_raceresult(race_dir / "raceresult.html")
        except Exception as exc:
            rendered.append(f"## 結果更新: {race.course} {race.rno}R\n- pending/skip: {exc}")
            continue
        if "winner" not in result:
            rendered.append(f"## 結果更新: {race.course} {race.rno}R\n- 結果未確定")
            continue
        matched += 1
        rendered.append(render_result_update(pred, result))
    print("\n\n".join(rendered) if rendered else "対象予測なし")
    print(f"\n更新対象={len(preds)} / 結果取得={matched}")


def scheduled_races(args: argparse.Namespace, client: OfficialClient, date_str: str) -> list[RaceSchedule]:
    if args.jcd:
        races = client.fetch_course_schedules(date_str, args.jcd.zfill(2), start_offset_min=args.start_offset_min)
    else:
        races = client.fetch_schedules(date_str, start_offset_min=args.start_offset_min)
    if args.rno:
        races = [race for race in races if race.rno == args.rno]
    return races


def render_run_plan(races: list[RaceSchedule], args: argparse.Namespace, now: datetime) -> str:
    lines = [
        "# 取得予定",
        "",
        f"- 予測取得: 締切 {args.predict_before_deadline_min:g} 分前",
        f"- 結果確認: 締切 {args.result_after_deadline_min:g} 分後",
        "",
        "| 場 | R | 締切 | 発走目安 | 予測取得予定 | 結果確認予定 |",
        "|---|---:|---:|---:|---:|---:|",
    ]
    for race in races:
        predict_at = race.deadline_dt - timedelta(minutes=args.predict_before_deadline_min)
        result_at = race.deadline_dt + timedelta(minutes=args.result_after_deadline_min)
        status = ""
        if result_at < now:
            status = " 済み時刻"
        elif predict_at < now:
            status = " 予測時刻超過"
        lines.append(
            f"| {race.course} | {race.rno}R | {race.deadline} | {race.start_time} | "
            f"{predict_at.strftime('%H:%M')} | {result_at.strftime('%H:%M')}{status} |"
        )
    return "\n".join(lines)


def run_scheduled(args: argparse.Namespace) -> None:
    date_str = args.date or datetime.now(JST).strftime("%Y%m%d")
    client = OfficialClient(
        sleep_sec=args.sleep_sec,
        cache_dir=Path(args.cache_dir),
        offline_root=Path(args.offline_root) if args.offline_root else None,
    )
    races = scheduled_races(args, client, date_str)
    now = datetime.now(JST)
    print(render_run_plan(races, args, now), flush=True)
    if args.list_run_plan:
        return

    pos_models, binary_models = load_position_models(Path(args.official_models_dir))
    candidate_model = load_candidate_model(Path(args.candidate_model))
    upset_model_path = Path(args.upset_model) if args.upset_model else None
    upset_model = load_pickle_model(upset_model_path) if upset_model_path and upset_model_path.exists() else None
    events: list[tuple[datetime, str, RaceSchedule]] = []
    for race in races:
        events.append((race.deadline_dt - timedelta(minutes=args.predict_before_deadline_min), "predict", race))
        events.append((race.deadline_dt + timedelta(minutes=args.result_after_deadline_min), "result", race))
    events.sort(key=lambda item: item[0])

    for event_at, kind, race in events:
        now = datetime.now(JST)
        if args.skip_past and event_at < now:
            continue
        wait_sec = (event_at - now).total_seconds()
        if wait_sec > 0:
            print(
                f"sleep until {event_at.strftime('%H:%M:%S')} "
                f"/ {race.course} {race.rno}R {kind} / 締切 {race.deadline}",
                flush=True,
            )
            time.sleep(wait_sec)
        if kind == "predict":
            print(f"\n# 予測取得 {race.course} {race.rno}R / 締切 {race.deadline}", flush=True)
            try:
                print(
                    predict_and_save_race(
                        args, date_str, datetime.now(JST), client, race, pos_models, binary_models, candidate_model, upset_model
                    ),
                    flush=True,
                )
            except Exception as exc:
                print(f"## {race.course} {race.rno}R\n- skip: {exc}", flush=True)
        else:
            print(f"\n# 結果確認 {race.course} {race.rno}R / 締切 {race.deadline}", flush=True)
            run_args = argparse.Namespace(**vars(args))
            run_args.update_results = True
            run_args.jcd = race.jcd
            run_args.rno = race.rno
            run_update_results(run_args)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", default=None, help="YYYYMMDD. 省略時は今日")
    parser.add_argument("--list-schedule", action="store_true")
    parser.add_argument("--watch", action="store_true")
    parser.add_argument("--schedule-run", action="store_true")
    parser.add_argument("--list-run-plan", action="store_true")
    parser.add_argument("--update-results", action="store_true")
    parser.add_argument("--watch-start", default=None, help="HH:MM. この時刻までは待機")
    parser.add_argument("--watch-end", default=None, help="HH:MM. この時刻を過ぎたら終了")
    parser.add_argument("--interval-sec", type=int, default=60)
    parser.add_argument("--from-min", type=float, default=5)
    parser.add_argument("--to-min", type=float, default=35)
    parser.add_argument("--jcd", default=None)
    parser.add_argument("--rno", type=int, default=None)
    parser.add_argument("--sleep-sec", type=float, default=0.3)
    parser.add_argument("--start-offset-min", type=int, default=2)
    parser.add_argument("--predict-before-deadline-min", type=float, default=15)
    parser.add_argument("--result-after-deadline-min", type=float, default=7)
    parser.add_argument("--skip-past", action="store_true")
    parser.add_argument("--cache-dir", default=str(WORK_DIR / "live_pages"))
    parser.add_argument("--offline-root", default=None)
    parser.add_argument("--official-models-dir", default=str(DEFAULT_OFFICIAL_MODELS_DIR))
    parser.add_argument(
        "--candidate-model",
        default=str(DEFAULT_CANDIDATE_MODEL),
    )
    parser.add_argument("--upset-model", default=str(DEFAULT_UPSET_MODEL))
    parser.add_argument("--top-candidates", type=int, default=10)
    parser.add_argument("--top-per-race", type=int, default=5)
    parser.add_argument("--min-odds", type=float, default=50)
    parser.add_argument("--max-odds", type=float, default=300)
    parser.add_argument("--min-model-ev", type=float, default=0.1)
    parser.add_argument("--prob-scale", type=float, default=0.2)
    parser.add_argument("--kelly-scale", type=float, default=0.25)
    parser.add_argument("--max-kelly-fraction", type=float, default=0.003)
    parser.add_argument("--bankroll", type=float, default=10000)
    parser.add_argument("--base-stake", type=int, default=100)
    args = parser.parse_args()
    if args.update_results:
        run_update_results(args)
    elif args.schedule_run or args.list_run_plan:
        run_scheduled(args)
    elif args.watch:
        run_watch(args)
    else:
        run_once(args)


if __name__ == "__main__":
    main()
