from __future__ import annotations

import argparse
import json
import os
import pickle
import re
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from lxml import html

try:
    import requests
except ModuleNotFoundError:
    requests = None


JST = timezone(timedelta(hours=9), "JST")
BASE_URL = "https://www.boatrace.jp/owpc/pc/race"
PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_ROOT = PROJECT_ROOT.parent / "bortrace_data"
OUTPUT_DIR = PROJECT_ROOT / "outputs"
MODEL_PATH = DATA_ROOT / "output_v4" / "final_model_v4.pkl"
CONFIG_PATH = DATA_ROOT / "output_v4" / "model_config_v4.pkl"
PREDICTION_DIR = OUTPUT_DIR / "live_predictions"
NOTIFIED_LOG = OUTPUT_DIR / "live_notified_races.log"

COURSE_MAP = {
    "桐生": "01", "戸田": "02", "江戸川": "03", "平和島": "04", "多摩川": "05",
    "浜名湖": "06", "蒲郡": "07", "常滑": "08", "津": "09", "三国": "10",
    "びわこ": "11", "住之江": "12", "尼崎": "13", "鳴門": "14", "丸亀": "15",
    "児島": "16", "宮島": "17", "徳山": "18", "下関": "19", "若松": "20",
    "芦屋": "21", "福岡": "22", "唐津": "23", "大村": "24",
}
JCD_TO_COURSE = {v: k for k, v in COURSE_MAP.items()}
CLASS_VALUE = {"A1": 4, "A2": 3, "B1": 2, "B2": 1}


@dataclass(frozen=True)
class RaceSchedule:
    date: str
    course: str
    jcd: str
    rno: int
    deadline: str
    url: str

    @property
    def race_id(self) -> str:
        return f"{self.date}_{self.jcd}_{self.rno}"

    @property
    def deadline_dt(self) -> datetime:
        return datetime.strptime(f"{self.date} {self.deadline}", "%Y%m%d %H:%M").replace(tzinfo=JST)


def text_of(node: Any) -> str:
    return " ".join(" ".join(node.xpath(".//text()")).split())


def to_float(value: str | None, default: float | None = None) -> float | None:
    if value is None:
        return default
    value = value.replace(",", "").replace("¥", "").replace("kg", "").strip()
    if value.startswith("."):
        value = "0" + value
    try:
        return float(value)
    except ValueError:
        return default


def load_assets() -> tuple[Any, dict[str, Any]]:
    if not MODEL_PATH.exists() or not CONFIG_PATH.exists():
        raise FileNotFoundError(f"model/config not found: {MODEL_PATH}, {CONFIG_PATH}")
    with MODEL_PATH.open("rb") as f:
        model = pickle.load(f)
    with CONFIG_PATH.open("rb") as f:
        config = pickle.load(f)
    return model, config


class OfficialLiveClient:
    def __init__(self) -> None:
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
            "Accept-Language": "ja,en-US;q=0.9,en;q=0.8",
        }
        self.session = requests.Session() if requests else None
        if self.session:
            self.session.headers.update(self.headers)

    def get_doc(self, url: str, referer: str | None = None, retries: int = 3):
        last_error: Exception | None = None
        for attempt in range(retries):
            try:
                headers = {"Referer": referer} if referer else {}
                if self.session:
                    res = self.session.get(url, headers=headers, timeout=20)
                    res.raise_for_status()
                    content = res.content
                else:
                    req_headers = dict(self.headers)
                    req_headers.update(headers)
                    req = urllib.request.Request(url, headers=req_headers)
                    with urllib.request.urlopen(req, timeout=20) as res:
                        content = res.read()
                return html.fromstring(content)
            except Exception as exc:
                last_error = exc
                time.sleep(1.5 * (attempt + 1))
        raise RuntimeError(f"failed to fetch {url}: {last_error}")

    def fetch_schedules(self, date_str: str) -> list[RaceSchedule]:
        index_url = f"{BASE_URL}/index?hd={date_str}"
        index_doc = self.get_doc(index_url)
        active_jcds = sorted(
            {
                m.group(1)
                for href in index_doc.xpath("//a/@href")
                for m in [re.search(r"jcd=(\d{2})", href)]
                if m and m.group(1) in JCD_TO_COURSE
            }
        )
        schedules: dict[tuple[str, int], RaceSchedule] = {}
        for jcd in active_jcds:
            raceindex_url = f"{BASE_URL}/raceindex?jcd={jcd}&hd={date_str}"
            doc = self.get_doc(raceindex_url, referer=index_url)
            for link in doc.xpath("//a[contains(@href, 'racelist') and contains(@href, 'rno=')]"):
                href = link.get("href") or ""
                m_rno = re.search(r"rno=(\d{1,2})", href)
                if not m_rno:
                    continue
                rno = int(m_rno.group(1))
                row = link.xpath("ancestor::tr[1]")
                row_text = text_of(row[0]) if row else text_of(link)
                m_time = re.search(r"(\d{1,2}:\d{2})", row_text)
                if not m_time:
                    continue
                deadline = m_time.group(1).zfill(5)
                url = "https://www.boatrace.jp" + href if href.startswith("/") else href
                schedules[(jcd, rno)] = RaceSchedule(
                    date=date_str,
                    course=JCD_TO_COURSE[jcd],
                    jcd=jcd,
                    rno=rno,
                    deadline=deadline,
                    url=url,
                )
            time.sleep(0.2)
        return sorted(schedules.values(), key=lambda r: (r.deadline, r.jcd, r.rno))

    def fetch_race_data(self, race: RaceSchedule) -> dict[str, Any]:
        racelist_url = f"{BASE_URL}/racelist?rno={race.rno}&jcd={race.jcd}&hd={race.date}"
        beforeinfo_url = f"{BASE_URL}/beforeinfo?rno={race.rno}&jcd={race.jcd}&hd={race.date}"
        list_doc = self.get_doc(racelist_url, referer=f"{BASE_URL}/index?hd={race.date}")
        before_doc = self.get_doc(beforeinfo_url, referer=racelist_url)
        if "データがありません" in text_of(before_doc):
            raise RuntimeError("beforeinfo has no exhibition data yet")
        data = {"deadline": race.deadline}
        data.update(parse_racelist(list_doc))
        data.update(parse_beforeinfo(before_doc))
        missing = [f"ex_time_{i}" for i in range(1, 7) if data.get(f"ex_time_{i}") is None]
        if missing:
            raise RuntimeError(f"missing exhibition fields: {', '.join(missing)}")
        return data


def parse_racelist(doc) -> dict[str, Any]:
    out: dict[str, Any] = {}
    tables = doc.xpath("//table")
    if len(tables) < 2:
        return out
    for tr in tables[1].xpath(".//tr"):
        cells = [text_of(c) for c in tr.xpath("./th|./td")]
        if len(cells) < 8 or cells[0] not in {"1", "2", "3", "4", "5", "6", "１", "２", "３", "４", "５", "６"}:
            continue
        boat = int(cells[0].translate(str.maketrans("１２３４５６", "123456")))
        info = cells[2]
        m_class = re.search(r"/\s*([AB][12])", info)
        if not m_class:
            continue
        out[f"rank_{boat}"] = m_class.group(1) if m_class else "B2"
        rates = re.findall(r"\d+\.\d+", cells[4])
        out[f"win_rate_{boat}"] = float(rates[0]) if rates else 0.0
    return out


def parse_beforeinfo(doc) -> dict[str, Any]:
    out: dict[str, Any] = {"wind_speed": 0, "wave": 0}
    body_text = text_of(doc)
    wind = re.search(r"風速\s*(\d+)m", body_text)
    wave = re.search(r"波高\s*(\d+)cm", body_text)
    if wind:
        out["wind_speed"] = int(wind.group(1))
    if wave:
        out["wave"] = int(wave.group(1))

    tables = doc.xpath("//table")
    target_table = None
    for table in tables:
        table_text = text_of(table)
        if "展示" in table_text and "チルト" in table_text and "体重" in table_text:
            target_table = table
            break
    if target_table is not None:
        for tr in target_table.xpath(".//tr"):
            cells = [text_of(c) for c in tr.xpath("./th|./td")]
            if len(cells) < 6 or cells[0] not in {"1", "2", "3", "4", "5", "6"}:
                continue
            boat = int(cells[0])
            out[f"weight_{boat}"] = to_float(cells[3])
            out[f"ex_time_{boat}"] = to_float(cells[4])
            out[f"tilt_{boat}"] = to_float(cells[5])

    start_table = None
    for table in tables:
        if "スタート展示" in text_of(table):
            start_table = table
            break
    if start_table is not None:
        for tr in start_table.xpath(".//tr"):
            row_text = text_of(tr)
            m = re.match(r"^([1-6])\s+(F?\.?\d+)", row_text)
            if m:
                course = int(m.group(1))
                out[f"st_{course}"] = to_float(m.group(2).replace("F", ""), 0.15)

    for i in range(1, 7):
        out.setdefault(f"st_{i}", 0.15)
    return out


def build_features(data: dict[str, Any], feature_columns: list[str]) -> tuple[pd.DataFrame, dict[str, Any]]:
    ex_vals = [float(data[f"ex_time_{i}"]) for i in range(1, 7)]
    ex_mean = float(np.mean(ex_vals))
    ex_ranks = pd.Series(ex_vals).rank(method="min").tolist()
    features: dict[str, Any] = {"wind_speed": data.get("wind_speed", 0), "wave": data.get("wave", 0)}
    for i in range(1, 7):
        rank_val = CLASS_VALUE.get(data.get(f"rank_{i}", "B2"), 1)
        features[f"rank_val_{i}"] = rank_val
        features[f"win_rate_{i}"] = float(data.get(f"win_rate_{i}", 0.0))
        features[f"ex_time_{i}"] = float(data[f"ex_time_{i}"])
        features[f"ex_diff_{i}"] = float(data[f"ex_time_{i}"]) - ex_mean
        features[f"ex_rank_{i}"] = int(ex_ranks[i - 1])
        features[f"st_{i}"] = float(data.get(f"st_{i}", 0.15))
    features["is_debuff_1"] = int(features["rank_val_1"] <= 2 and features["ex_rank_1"] >= 4)
    frame = pd.DataFrame([features])
    for col in feature_columns:
        if col not in frame.columns:
            frame[col] = 0
    return frame[feature_columns], features


def predict_race(model: Any, config: dict[str, Any], client: OfficialLiveClient, race: RaceSchedule) -> dict[str, Any]:
    data = client.fetch_race_data(race)
    x, features = build_features(data, config["features"])
    probs = model.predict(x)[0]
    in_win_prob = float(probs[0])
    upset_prob = 1.0 - in_win_prob
    ranking = sorted(((i + 1, float(p)) for i, p in enumerate(probs)), key=lambda item: item[1], reverse=True)
    other_ranking = [item for item in ranking if item[0] != 1]
    top_other = other_ranking[0]
    strategy = None
    if upset_prob >= 0.55:
        if top_other[1] >= 0.35:
            strategy = "FOCUS"
        elif top_other[1] >= 0.25:
            strategy = "STANDARD"
        else:
            strategy = "WIDE"
    return {
        "race_id": race.race_id,
        "date": race.date,
        "jcd": race.jcd,
        "course": race.course,
        "rno": race.rno,
        "deadline": race.deadline,
        "deadline_ts": int(race.deadline_dt.timestamp()),
        "predicted_at": datetime.now(JST).isoformat(),
        "strategy": strategy,
        "in_win_prob": in_win_prob,
        "upset_prob": upset_prob,
        "ranking": ranking,
        "top_other": top_other,
        "features": features,
        "url": race.url,
    }


def is_notified(race_id: str) -> bool:
    if not NOTIFIED_LOG.exists():
        return False
    return race_id in set(NOTIFIED_LOG.read_text(encoding="utf-8").splitlines())


def mark_notified(race_id: str) -> None:
    NOTIFIED_LOG.parent.mkdir(parents=True, exist_ok=True)
    with NOTIFIED_LOG.open("a", encoding="utf-8") as f:
        f.write(race_id + "\n")


def append_prediction(record: dict[str, Any]) -> None:
    PREDICTION_DIR.mkdir(parents=True, exist_ok=True)
    path = PREDICTION_DIR / f"predictions_{record['date']}.jsonl"
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")


def build_embed(record: dict[str, Any]) -> dict[str, Any]:
    rank_lines = [
        f"{idx + 1}. **{boat}号艇** `{prob:.1%}`"
        for idx, (boat, prob) in enumerate(record["ranking"][:6])
    ]
    features = record["features"]
    ex_lines = [
        f"{i}号艇: 展示 `{features.get(f'ex_time_{i}', 0):.2f}` / ST `{features.get(f'st_{i}', 0):.2f}` / 展示順位 `{int(features.get(f'ex_rank_{i}', 0))}`"
        for i in range(1, 7)
    ]
    color = 0xE74C3C if record["strategy"] == "FOCUS" else 0xF1C40F if record["strategy"] == "STANDARD" else 0x3498DB
    bet = "1抜きBOX" if record["strategy"] == "WIDE" else f"{record['top_other'][0]}号艇軸候補"
    return {
        "title": f"{record['course']} {record['rno']}R | {record['strategy']}",
        "url": record["url"],
        "color": color,
        "description": (
            f"締切 **{record['deadline']}** / <t:{record['deadline_ts']}:R>\n"
            f"イン飛び `{record['upset_prob']:.1%}` / 1号艇勝率 `{record['in_win_prob']:.1%}`"
        ),
        "fields": [
            {"name": "AI順位", "value": "\n".join(rank_lines), "inline": True},
            {"name": "推奨", "value": bet, "inline": True},
            {"name": "展示航走", "value": "\n".join(ex_lines), "inline": False},
            {
                "name": "根拠",
                "value": f"1号艇級別 `{features.get('rank_val_1')}` / 1号艇展示順位 `{int(features.get('ex_rank_1', 0))}` / 風 `{features.get('wind_speed')}m` / 波 `{features.get('wave')}cm`",
                "inline": False,
            },
        ],
        "footer": {"text": f"race_id={record['race_id']}"},
        "timestamp": datetime.now(JST).isoformat(),
    }


def post_discord(record: dict[str, Any], webhook_url: str) -> None:
    payload = {
        "content": f"🎯 投資候補: **{record['course']} {record['rno']}R** 締切 <t:{record['deadline_ts']}:R>",
        "embeds": [build_embed(record)],
    }
    if requests:
        res = requests.post(webhook_url, json=payload, timeout=15)
        res.raise_for_status()
    else:
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        req = urllib.request.Request(
            webhook_url,
            data=data,
            headers={"Content-Type": "application/json", "User-Agent": "Mozilla/5.0"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=15) as res:
            if res.status >= 400:
                raise RuntimeError(f"Discord webhook failed: HTTP {res.status}")


def run(minutes_from: int, minutes_to: int, notify_all: bool = False) -> None:
    model, config = load_assets()
    client = OfficialLiveClient()
    now = datetime.now(JST)
    date_str = now.strftime("%Y%m%d")
    schedules = client.fetch_schedules(date_str)
    webhook = os.environ.get("DISCORD_WEBHOOK_URL")
    hits = 0
    for race in schedules:
        diff_min = (race.deadline_dt - now).total_seconds() / 60
        if not (minutes_from <= diff_min <= minutes_to):
            continue
        if is_notified(race.race_id):
            continue
        try:
            record = predict_race(model, config, client, race)
        except Exception as exc:
            print(f"skip {race.course} {race.rno}R: {exc}")
            continue
        append_prediction(record)
        if record["strategy"] or notify_all:
            hits += 1
            if webhook:
                post_discord(record, webhook)
            mark_notified(race.race_id)
            print(f"sent {race.course} {race.rno}R {record['strategy']} upset={record['upset_prob']:.1%}")
    print(f"done targets={hits}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--from-min", type=int, default=5)
    parser.add_argument("--to-min", type=int, default=35)
    parser.add_argument("--notify-all", action="store_true")
    args = parser.parse_args()
    run(args.from_min, args.to_min, notify_all=args.notify_all)


if __name__ == "__main__":
    main()
