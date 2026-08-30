from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd
from lxml import html

from .paths import LEGACY_DATA_ROOT
from .kibetsu import enrich_race


FULLWIDTH_DIGITS = str.maketrans("１２３４５６７８９０", "1234567890")
CLASS_VALUE = {"A1": 4, "A2": 3, "B1": 2, "B2": 1}
GRADE_VALUE = {
    "ippan": 0,
    "G3": 1,
    "G2": 2,
    "G1": 3,
    "PG1": 4,
    "SG": 5,
}


@dataclass(frozen=True)
class RacePath:
    date: str
    jcd: str
    rno: int
    root: Path

    @property
    def key(self) -> tuple[str, str, int]:
        return (self.date, self.jcd, self.rno)

    def page(self, name: str) -> Path:
        return self.root / f"{name}.html"


def _text(node: Any) -> str:
    return " ".join(" ".join(node.xpath(".//text()")).split())


def _to_float(value: str | None) -> float | None:
    if value is None:
        return None
    value = value.replace(",", "").replace("¥", "").strip()
    if value in {"", "-", "--"}:
        return None
    if value.startswith("."):
        value = "0" + value
    try:
        return float(value)
    except ValueError:
        return None


def _to_int(value: str | None) -> int | None:
    f = _to_float(value)
    if f is None:
        return None
    return int(f)


def _norm_boat(value: str) -> int | None:
    value = value.strip().translate(FULLWIDTH_DIGITS)
    if value in {"1", "2", "3", "4", "5", "6"}:
        return int(value)
    return None


def _read_doc(path: Path):
    # Official pages are saved as UTF-8 even when an upstream/meta declaration is
    # occasionally inconsistent. Passing bytes lets libxml honour the stale
    # declaration and corrupt Japanese labels used by metadata parsing.
    return html.fromstring(path.read_bytes(), parser=html.HTMLParser(encoding="utf-8"))


def iter_race_paths(
    official_root: Path | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
) -> list[RacePath]:
    official_root = official_root or (LEGACY_DATA_ROOT / "official_pages")
    paths: list[RacePath] = []
    if not official_root.exists():
        return paths

    for date_dir in sorted(p for p in official_root.iterdir() if p.is_dir() and re.fullmatch(r"\d{8}", p.name)):
        date = date_dir.name
        if start_date and date < start_date:
            continue
        if end_date and date > end_date:
            continue
        for jcd_dir in sorted(p for p in date_dir.iterdir() if p.is_dir() and re.fullmatch(r"\d{2}", p.name)):
            for race_dir in sorted((p for p in jcd_dir.iterdir() if p.is_dir() and p.name.isdigit()), key=lambda p: int(p.name)):
                paths.append(RacePath(date=date, jcd=jcd_dir.name, rno=int(race_dir.name), root=race_dir))
    return paths


def _class_tokens(node: Any) -> set[str]:
    return {token for token in (node.get("class") or "").split() if token}


def _parse_common_race_metadata(doc: Any, rno: int | None = None) -> dict[str, Any]:
    """Parse fields visible before the race from the common official-page header."""
    out: dict[str, Any] = {}
    title_nodes = doc.xpath("//div[contains(@class,'heading2_title')][.//h2[contains(@class,'heading2_titleName')]]")
    if title_nodes:
        title_node = title_nodes[0]
        title = _text(title_node.xpath(".//h2[contains(@class,'heading2_titleName')]")[0])
        out["event_title"] = title
        out["is_womens_event"] = int(any(x in title for x in ("女子", "レディース", "ヴィーナス")))
        out["is_rookie_event"] = int("ルーキー" in title)
        out["is_senior_event"] = int("マスターズ" in title)
        grade_token = next((x[3:] for x in _class_tokens(title_node) if x.startswith("is-") and x != "is-type3"), "")
        normalized = grade_token.upper()
        if normalized.startswith("SG"):
            grade = "SG"
        elif normalized.startswith("PG1"):
            grade = "PG1"
        elif normalized.startswith("G1"):
            grade = "G1"
        elif normalized.startswith("G2"):
            grade = "G2"
        elif normalized.startswith("G3"):
            grade = "G3"
        else:
            grade = "ippan"
        out["grade"] = grade
        out["grade_val"] = GRADE_VALUE[grade]

    active_days = doc.xpath("//ul[contains(@class,'tab2_tabs')]/li[contains(@class,'is-active2')]//span[contains(@class,'tab2_inner')]")
    if active_days:
        day_text = _text(active_days[0])
        out["event_day_label"] = day_text
        out["is_final_day"] = int("最終日" in day_text)
        if "初日" in day_text:
            out["event_day"] = 1
        else:
            m = re.search(r"([0-9０-９]+)日目", day_text.translate(FULLWIDTH_DIGITS))
            if m:
                out["event_day"] = int(m.group(1))
        if "event_day" not in out:
            active_li = active_days[0].xpath("ancestor::li[1]")
            if active_li:
                preceding = active_li[0].xpath("preceding-sibling::li")
                out["event_day"] = len(preceding) + 1
        day_tabs = doc.xpath("//ul[contains(@class,'tab2_tabs')]/li")
        if day_tabs:
            out["event_days_total"] = len(day_tabs)

    detail_nodes = doc.xpath("//h3[contains(@class,'title16_titleDetail')]")
    if detail_nodes:
        detail = _text(detail_nodes[0]).translate(FULLWIDTH_DIGITS)
        distance = re.search(r"(\d{3,4})\s*m", detail, re.I)
        if distance:
            out["race_distance"] = int(distance.group(1))
        stage = re.sub(r"\d{3,4}\s*m.*$", "", detail, flags=re.I).strip()
        if stage:
            out["race_stage"] = stage
            out["is_qualifying"] = int("予選" in stage)
            out["is_semifinal"] = int("準優" in stage)
            out["is_final"] = int("優勝" in stage)
            out["is_selection"] = int("選抜" in stage)
            out["is_special_selection"] = int(any(x in stage for x in ("特選", "特賞", "ドリーム")))
            out["is_general_race"] = int("一般" in stage)

    labels = " ".join(_text(x) for x in doc.xpath("//div[contains(@class,'title16_titleLabels')]//span"))
    out["is_fixed_entry"] = int("進入固定" in labels)
    out["uses_stabilizer"] = int("安定板" in labels)

    if rno is not None:
        for table in doc.xpath("//table"):
            headers = [_text(x) for x in table.xpath(".//thead//th")]
            wanted = f"{rno}R"
            if wanted not in headers:
                continue
            idx = headers.index(wanted)
            deadline_cells = table.xpath(".//tbody/tr[.//*[contains(normalize-space(.),'締切予定時刻')]]/td")
            # The first td is the row label and race cells follow it.
            if len(deadline_cells) > idx:
                value = _text(deadline_cells[idx])
                m = re.search(r"(\d{1,2}):(\d{2})", value)
                if m:
                    out["deadline_hour"] = int(m.group(1))
                    out["deadline_minute"] = int(m.group(2))
                    out["deadline_minutes"] = int(m.group(1)) * 60 + int(m.group(2))
            break
    return out


def parse_racelist(path: Path, rno: int | None = None) -> dict[str, Any]:
    if not path.exists():
        return {}
    doc = _read_doc(path)
    tables = doc.xpath("//table")
    if len(tables) < 2:
        return {}
    out: dict[str, Any] = _parse_common_race_metadata(doc, rno=rno)
    for tr in tables[1].xpath(".//tr"):
        cells = [_text(c) for c in tr.xpath("./th|./td")]
        if len(cells) < 8:
            continue
        boat = _norm_boat(cells[0])
        if boat is None:
            continue
        info = cells[2]
        m = re.search(r"(\d{4})\s*/\s*([AB][12])\s+(.+?)\s+(.+?)/(.+?)\s+(\d+)歳/([\d.]+)kg", info)
        if m:
            out[f"reg_no_{boat}"] = int(m.group(1))
            out[f"class_{boat}"] = m.group(2)
            out[f"class_val_{boat}"] = CLASS_VALUE.get(m.group(2), 0)
            out[f"age_{boat}"] = int(m.group(6))
            out[f"entry_weight_{boat}"] = float(m.group(7))
        flst = re.search(r"F(\d+)\s+L(\d+)\s+([\d.]+)", cells[3])
        if flst:
            out[f"f_count_{boat}"] = int(flst.group(1))
            out[f"l_count_{boat}"] = int(flst.group(2))
            out[f"avg_st_{boat}"] = float(flst.group(3))
        for prefix, cell in [
            ("national", cells[4]),
            ("local", cells[5]),
            ("motor", cells[6]),
            ("boat", cells[7]),
        ]:
            vals = re.findall(r"\d+\.\d+|\d+", cell)
            if prefix in {"national", "local"} and len(vals) >= 3:
                out[f"{prefix}_win_rate_{boat}"] = float(vals[0])
                out[f"{prefix}_quinella_rate_{boat}"] = float(vals[1])
                out[f"{prefix}_trio_rate_{boat}"] = float(vals[2])
            elif prefix in {"motor", "boat"} and len(vals) >= 3:
                out[f"{prefix}_no_{boat}"] = int(vals[0])
                out[f"{prefix}_quinella_rate_{boat}"] = float(vals[1])
                out[f"{prefix}_trio_rate_{boat}"] = float(vals[2])
    return out


def parse_beforeinfo(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    doc = _read_doc(path)
    tables = doc.xpath("//table")
    out: dict[str, Any] = {}
    weather_nodes = doc.xpath("//div[contains(concat(' ',normalize-space(@class),' '),' weather1 ')]")
    if weather_nodes:
        weather = weather_nodes[-1]
        weather_title = weather.xpath(".//*[contains(@class,'weather1_title')]")
        if weather_title:
            m = re.search(r"(\d+)R時点", _text(weather_title[0]).translate(FULLWIDTH_DIGITS))
            if m:
                out["weather_asof_rno"] = int(m.group(1))
        for unit in weather.xpath(".//div[contains(@class,'weather1_bodyUnit')]"):
            classes = _class_tokens(unit)
            label_nodes = unit.xpath(".//*[contains(@class,'weather1_bodyUnitLabelTitle')]")
            data_nodes = unit.xpath(".//*[contains(@class,'weather1_bodyUnitLabelData')]")
            label = _text(label_nodes[0]) if label_nodes else ""
            data = _text(data_nodes[0]) if data_nodes else ""
            if "is-direction" in classes:
                image = unit.xpath(".//*[contains(@class,'weather1_bodyUnitImage')]")
                if image:
                    token = next((x for x in _class_tokens(image[0]) if re.fullmatch(r"is-direction\d+", x)), "")
                    if token:
                        out["course_direction_code"] = int(re.search(r"\d+", token).group())
                out["air_temperature"] = _to_float(data.replace("℃", ""))
            elif "is-weather" in classes:
                out["weather"] = label
                image = unit.xpath(".//*[contains(@class,'weather1_bodyUnitImage')]")
                if image:
                    token = next((x for x in _class_tokens(image[0]) if re.fullmatch(r"is-weather\d+", x)), "")
                    if token:
                        out["weather_code"] = int(re.search(r"\d+", token).group())
            elif "is-wind" in classes:
                out["wind_speed"] = _to_float(data.replace("m", ""))
            elif "is-windDirection" in classes:
                image = unit.xpath(".//*[contains(@class,'weather1_bodyUnitImage')]")
                if image:
                    token = next((x for x in _class_tokens(image[0]) if re.fullmatch(r"is-wind\d+", x)), "")
                    if token:
                        out["wind_direction_code"] = int(re.search(r"\d+", token).group())
            elif "is-waterTemperature" in classes:
                out["water_temperature"] = _to_float(data.replace("℃", ""))
            elif "is-wave" in classes:
                out["wave_height"] = _to_float(data.replace("cm", ""))
    if len(tables) >= 2:
        for tr in tables[1].xpath(".//tr"):
            cells = [_text(c) for c in tr.xpath("./th|./td")]
            if len(cells) < 6:
                continue
            boat = _norm_boat(cells[0])
            if boat is None:
                continue
            out[f"weight_{boat}"] = _to_float(cells[3].replace("kg", ""))
            out[f"ex_time_{boat}"] = _to_float(cells[4])
            out[f"tilt_{boat}"] = _to_float(cells[5])
    if len(tables) >= 3:
        for tr in tables[2].xpath(".//tr"):
            text = _text(tr)
            m = re.fullmatch(r"([1-6])\s+(\.?\d+\.\d+|\.?\d+)", text)
            if not m:
                m = re.fullmatch(r"([1-6])\s+(\.\d{2}|F\.\d{2}|L\.\d{2})", text)
            if m:
                course = int(m.group(1))
                st = m.group(2).replace("F", "").replace("L", "")
                out[f"start_ex_course_{course}"] = course
                out[f"start_ex_st_by_course_{course}"] = _to_float(st)
    return out


def parse_prerace_metadata(path: Path, rno: int) -> dict[str, Any]:
    """Read all newly-added pre-race metadata from a single beforeinfo page."""
    if not path.exists():
        return {}
    out = parse_beforeinfo(path)
    out.update(_parse_common_race_metadata(_read_doc(path), rno=rno))
    return out


def parse_raceresult(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    doc = _read_doc(path)
    tables = doc.xpath("//table")
    out: dict[str, Any] = {}
    if len(tables) >= 2:
        finish_order: list[int] = []
        for tr in tables[1].xpath(".//tr"):
            cells = [_text(c) for c in tr.xpath("./th|./td")]
            if len(cells) < 2:
                continue
            finish = _norm_boat(cells[0])
            boat = _norm_boat(cells[1])
            if finish is not None and boat is not None:
                out[f"finish_pos_{boat}"] = finish
                finish_order.append(boat)
        for idx, boat in enumerate(finish_order[:6], start=1):
            out[f"target_pos{idx}"] = boat - 1
        if finish_order:
            out["winner"] = finish_order[0]
    if len(tables) >= 3:
        for tr in tables[2].xpath(".//tr"):
            text = _text(tr)
            m = re.match(r"^([1-6])\s+(F?\.?\d+)", text)
            if m:
                boat = int(m.group(1))
                out[f"result_st_{boat}"] = _to_float(m.group(2).replace("F", ""))
    if len(tables) >= 4:
        rows = [[_text(c) for c in tr.xpath("./th|./td")] for tr in tables[3].xpath(".//tr")]
        for row in rows:
            if len(row) >= 4 and row[0] == "3連単":
                out["trifecta"] = row[1].replace(" ", "")
                out["payout_3t"] = _to_int(row[2])
                out["popularity_3t"] = _to_int(row[3])
                break
    if len(tables) >= 6:
        out["winning_method"] = _text(tables[5])
    return out


def add_engineered_features(row: dict[str, Any]) -> dict[str, Any]:
    for name in ["ex_time", "avg_st", "national_win_rate", "local_win_rate", "motor_quinella_rate", "boat_quinella_rate"]:
        vals = [row.get(f"{name}_{i}") for i in range(1, 7)]
        nums = [float(v) for v in vals if v is not None and pd.notna(v)]
        if not nums:
            continue
        mean = sum(nums) / len(nums)
        ordered = sorted((float(v), i) for i, v in enumerate(vals, start=1) if v is not None and pd.notna(v))
        rank = {boat: idx for idx, (_, boat) in enumerate(ordered, start=1)}
        reverse_rank = {boat: idx for idx, (_, boat) in enumerate(reversed(ordered), start=1)}
        for i, v in enumerate(vals, start=1):
            if v is None or pd.isna(v):
                continue
            value = float(v)
            row[f"{name}_diff_{i}"] = value - mean
            row[f"{name}_rank_low_{i}"] = rank.get(i)
            row[f"{name}_rank_high_{i}"] = reverse_rank.get(i)
    row["upset"] = int(row.get("winner") not in (None, 1))
    payout = row.get("payout_3t")
    if payout is not None:
        row["is_high_payout_5000"] = int(payout >= 5000)
        row["is_high_payout_10000"] = int(payout >= 10000)
        row["is_high_payout_30000"] = int(payout >= 30000)
    return row


def parse_race(race: RacePath) -> dict[str, Any] | None:
    result = parse_raceresult(race.page("raceresult"))
    if "winner" not in result:
        return None
    row: dict[str, Any] = {
        "date": race.date,
        "year": int(race.date[:4]),
        "month": int(race.date[4:6]),
        "jcd": int(race.jcd),
        "rno": race.rno,
    }
    row.update(parse_racelist(race.page("racelist"), rno=race.rno))
    row.update(parse_beforeinfo(race.page("beforeinfo")))
    enrich_race(row, race.date)
    row.update(result)
    return add_engineered_features(row)


def build_official_dataset(
    start_date: str | None = None,
    end_date: str | None = None,
    official_root: Path | None = None,
    progress_every: int = 5000,
    max_races: int | None = None,
) -> pd.DataFrame:
    races = iter_race_paths(official_root=official_root, start_date=start_date, end_date=end_date)
    rows: list[dict[str, Any]] = []
    for idx, race in enumerate(races, start=1):
        parsed = parse_race(race)
        if parsed is not None:
            rows.append(parsed)
        if progress_every and idx % progress_every == 0:
            print(f"parsed race_dirs={idx} rows={len(rows)} latest={race.date} jcd={race.jcd} rno={race.rno}", flush=True)
        if max_races is not None and idx >= max_races:
            break
    return pd.DataFrame(rows)
