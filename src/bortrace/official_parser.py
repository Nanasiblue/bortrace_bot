from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd
from lxml import html

from .paths import LEGACY_DATA_ROOT


FULLWIDTH_DIGITS = str.maketrans("１２３４５６７８９０", "1234567890")
CLASS_VALUE = {"A1": 4, "A2": 3, "B1": 2, "B2": 1}


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
    return html.fromstring(path.read_bytes())


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


def parse_racelist(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    doc = _read_doc(path)
    tables = doc.xpath("//table")
    if len(tables) < 2:
        return {}
    out: dict[str, Any] = {}
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
    row.update(parse_racelist(race.page("racelist")))
    row.update(parse_beforeinfo(race.page("beforeinfo")))
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
