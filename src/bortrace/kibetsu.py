from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Any

from .paths import LEGACY_DATA_ROOT


CLASS_VALUE = {"A1": 4, "A2": 3, "B1": 2, "B2": 1}


def period_key(date: str) -> str:
    year, month = int(date[:4]), int(date[4:6])
    if month <= 4:
        return f"{year - 1:04d}10"[2:]
    if month <= 10:
        return f"{year:04d}04"[2:]
    return f"{year:04d}10"[2:]


def _integer(raw: bytes) -> int | None:
    text = raw.decode("ascii", errors="ignore").strip()
    return int(text) if text.isdigit() else None


def _scaled(raw: bytes, scale: float) -> float | None:
    value = _integer(raw)
    return None if value is None else value / scale


def parse_line(line: bytes) -> dict[str, Any] | None:
    line = line.rstrip(b"\r\n")
    if len(line) < 416:
        return None
    reg_no = _integer(line[0:4])
    if reg_no is None:
        return None
    out: dict[str, Any] = {
        "reg_no": reg_no,
        "class_val_master": CLASS_VALUE.get(line[39:41].decode("ascii", errors="ignore")),
        "gender_val": _integer(line[48:49]),
        "height": _integer(line[51:54]),
        "ability_index_previous": _scaled(line[166:170], 100.0),
        "ability_index": _scaled(line[170:174], 100.0),
        "training_term": _integer(line[195:198]),
    }
    for idx, start in enumerate((160, 162, 164), start=1):
        out[f"class_val_lag{idx}"] = CLASS_VALUE.get(line[start:start + 2].decode("ascii", errors="ignore"))

    offset = 82
    for course in range(1, 7):
        out[f"course_{course}_entries"] = _integer(line[offset:offset + 3])
        out[f"course_{course}_quinella_rate"] = _scaled(line[offset + 3:offset + 7], 10.0)
        out[f"course_{course}_avg_st"] = _scaled(line[offset + 7:offset + 10], 100.0)
        out[f"course_{course}_avg_st_rank"] = _scaled(line[offset + 10:offset + 13], 100.0)
        offset += 13

    offset = 198
    for course in range(1, 7):
        finishes = [_integer(line[offset + i * 3:offset + (i + 1) * 3]) or 0 for i in range(6)]
        offset += 18
        incidents = [_integer(line[offset + i * 2:offset + (i + 1) * 2]) or 0 for i in range(8)]
        offset += 16
        total = sum(finishes)
        out[f"course_{course}_win_rate"] = 100.0 * finishes[0] / total if total else None
        out[f"course_{course}_top3_rate"] = 100.0 * sum(finishes[:3]) / total if total else None
        out[f"course_{course}_incident_rate"] = 100.0 * sum(incidents) / max(total + sum(incidents), 1)
    return out


@lru_cache(maxsize=16)
def load_period(key: str, root: str | None = None) -> dict[int, dict[str, Any]]:
    base = Path(root) if root else LEGACY_DATA_ROOT / "kibetsu"
    path = base / f"fan{key}.txt"
    if not path.exists():
        return {}
    parsed = (parse_line(line) for line in path.read_bytes().splitlines())
    return {row["reg_no"]: row for row in parsed if row is not None}


def enrich_race(row: dict[str, Any], date: str, root: str | None = None) -> None:
    master = load_period(period_key(date), root)
    if not master:
        return
    female_count = 0
    known_gender = 0
    for boat in range(1, 7):
        racer = master.get(row.get(f"reg_no_{boat}"))
        if not racer:
            continue
        for name in (
            "gender_val", "height", "ability_index", "ability_index_previous",
            "training_term", "class_val_lag1", "class_val_lag2", "class_val_lag3",
        ):
            row[f"{name}_{boat}"] = racer.get(name)
        gender = racer.get("gender_val")
        if gender in (1, 2):
            known_gender += 1
            row[f"is_female_{boat}"] = int(gender == 2)
            female_count += int(gender == 2)
        course = row.get(f"start_ex_course_{boat}") or boat
        try:
            course = int(course)
        except (TypeError, ValueError):
            course = boat
        for name in (
            "entries", "quinella_rate", "avg_st", "avg_st_rank",
            "win_rate", "top3_rate", "incident_rate",
        ):
            row[f"historical_course_{name}_{boat}"] = racer.get(f"course_{course}_{name}")
    if known_gender:
        row["female_count"] = female_count
        row["is_all_female"] = int(female_count == 6 and known_gender == 6)
        row["is_mixed_gender"] = int(0 < female_count < known_gender)
