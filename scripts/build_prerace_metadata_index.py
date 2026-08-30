from __future__ import annotations

import argparse
import json
import re
import subprocess
from html import unescape
from pathlib import Path
from typing import Any, Iterator

import pandas as pd

from bortrace.official_parser import GRADE_VALUE
from bortrace.paths import LEGACY_DATA_ROOT


TAG = re.compile(r"<[^>]+>")


def text(fragment: str) -> str:
    return " ".join(unescape(TAG.sub(" ", fragment)).split())


def key_from_path(value: str) -> tuple[str, int, int]:
    parts = Path(value).parts
    idx = parts.index("official_pages")
    return parts[idx + 1], int(parts[idx + 2]), int(parts[idx + 3])


def matches(root: Path, pattern: str) -> Iterator[tuple[tuple[str, int, int], str]]:
    command = ["rg", "--json", "--pcre2", "-U", "-o", "-g", "beforeinfo.html", pattern, str(root)]
    process = subprocess.Popen(command, stdout=subprocess.PIPE, text=True, encoding="utf-8", errors="replace")
    assert process.stdout is not None
    for line in process.stdout:
        item = json.loads(line)
        if item.get("type") != "match":
            continue
        data = item["data"]
        yield key_from_path(data["path"]["text"]), data["lines"]["text"]
    code = process.wait()
    if code not in (0, 1):
        raise subprocess.CalledProcessError(code, command)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", default=str(LEGACY_DATA_ROOT / "official_pages"))
    parser.add_argument("--out", required=True)
    args = parser.parse_args()
    root = Path(args.root)
    rows: dict[tuple[str, int, int], dict[str, Any]] = {}

    blocks = [
        r'<div class="heading2_title[^>]*>.*?</div>',
        r'<li class="is-active2">.*?</li>',
        r'<h3 class="title16_titleDetail__add2020">\s*[^<]+',
        r'<span class="label2[^>]*>(?:安定板使用|進入固定)</span>',
        r'<div class="table1 h-mt10">.*?</table>',
        r'<p class="weather1_title">[^<]*',
        r'weather1_bodyUnitLabelTitle">(?:気温|水温|風速|波高)</span>\s*<span class="weather1_bodyUnitLabelData">[^<]*',
        r'is-weather\d+.*?weather1_bodyUnitLabelTitle">[^<]+',
        r'is-windDirection.*?is-wind\d+',
        r'weather1_bodyUnit is-direction.*?is-direction\d+',
    ]
    # One directory walk is much faster than five walks on the large archive.
    patterns = {"all": "|".join(f"(?:{block})" for block in blocks)}
    for kind, pattern in patterns.items():
        count = 0
        for key, fragment in matches(root, pattern):
            row = rows.setdefault(key, {"date": key[0], "jcd": key[1], "rno": key[2]})
            current_kind = kind
            if kind == "all":
                if "heading2_title" in fragment:
                    current_kind = "heading"
                elif "is-active2" in fragment:
                    current_kind = "day"
                elif "title16_titleDetail" in fragment or "label2" in fragment:
                    current_kind = "detail"
                elif "table1 h-mt10" in fragment:
                    current_kind = "schedule"
                else:
                    current_kind = "weather"
            if current_kind == "heading":
                title = re.search(r'heading2_titleName[^>]*>([^<]*)', fragment)
                if title:
                    value = text(title.group(1))
                    row["event_title"] = value
                    row["is_womens_event"] = int(any(x in value for x in ("女子", "レディース", "ヴィーナス")))
                    row["is_rookie_event"] = int("ルーキー" in value)
                    row["is_senior_event"] = int("マスターズ" in value)
                token = re.search(r'heading2_title\s+(is-[A-Za-z0-9_]+)', fragment)
                grade_token = token.group(1)[3:].upper() if token else ""
                grade = next((x for x in ("SG", "PG1", "G1", "G2", "G3") if grade_token.startswith(x)), "ippan")
                row["grade_val"] = GRADE_VALUE[grade]
            elif current_kind == "day":
                value = text(fragment)
                row["is_final_day"] = int("最終日" in value)
                if "初日" in value:
                    row["event_day"] = 1
                else:
                    found = re.search(r"(\d+)日目", value.translate(str.maketrans("１２３４５６７８９０", "1234567890")))
                    if found:
                        row["event_day"] = int(found.group(1))
            elif current_kind == "detail":
                value = text(fragment)
                if "title16_titleDetail" in fragment:
                    distance = re.search(r"(\d{3,4})m", value)
                    if distance:
                        row["race_distance"] = int(distance.group(1))
                    stage = re.sub(r"\d{3,4}m.*", "", value).strip()
                    row["is_qualifying"] = int("予選" in stage)
                    row["is_semifinal"] = int("準優" in stage)
                    row["is_final"] = int("優勝" in stage)
                    row["is_selection"] = int("選抜" in stage)
                    row["is_special_selection"] = int(any(x in stage for x in ("特選", "特賞", "ドリーム")))
                    row["is_general_race"] = int("一般" in stage)
                if "進入固定" in value:
                    row["is_fixed_entry"] = 1
                if "安定板" in value:
                    row["uses_stabilizer"] = 1
            elif current_kind == "schedule":
                times = re.findall(r"\b(\d{1,2}):(\d{2})\b", text(fragment))
                if len(times) >= key[2]:
                    hour, minute = map(int, times[key[2] - 1])
                    row["deadline_hour"] = hour
                    row["deadline_minute"] = minute
                    row["deadline_minutes"] = hour * 60 + minute
            elif current_kind == "weather":
                value = text(fragment)
                specs = {
                    "air_temperature": (r"気温\s*([\d.]+)℃", float),
                    "water_temperature": (r"水温\s*([\d.]+)℃", float),
                    "wind_speed": (r"風速\s*([\d.]+)m", float),
                    "wave_height": (r"波高\s*([\d.]+)cm", float),
                    "weather_asof_rno": (r"(\d+)R時点", int),
                }
                for name, (regex, cast) in specs.items():
                    found = re.search(regex, value)
                    if found:
                        row[name] = cast(found.group(1))
                weather = re.search(r'is-weather(\d+).*?weather1_bodyUnitLabelTitle">([^<]+)', fragment, re.S)
                if weather:
                    row["weather_code"] = int(weather.group(1))
                wind = re.search(r'is-windDirection.*?is-wind(\d+)', fragment, re.S)
                if wind:
                    row["wind_direction_code"] = int(wind.group(1))
                direction = re.findall(r'is-direction(\d+)', fragment, re.S)
                if direction:
                    row["course_direction_code"] = int(direction[-1])
            count += 1
        print(f"{kind} matches={count} races={len(rows)}", flush=True)

    frame = pd.DataFrame(rows.values())
    for name in ("is_fixed_entry", "uses_stabilizer"):
        if name in frame:
            frame[name] = frame[name].fillna(0).astype(int)
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(out, index=False, encoding="utf-8")
    print(f"rows={len(frame)} cols={len(frame.columns)} out={out}")


if __name__ == "__main__":
    main()
