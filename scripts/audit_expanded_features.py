from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


FEATURES = [
    "grade_val", "event_day", "event_days_total", "is_final_day",
    "race_distance", "is_qualifying", "is_semifinal", "is_final",
    "is_selection", "is_special_selection", "is_general_race",
    "is_womens_event", "is_rookie_event", "is_senior_event",
    "is_fixed_entry", "uses_stabilizer", "deadline_minutes",
    "weather_asof_rno", "weather_code", "air_temperature",
    "water_temperature", "wind_speed", "wind_direction_code",
    "course_direction_code", "wave_height",
]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset-dir", required=True)
    parser.add_argument("--out", required=True)
    args = parser.parse_args()

    files = sorted(Path(args.dataset_dir).glob("official_race_dataset_*.csv"))
    if not files:
        raise SystemExit("No monthly dataset files found")
    chunks = []
    for path in files:
        header = pd.read_csv(path, nrows=0).columns
        wanted = [x for x in ["date", *FEATURES] if x in header]
        chunks.append(pd.read_csv(path, usecols=wanted))
    frame = pd.concat(chunks, ignore_index=True, sort=False)
    rows = []
    for feature in FEATURES:
        if feature not in frame:
            rows.append({"feature": feature, "rows": len(frame), "present": False})
            continue
        values = frame[feature]
        rows.append({
            "feature": feature,
            "rows": len(frame),
            "present": True,
            "non_null": int(values.notna().sum()),
            "coverage": float(values.notna().mean()),
            "unique": int(values.nunique(dropna=True)),
            "min": pd.to_numeric(values, errors="coerce").min(),
            "max": pd.to_numeric(values, errors="coerce").max(),
        })
    result = pd.DataFrame(rows)
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    result.to_csv(out, index=False, encoding="utf-8")
    print(f"files={len(files)} rows={len(frame)}")
    print(result.to_string(index=False))
    print(f"out={out}")


if __name__ == "__main__":
    main()
