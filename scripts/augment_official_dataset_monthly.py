from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from bortrace.kibetsu import enrich_race
from bortrace.official_parser import parse_prerace_metadata
from bortrace.paths import LEGACY_DATA_ROOT


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--src-dir", required=True)
    parser.add_argument("--out-dir", required=True)
    parser.add_argument("--start-month", required=True)
    parser.add_argument("--end-month", required=True)
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--metadata-index")
    parser.add_argument("--skip-html", action="store_true")
    args = parser.parse_args()
    source = Path(args.src_dir)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    page_root = LEGACY_DATA_ROOT / "official_pages"
    metadata = None
    if args.metadata_index:
        metadata = pd.read_csv(args.metadata_index).set_index(["date", "jcd", "rno"])

    for src in sorted(source.glob("official_race_dataset_*.csv")):
        month = src.stem.rsplit("_", 1)[-1]
        if not (args.start_month <= month <= args.end_month):
            continue
        out = out_dir / src.name
        if out.exists() and out.stat().st_size > 1000 and not args.force:
            print(f"exists {month}", flush=True)
            continue
        frame = pd.read_csv(src)
        additions = []
        for index, record in enumerate(frame.itertuples(index=False), start=1):
            date = str(record.date)
            jcd = f"{int(record.jcd):02d}"
            rno = int(record.rno)
            if args.skip_html:
                values = {}
            elif metadata is not None and (int(date), int(jcd), rno) in metadata.index:
                values = metadata.loc[(int(date), int(jcd), rno)].dropna().to_dict()
            else:
                values = parse_prerace_metadata(page_root / date / jcd / str(rno) / "beforeinfo.html", rno)
            # Existing exhibition columns are retained from the proven old CSV.
            values = {k: v for k, v in values.items() if not any(k.startswith(prefix) for prefix in (
                "weight_", "ex_time_", "tilt_", "start_ex_course_", "start_ex_st_by_course_"
            ))}
            race = record._asdict()
            enrich_race(race, date)
            values.update({k: v for k, v in race.items() if k not in frame.columns})
            additions.append(values)
            if index % 1000 == 0:
                print(f"{month} {index}/{len(frame)}", flush=True)
        extra = pd.DataFrame(additions, index=frame.index)
        overlap = [x for x in extra.columns if x in frame.columns]
        if overlap:
            frame = frame.drop(columns=overlap)
        expanded = pd.concat([frame, extra], axis=1)
        tmp = out.with_suffix(".csv.tmp")
        expanded.to_csv(tmp, index=False, encoding="utf-8")
        tmp.replace(out)
        print(f"done {month} rows={len(expanded)} cols={len(expanded.columns)}", flush=True)


if __name__ == "__main__":
    main()
