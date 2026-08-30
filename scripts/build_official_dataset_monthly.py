from __future__ import annotations

import argparse
import gc
from datetime import date
from pathlib import Path

import pandas as pd

from bortrace.official_parser import build_official_dataset
from bortrace.paths import OUTPUT_DIR


def iter_months(start_yyyymm: str, end_yyyymm: str):
    year = int(start_yyyymm[:4])
    month = int(start_yyyymm[4:6])
    end_year = int(end_yyyymm[:4])
    end_month = int(end_yyyymm[4:6])
    while (year, month) <= (end_year, end_month):
        start = date(year, month, 1)
        if month == 12:
            end = date(year, 12, 31)
            next_year, next_month = year + 1, 1
        else:
            end = date(year, month + 1, 1) - pd.Timedelta(days=1)
            next_year, next_month = year, month + 1
        yield start.strftime("%Y%m%d"), end.strftime("%Y%m%d"), f"{year}{month:02d}"
        year, month = next_year, next_month


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-month", default="202101")
    parser.add_argument("--end-month", default="202607")
    parser.add_argument("--out-dir", default=str(OUTPUT_DIR / "official_dataset_parts"))
    parser.add_argument("--progress-every", type=int, default=1000)
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    total_rows = 0
    written: list[Path] = []
    for start_date, end_date, yyyymm in iter_months(args.start_month, args.end_month):
        out = out_dir / f"official_race_dataset_{yyyymm}.csv"
        if out.exists() and out.stat().st_size > 0 and not args.force:
            print(f"skip existing {yyyymm}: {out}", flush=True)
            written.append(out)
            continue

        print(f"start {yyyymm} {start_date}-{end_date}", flush=True)
        df = build_official_dataset(
            start_date=start_date,
            end_date=end_date,
            progress_every=args.progress_every,
        )
        df.to_csv(out, index=False, encoding="utf-8")
        total_rows += len(df)
        written.append(out)
        print(f"done {yyyymm} rows={len(df)} cols={len(df.columns)} out={out}", flush=True)
        del df
        gc.collect()

    manifest = out_dir / "manifest.txt"
    manifest.write_text("\n".join(str(p) for p in written) + "\n", encoding="utf-8")
    print(f"monthly_done files={len(written)} new_rows={total_rows}", flush=True)
    print(f"manifest={manifest}", flush=True)


if __name__ == "__main__":
    main()
