from __future__ import annotations

import argparse
from pathlib import Path

from bortrace.official_parser import build_official_dataset
from bortrace.paths import OUTPUT_DIR


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-date", default=None)
    parser.add_argument("--end-date", default=None)
    parser.add_argument("--out", default=str(OUTPUT_DIR / "official_race_dataset.csv"))
    parser.add_argument("--progress-every", type=int, default=5000)
    parser.add_argument("--max-races", type=int, default=None)
    args = parser.parse_args()

    df = build_official_dataset(
        start_date=args.start_date,
        end_date=args.end_date,
        progress_every=args.progress_every,
        max_races=args.max_races,
    )
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out, index=False, encoding="utf-8")
    print(f"rows={len(df)} cols={len(df.columns)}")
    print(f"out={out}")
    if not df.empty:
        print(df.groupby("year").size().to_string())


if __name__ == "__main__":
    main()
