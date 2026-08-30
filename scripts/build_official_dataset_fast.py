from __future__ import annotations

import argparse
import concurrent.futures as futures
from pathlib import Path

import pandas as pd

from bortrace.official_parser import iter_race_paths, parse_race
from bortrace.paths import OUTPUT_DIR


def parse_one(args: tuple[str, str, int, str]) -> dict | None:
    date, jcd, rno, root = args
    from bortrace.official_parser import RacePath, parse_race

    return parse_race(RacePath(date=date, jcd=jcd, rno=rno, root=Path(root)))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--out", default=str(OUTPUT_DIR / "official_race_dataset.csv"))
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--progress-every", type=int, default=500)
    args = parser.parse_args()

    races = iter_race_paths(start_date=args.start_date, end_date=args.end_date)
    tasks = [(race.date, race.jcd, race.rno, str(race.root)) for race in races]
    rows: list[dict] = []

    with futures.ProcessPoolExecutor(max_workers=args.workers) as executor:
        for idx, row in enumerate(executor.map(parse_one, tasks, chunksize=50), start=1):
            if row is not None:
                rows.append(row)
            if args.progress_every and idx % args.progress_every == 0:
                race = races[idx - 1]
                print(
                    f"parsed race_dirs={idx}/{len(races)} rows={len(rows)} "
                    f"latest={race.date} jcd={race.jcd} rno={race.rno}",
                    flush=True,
                )

    df = pd.DataFrame(rows)
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out, index=False, encoding="utf-8")
    print(f"rows={len(df)} cols={len(df.columns)}")
    print(f"out={out}")
    if not df.empty:
        print(df.groupby("year").size().to_string())


if __name__ == "__main__":
    main()
