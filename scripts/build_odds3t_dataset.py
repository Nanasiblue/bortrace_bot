from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd
from lxml import html

from bortrace.paths import LEGACY_DATA_ROOT, OUTPUT_DIR


def iter_odds_files(start_date: str | None, end_date: str | None):
    root = LEGACY_DATA_ROOT / "official_pages"
    for date_dir in sorted(p for p in root.iterdir() if p.is_dir() and p.name.isdigit()):
        date = date_dir.name
        if start_date and date < start_date:
            continue
        if end_date and date > end_date:
            continue
        for jcd_dir in sorted(p for p in date_dir.iterdir() if p.is_dir() and p.name.isdigit()):
            for race_dir in sorted((p for p in jcd_dir.iterdir() if p.is_dir() and p.name.isdigit()), key=lambda p: int(p.name)):
                path = race_dir / "odds3t.html"
                if path.exists() and path.stat().st_size > 1000:
                    yield date, jcd_dir.name, int(race_dir.name), path


def _to_int(text: str) -> int | None:
    text = text.strip()
    if not text.isdigit():
        return None
    return int(text)


def _to_float(text: str) -> float | None:
    text = text.strip().replace(",", "")
    if not text or text in {"-", "--"}:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def parse_odds3t_fast(path: Path) -> list[dict[str, float | int | str]]:
    try:
        doc = html.fromstring(path.read_bytes())
    except Exception:
        return []

    tables = doc.xpath("//div[contains(@class, 'table1')]/table[.//td[contains(@class, 'oddsPoint')]]")
    if not tables:
        return []
    table = tables[0]
    first_boats: list[int] = []
    for th in table.xpath(".//thead//th"):
        classes = th.get("class", "")
        text = "".join(th.itertext()).strip()
        boat = _to_int(text)
        if boat is not None and "is-boatColor" in classes:
            first_boats.append(boat)
    first_boats = first_boats[:6]
    if len(first_boats) != 6:
        first_boats = [1, 2, 3, 4, 5, 6]

    rows: list[dict[str, float | int | str]] = []
    current_second: list[int | None] = [None] * 6
    for tr in table.xpath(".//tbody/tr"):
        cells = tr.xpath("./td")
        cell_index = 0
        for group_index, first in enumerate(first_boats):
            if cell_index >= len(cells):
                break
            second = current_second[group_index]
            first_cell = cells[cell_index]
            first_text = "".join(first_cell.itertext()).strip()
            if first_cell.get("rowspan"):
                maybe_second = _to_int(first_text)
                if maybe_second is None:
                    break
                second = maybe_second
                current_second[group_index] = second
                cell_index += 1
            if second is None or cell_index + 1 >= len(cells):
                break
            third = _to_int("".join(cells[cell_index].itertext()))
            odds = _to_float("".join(cells[cell_index + 1].itertext()))
            cell_index += 2
            if third is None or odds is None:
                continue
            if len({first, second, third}) != 3:
                continue
            rows.append(
                {
                    "combination": f"{first}-{second}-{third}",
                    "first": first,
                    "second": second,
                    "third": third,
                    "odds": odds,
                }
            )
    return rows


def parse_odds3t(path: Path) -> list[dict[str, float | int | str]]:
    fast_rows = parse_odds3t_fast(path)
    if fast_rows:
        return fast_rows
    try:
        tables = pd.read_html(path)
    except Exception:
        return []
    if len(tables) < 2:
        return []
    df = tables[1]
    rows: list[dict[str, float | int | str]] = []
    for start_col in range(0, len(df.columns), 3):
        if start_col + 2 >= len(df.columns):
            continue
        try:
            first = int(str(df.columns[start_col]).split(".")[0])
        except ValueError:
            continue
        if first < 1 or first > 6:
            continue
        for _, row in df.iterrows():
            try:
                second = int(row.iloc[start_col])
                third = int(row.iloc[start_col + 1])
                odds = float(row.iloc[start_col + 2])
            except (TypeError, ValueError):
                continue
            if len({first, second, third}) != 3:
                continue
            rows.append(
                {
                    "combination": f"{first}-{second}-{third}",
                    "first": first,
                    "second": second,
                    "third": third,
                    "odds": odds,
                }
            )
    return rows


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-date", default=None)
    parser.add_argument("--end-date", default=None)
    parser.add_argument("--out", default=str(OUTPUT_DIR / "odds3t_dataset.csv"))
    parser.add_argument("--progress-every", type=int, default=5000)
    args = parser.parse_args()

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    first_write = True
    race_count = 0
    row_count = 0
    for date, jcd, rno, path in iter_odds_files(args.start_date, args.end_date):
        parsed = parse_odds3t(path)
        race_count += 1
        if parsed:
            for item in parsed:
                item["date"] = date
                item["jcd"] = int(jcd)
                item["rno"] = rno
            frame = pd.DataFrame(parsed)
            frame.to_csv(out, mode="w" if first_write else "a", header=first_write, index=False, encoding="utf-8")
            first_write = False
            row_count += len(frame)
        if args.progress_every and race_count % args.progress_every == 0:
            print(f"parsed races={race_count} odds_rows={row_count} latest={date} jcd={jcd} rno={rno}", flush=True)
    print(f"races={race_count} odds_rows={row_count} out={out}", flush=True)


if __name__ == "__main__":
    main()
