from __future__ import annotations

import argparse
import json
import shutil
import sys
import time
import urllib.request
from datetime import datetime
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from bortrace.official_parser import RacePath, parse_beforeinfo, parse_race
from bortrace.paths import LEGACY_DATA_ROOT

BASE_URL = "https://www.boatrace.jp/owpc/pc/race/beforeinfo"
ERROR_MARKERS = ("システムエラー", "予期せぬエラー", "お探しのページは見つかりません", "Not Found", "Service Unavailable")
START_COLS = [f"start_ex_{kind}_{boat}" for kind in ("course", "st_by_course") for boat in range(1, 7)]
EX_COLS = [f"ex_time_{boat}" for boat in range(1, 7)]
BEFOREINFO_PREFIXES = ("weight_", "ex_time_", "tilt_", "start_ex_")


def load_candidates(dataset_dir: Path, start_month: str, end_month: str, include_partial: bool) -> pd.DataFrame:
    rows: list[pd.DataFrame] = []
    wanted = ["date", "jcd", "rno", *START_COLS, *EX_COLS]
    for path in sorted(dataset_dir.glob("official_race_dataset_*.csv")):
        month = path.stem.rsplit("_", 1)[-1]
        if not (start_month <= month <= end_month) or path.stat().st_size <= 1000:
            continue
        frame = pd.read_csv(path, usecols=lambda col: col in wanted)
        start_missing = frame[START_COLS].isna()
        ex_missing = frame[EX_COLS].isna()
        if include_partial:
            mask = start_missing.any(axis=1) | ex_missing.any(axis=1)
        else:
            course_all = frame[[f"start_ex_course_{b}" for b in range(1, 7)]].isna().all(axis=1)
            st_all = frame[[f"start_ex_st_by_course_{b}" for b in range(1, 7)]].isna().all(axis=1)
            mask = course_all | st_all
        selected = frame.loc[mask, ["date", "jcd", "rno"]].copy()
        selected["month"] = month
        selected["dataset_path"] = str(path)
        selected["reason"] = "partial_exhibition_missing" if include_partial else "all6_start_ex_missing"
        rows.append(selected)
    if not rows:
        return pd.DataFrame(columns=["date", "jcd", "rno", "month", "dataset_path", "reason"])
    result = pd.concat(rows, ignore_index=True).drop_duplicates(["date", "jcd", "rno"])
    result["date"] = result["date"].astype(str).str.replace(r"\.0$", "", regex=True).str.zfill(8)
    result["jcd"] = pd.to_numeric(result["jcd"], errors="raise").astype(int)
    result["rno"] = pd.to_numeric(result["rno"], errors="raise").astype(int)
    return result.sort_values(["date", "jcd", "rno"]).reset_index(drop=True)


def valid_html(content: bytes) -> bool:
    if len(content) < 1000:
        return False
    text = content.decode("utf-8", errors="ignore")
    return not any(marker in text for marker in ERROR_MARKERS)


def completeness(values: dict) -> int:
    return sum(pd.notna(values.get(col)) for col in [*START_COLS, *EX_COLS])


def patch_month_csv(path: Path, race: RacePath, parsed: dict) -> bool:
    frame = pd.read_csv(path)
    date = pd.to_numeric(frame["date"], errors="coerce").astype("Int64")
    mask = (date == int(race.date)) & (pd.to_numeric(frame["jcd"], errors="coerce") == int(race.jcd)) & (pd.to_numeric(frame["rno"], errors="coerce") == race.rno)
    indexes = frame.index[mask]
    if len(indexes) != 1:
        return False
    idx = indexes[0]
    clear_cols = [col for col in frame.columns if col.startswith(BEFOREINFO_PREFIXES) or col.startswith("ex_time_diff_") or col.startswith("ex_time_rank_")]
    frame.loc[idx, clear_cols] = pd.NA
    for col, value in parsed.items():
        if col not in frame.columns:
            frame[col] = pd.NA
        frame.at[idx, col] = value
    tmp = path.with_suffix(path.suffix + ".tmp")
    frame.to_csv(tmp, index=False, encoding="utf-8")
    tmp.replace(path)
    return True


def main() -> None:
    parser = argparse.ArgumentParser(description="Selectively refetch official beforeinfo pages with recoverable exhibition-data gaps.")
    parser.add_argument("--dataset-dir", default="outputs/official_dataset_parts")
    parser.add_argument("--official-root", default=str(LEGACY_DATA_ROOT / "official_pages"))
    parser.add_argument("--start-month", default="202101")
    parser.add_argument("--end-month", default="202607")
    parser.add_argument("--include-partial", action="store_true", help="Also retry races with only one or more missing exhibition cells.")
    parser.add_argument("--apply", action="store_true", help="Actually download. Without this flag only a candidate report is written.")
    parser.add_argument("--patch-dataset", action="store_true", help="Patch the affected monthly CSV row after a verified improvement.")
    parser.add_argument("--max-races", type=int, default=None)
    parser.add_argument("--sleep", type=float, default=0.5)
    parser.add_argument("--timeout", type=float, default=30.0)
    parser.add_argument("--retries", type=int, default=2)
    parser.add_argument("--report", default="outputs/refetch_missing_beforeinfo_report.csv")
    args = parser.parse_args()

    candidates = load_candidates(Path(args.dataset_dir), args.start_month, args.end_month, args.include_partial)
    if args.max_races is not None:
        candidates = candidates.head(args.max_races)
    print(f"candidates={len(candidates)} apply={args.apply} include_partial={args.include_partial}", flush=True)
    results: list[dict] = []
    official_root = Path(args.official_root)
    backup_root = official_root.parent / f"beforeinfo_backup_{datetime.now():%Y%m%d_%H%M%S}"

    for number, row in enumerate(candidates.itertuples(index=False), 1):
        date, jcd, rno = row.date, f"{int(row.jcd):02d}", int(row.rno)
        race = RacePath(date=date, jcd=jcd, rno=rno, root=official_root / date / jcd / str(rno))
        page = race.page("beforeinfo")
        url = f"{BASE_URL}?hd={date}&jcd={jcd}&rno={rno}"
        result = {"date": date, "jcd": jcd, "rno": rno, "reason": row.reason, "url": url, "path": str(page)}
        if not args.apply:
            result["status"] = "dry_run"
            results.append(result)
            continue

        old_values = parse_beforeinfo(page) if page.exists() else {}
        old_score = completeness(old_values)
        content = None
        error = ""
        for attempt in range(args.retries + 1):
            try:
                request = urllib.request.Request(
                    url,
                    headers={"User-Agent": "Mozilla/5.0 (compatible; bortrace-repair/1.0; selective-retry)"},
                )
                with urllib.request.urlopen(request, timeout=args.timeout) as response:
                    content = response.read()
                if not valid_html(content):
                    raise ValueError("invalid or weak HTML")
                break
            except Exception as exc:
                error = str(exc)
                content = None
                if attempt < args.retries:
                    time.sleep(min(8.0, 1.5 * (2**attempt)))
        if content is None:
            result.update(status="download_error", message=error, old_score=old_score)
            results.append(result)
            continue

        page.parent.mkdir(parents=True, exist_ok=True)
        backup = backup_root / date / jcd / str(rno) / "beforeinfo.html"
        if page.exists():
            backup.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(page, backup)
        tmp = page.with_suffix(".html.tmp")
        tmp.write_bytes(content)
        tmp.replace(page)
        try:
            new_values = parse_beforeinfo(page)
            new_score = completeness(new_values)
        except Exception as exc:
            new_score = -1
            error = f"parse_error: {exc}"

        if new_score <= old_score:
            if backup.exists():
                shutil.copy2(backup, page)
            elif page.exists():
                page.unlink()
            result.update(status="not_improved_restored", message=error, old_score=old_score, new_score=new_score)
        else:
            patched = False
            if args.patch_dataset:
                parsed = parse_race(race)
                if parsed is not None:
                    patched = patch_month_csv(Path(row.dataset_path), race, parsed)
            result.update(status="improved", old_score=old_score, new_score=new_score, dataset_patched=patched)
        results.append(result)
        print(f"{number}/{len(candidates)} {date} {jcd} {rno} {result['status']} {old_score}->{result.get('new_score', '-')}", flush=True)
        time.sleep(max(0.0, args.sleep))

    report = Path(args.report)
    report.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(results).to_csv(report, index=False, encoding="utf-8")
    summary = pd.Series([row["status"] for row in results]).value_counts().to_dict() if results else {}
    metadata = report.with_suffix(".json")
    metadata.write_text(json.dumps({"candidates": len(candidates), "summary": summary, "report": str(report), "backup_root": str(backup_root) if args.apply else None}, ensure_ascii=False, indent=2), encoding="utf-8")
    print("summary=" + json.dumps(summary, ensure_ascii=False), flush=True)
    print(f"report={report}", flush=True)


if __name__ == "__main__":
    main()
