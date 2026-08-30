import argparse
import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path

import lhafile
import requests

BASE_URL = "https://www1.mbrace.or.jp/od2"


def daterange(start, end):
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)


def download_one(base_dir: Path, current_date: datetime, file_type: str):
    date_str = current_date.strftime("%Y%m%d")
    date_short = date_str[2:]
    year_month = date_str[:6]
    file_name = f"{file_type}{date_short}.lzh"
    url = f"{BASE_URL}/{file_type.upper()}/{year_month}/{file_name}"
    lzh_dir = base_dir / "lzh"
    txt_dir = base_dir / "txt"
    lzh_dir.mkdir(parents=True, exist_ok=True)
    txt_dir.mkdir(parents=True, exist_ok=True)
    save_path = lzh_dir / file_name
    txt_path = txt_dir / f"{file_type.upper()}{date_short}.TXT"

    if txt_path.exists() and txt_path.stat().st_size > 0:
        return {"date": date_str, "type": file_type.upper(), "status": "exists", "txt": str(txt_path)}

    try:
        res = requests.get(url, timeout=20)
    except Exception as exc:
        return {"date": date_str, "type": file_type.upper(), "status": "error", "error": str(exc), "url": url}

    if res.status_code == 404:
        return {"date": date_str, "type": file_type.upper(), "status": "missing", "url": url}
    if res.status_code != 200:
        return {"date": date_str, "type": file_type.upper(), "status": "http_error", "code": res.status_code, "url": url}

    save_path.write_bytes(res.content)
    try:
        archive = lhafile.Lhafile(str(save_path))
        members = archive.infolist()
        if not members:
            return {"date": date_str, "type": file_type.upper(), "status": "empty_archive", "url": url}
        data = archive.read(members[0].filename)
        txt_path.write_bytes(data)
    except Exception as exc:
        return {"date": date_str, "type": file_type.upper(), "status": "extract_error", "error": str(exc), "url": url}

    return {"date": date_str, "type": file_type.upper(), "status": "success", "bytes": txt_path.stat().st_size, "url": url}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-dir", required=True)
    parser.add_argument("--start", default="20260201")
    parser.add_argument("--end", default="20260714")
    parser.add_argument("--workers", type=int, default=4)
    args = parser.parse_args()

    base_dir = Path(args.base_dir)
    start = datetime.strptime(args.start, "%Y%m%d")
    end = datetime.strptime(args.end, "%Y%m%d")
    tasks = [(base_dir, d, ft) for d in daterange(start, end) for ft in ("b", "k")]
    results = []
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = [pool.submit(download_one, *task) for task in tasks]
        for idx, future in enumerate(as_completed(futures), 1):
            result = future.result()
            results.append(result)
            if result["status"] in {"success", "error", "http_error", "extract_error", "missing"}:
                print(f"{idx:03d}/{len(tasks)} {result['date']} {result['type']} {result['status']}", flush=True)
            time.sleep(0.05)

    summary = {}
    for result in results:
        summary[result["status"]] = summary.get(result["status"], 0) + 1
    report_path = base_dir / "download_20260201_20260714_report.json"
    report_path.write_text(json.dumps({"summary": summary, "results": sorted(results, key=lambda r: (r["date"], r["type"]))}, ensure_ascii=False, indent=2), encoding="utf-8")
    print("SUMMARY", json.dumps(summary, ensure_ascii=False, sort_keys=True))
    print("REPORT", report_path)


if __name__ == "__main__":
    main()
