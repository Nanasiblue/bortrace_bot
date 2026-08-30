from __future__ import annotations

import argparse
import subprocess
import urllib.request
from pathlib import Path

from bortrace.paths import LEGACY_DATA_ROOT


URL = "https://www.boatrace.jp/static_extra/pc_static/download/data/kibetsu/fan{key}.lzh"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-year", type=int, default=2020)
    parser.add_argument("--end-year", type=int, default=2026)
    parser.add_argument("--out-dir", default=str(LEGACY_DATA_ROOT / "kibetsu"))
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()
    out = Path(args.out_dir)
    out.mkdir(parents=True, exist_ok=True)
    keys = [f"{year % 100:02d}{half}" for year in range(args.start_year, args.end_year + 1) for half in ("04", "10")]
    # The current year's October file does not exist until autumn; skip unavailable files.
    for key in keys:
        txt = out / f"fan{key}.txt"
        archive = out / f"fan{key}.lzh"
        if txt.exists() and not args.force:
            print(f"exists {txt.name}")
            continue
        try:
            print(f"download fan{key}.lzh", flush=True)
            data = urllib.request.urlopen(URL.format(key=key), timeout=60).read()
            archive.write_bytes(data)
            subprocess.run(["tar", "-xf", str(archive), "-C", str(out)], check=True)
            print(f"ready {txt.name} bytes={txt.stat().st_size}")
        except Exception as exc:
            print(f"skip fan{key}: {exc}")


if __name__ == "__main__":
    main()
