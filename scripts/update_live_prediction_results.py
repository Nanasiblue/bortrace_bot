from __future__ import annotations

import argparse
import csv
import json
import re
import time
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from lxml import html

try:
    import requests
except ModuleNotFoundError:
    requests = None


JST = timezone(timedelta(hours=9), "JST")
BASE_URL = "https://www.boatrace.jp/owpc/pc/race"
PROJECT_ROOT = Path(__file__).resolve().parents[1]
OUTPUT_DIR = PROJECT_ROOT / "outputs"
PREDICTION_DIR = OUTPUT_DIR / "live_predictions"
FEEDBACK_DIR = OUTPUT_DIR / "live_feedback"


def text_of(node: Any) -> str:
    return " ".join(" ".join(node.xpath(".//text()")).split())


def to_int_money(value: str | None) -> int | None:
    if not value:
        return None
    m = re.search(r"([\d,]+)", value)
    if not m:
        return None
    return int(m.group(1).replace(",", ""))


class OfficialResultClient:
    def __init__(self) -> None:
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
            "Accept-Language": "ja,en-US;q=0.9,en;q=0.8",
        }
        self.session = requests.Session() if requests else None
        if self.session:
            self.session.headers.update(self.headers)

    def get_doc(self, url: str):
        if self.session:
            res = self.session.get(url, timeout=20)
            res.raise_for_status()
            content = res.content
        else:
            req = urllib.request.Request(url, headers=self.headers)
            with urllib.request.urlopen(req, timeout=20) as res:
                content = res.read()
        return html.fromstring(content)

    def fetch_result(self, date: str, jcd: str, rno: int) -> dict[str, Any] | None:
        url = f"{BASE_URL}/raceresult?rno={rno}&jcd={jcd}&hd={date}"
        doc = self.get_doc(url)
        if "レース不成立" in text_of(doc) or "データがありません" in text_of(doc):
            return None
        tables = doc.xpath("//table")
        result: dict[str, Any] = {"result_url": url}
        if len(tables) >= 2:
            order: list[int] = []
            for tr in tables[1].xpath(".//tr"):
                cells = [text_of(c) for c in tr.xpath("./th|./td")]
                if len(cells) < 2:
                    continue
                if cells[0] in {"１", "２", "３", "４", "５", "６", "1", "2", "3", "4", "5", "6"}:
                    boat_text = cells[1].translate(str.maketrans("１２３４５６", "123456"))
                    if boat_text in {"1", "2", "3", "4", "5", "6"}:
                        order.append(int(boat_text))
            if not order:
                return None
            result["finish_order"] = order
            result["winner"] = order[0]
            result["upset_actual"] = int(order[0] != 1)
        if len(tables) >= 4:
            for tr in tables[3].xpath(".//tr"):
                cells = [text_of(c) for c in tr.xpath("./th|./td")]
                if len(cells) >= 4 and cells[0] == "3連単":
                    result["trifecta"] = cells[1].replace(" ", "")
                    result["payout_3t"] = to_int_money(cells[2])
                    result["popularity_3t"] = to_int_money(cells[3])
                    break
        return result


def load_predictions(date: str) -> list[dict[str, Any]]:
    path = PREDICTION_DIR / f"predictions_{date}.jsonl"
    if not path.exists():
        return []
    rows = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.strip():
            rows.append(json.loads(line))
    return rows


def prediction_summary(pred: dict[str, Any], result: dict[str, Any]) -> dict[str, Any]:
    pred_top = int(pred["ranking"][0][0])
    actual_winner = int(result["winner"])
    top3_pred = [int(item[0]) for item in pred["ranking"][:3]]
    finish_order = [int(x) for x in result["finish_order"]]
    return {
        "race_id": pred["race_id"],
        "date": pred["date"],
        "jcd": pred["jcd"],
        "course": pred["course"],
        "rno": pred["rno"],
        "predicted_at": pred["predicted_at"],
        "deadline": pred["deadline"],
        "strategy": pred.get("strategy") or "",
        "pred_winner": pred_top,
        "actual_winner": actual_winner,
        "winner_hit": int(pred_top == actual_winner),
        "pred_upset_prob": pred["upset_prob"],
        "actual_upset": result["upset_actual"],
        "upset_hit": int((pred["upset_prob"] >= 0.5) == bool(result["upset_actual"])),
        "pred_top3": "-".join(map(str, top3_pred)),
        "actual_top3": "-".join(map(str, finish_order[:3])),
        "top3_box_hit": int(set(top3_pred) == set(finish_order[:3])),
        "trifecta": result.get("trifecta", ""),
        "payout_3t": result.get("payout_3t"),
        "popularity_3t": result.get("popularity_3t"),
        "result_url": result.get("result_url", ""),
    }


def write_feedback(date: str, rows: list[dict[str, Any]]) -> Path:
    FEEDBACK_DIR.mkdir(parents=True, exist_ok=True)
    path = FEEDBACK_DIR / f"feedback_{date}.csv"
    if not rows:
        path.write_text("", encoding="utf-8")
        return path
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
    return path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", default=datetime.now(JST).strftime("%Y%m%d"))
    parser.add_argument("--sleep", type=float, default=0.5)
    args = parser.parse_args()

    predictions = load_predictions(args.date)
    client = OfficialResultClient()
    rows: list[dict[str, Any]] = []
    for pred in predictions:
        try:
            result = client.fetch_result(pred["date"], pred["jcd"], int(pred["rno"]))
        except Exception as exc:
            print(f"skip {pred['race_id']}: {exc}")
            continue
        if not result:
            print(f"pending {pred['race_id']}")
            continue
        rows.append(prediction_summary(pred, result))
        time.sleep(args.sleep)
    out = write_feedback(args.date, rows)
    print(f"predictions={len(predictions)} matched={len(rows)} out={out}")
    if rows:
        winner_hit = sum(r["winner_hit"] for r in rows) / len(rows)
        upset_hit = sum(r["upset_hit"] for r in rows) / len(rows)
        top3_box = sum(r["top3_box_hit"] for r in rows) / len(rows)
        print(f"winner_hit={winner_hit:.1%} upset_hit={upset_hit:.1%} top3_box_hit={top3_box:.1%}")


if __name__ == "__main__":
    main()
