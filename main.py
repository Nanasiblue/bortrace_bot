import os
import pandas as pd
import numpy as np
import pickle
import re
import requests
import time
from bs4 import BeautifulSoup
from pathlib import Path
from datetime import datetime, timedelta, timezone

# ==========================================
# 設定
# ==========================================
DISCORD_WEBHOOK_URL = os.environ.get("DISCORD_WEBHOOK_URL")
JST = timezone(timedelta(hours=9), 'JST')

MODEL_PATH = Path("final_model_v4.pkl")
CONFIG_PATH = Path("model_config_v4.pkl")
LOG_FILE = Path("notified_races.log")

# ==========================================
# デバッグ用ヘルパー
# ==========================================
def debug_log(msg):
    now = datetime.now(JST).strftime("%H:%M:%S")
    print(f"[{now}] DEBUG: {msg}")

# ==========================================
# 重複通知防止ロジック
# ==========================================
def is_already_notified(race_id):
    if not LOG_FILE.exists(): return False
    with open(LOG_FILE, "r") as f:
        notified_races = f.read().splitlines()
    return race_id in notified_races

def save_notified_race(race_id):
    with open(LOG_FILE, "a") as f:
        f.write(race_id + "\n")

# ==========================================
# 1. スクレイパー (v5)
# ==========================================
class BoatRaceScraperV5:
    BASE_URL = "https://www.boatrace.jp/owpc/pc/race/beforeinfo"
    LIST_URL = "https://www.boatrace.jp/owpc/pc/race/racelist"
    INDEX_URL = "https://www.boatrace.jp/owpc/pc/race/index"
    
    COURSE_MAP = {
        "桐生": "01", "戸田": "02", "江戸川": "03", "平和島": "04", "多摩川": "05",
        "浜名湖": "06", "蒲郡": "07", "常滑": "08", "津": "09", "三国": "10",
        "びわこ": "11", "住之江": "12", "尼崎": "13", "鳴門": "14", "丸亀": "15",
        "児島": "16", "宮島": "17", "徳山": "18", "下関": "19", "若松": "20",
        "芦屋": "21", "福岡": "22", "唐津": "23", "大村": "24"
    }

    def __init__(self):
        self.headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}

    def _get_soup(self, url, retries=2):
        for i in range(retries):
            try:
                res = requests.get(url, headers=self.headers, timeout=10)
                res.raise_for_status()
                return BeautifulSoup(res.content, "html.parser")
            except Exception as e:
                debug_log(f"URL取得失敗({i+1}/2): {url} - {e}")
                time.sleep(1)
                continue
        return None

    def fetch_active_courses(self, date_str):
        soup = self._get_soup(f"{self.INDEX_URL}?hd={date_str}")
        if not soup: return []
        active_courses = []
        inv_map = {v: k for k, v in self.COURSE_MAP.items()}
        for link in soup.select("a[href*='jcd=']"):
            m = re.search(r"jcd=(\d{2})", link['href'])
            if m and m.group(1) in inv_map:
                active_courses.append(inv_map[m.group(1)])
        return sorted(list(set(active_courses)))

    def get_target_races_for_course(self, course, date_str, now_dt):
        jcd = self.COURSE_MAP[course]
        url = f"{self.LIST_URL}?jcd={jcd}&hd={date_str}"
        soup = self._get_soup(url)
        targets = []
        if not soup: return []
        bodies = soup.select("tbody") 
        current_r = 1
        for b in bodies:
            text = b.get_text().replace("\n", " ")
            m = re.search(r"締切予定.*?(\d{1,2}:\d{2})", text)
            if m:
                time_str = m.group(1).zfill(5)
                try:
                    race_dt = datetime.strptime(f"{date_str} {time_str}", "%Y%m%d %H:%M").replace(tzinfo=JST)
                    minutes = (race_dt - now_dt).total_seconds() / 60
                    # デバッグ用に全レースの時間差を出力
                    # debug_log(f"{course} {current_r}R: 締切まで {minutes:.1f}分")
                    if 10 <= minutes <= 25: 
                        targets.append(current_r)
                except: pass
            current_r += 1
            if current_r > 12: break
        return targets

    def fetch_race_data(self, course, rno, date_str):
        jcd = self.COURSE_MAP[course]
        try:
            soup_list = self._get_soup(f"{self.LIST_URL}?rno={rno}&jcd={jcd}&hd={date_str}")
            if not soup_list: return None
            
            bodies = soup_list.select("tbody.is-fs12") or soup_list.select("tbody")
            boat_info = {}
            for i in range(1, 7):
                rank, win_rate = "B2", 0.0
                for b in bodies:
                    if b.select_one(f".is-ladder{i}") or str(i) in b.text[:5]:
                        r_m = re.search(r"([AB][12])", b.get_text())
                        rank = r_m.group(1) if r_m else "B2"
                        rates = re.findall(r"(\d\.\d{2})", b.get_text())
                        win_rate = float(rates[0]) if rates else 0.0
                        break
                boat_info[i] = {"rank": rank, "win_rate": win_rate}

            soup_info = self._get_soup(f"{self.BASE_URL}?rno={rno}&jcd={jcd}&hd={date_str}")
            if not soup_info or "データがありません" in soup_info.text: return None

            weather = soup_info.select_one(".weather1")
            wind, wave = 0, 0
            if weather:
                w_m = re.search(r"風速.*?(\d+)m", weather.text)
                h_m = re.search(r"波高.*?(\d+)cm", weather.text)
                wind, wave = (int(w_m.group(1)), int(h_m.group(1))) if w_m else (0, 0)

            ex_rows = soup_info.select_one(".is-w748").select("tbody")
            data = {"wind_speed": wind, "wave": wave, "rank_1": boat_info[1]["rank"]}
            for i in range(1, 7):
                tds = ex_rows[i-1].select("td")
                ex_val = tds[4].text.strip()
                data[f"ex_time_{i}"] = float(ex_val) if ex_val and ex_val[0].isdigit() else 6.80
                st_text = tds[2].select_one(".is-fs11").text.strip() if tds[2].select_one(".is-fs11") else ".15"
                data[f"st_{i}"] = float("0"+re.search(r"(\.\d+)", st_text).group(1)) if re.search(r"(\.\d+)", st_text) else 0.15
                data[f"rank_{i}"] = boat_info[i]["rank"]
                data[f"win_rate_{i}"] = boat_info[i]["win_rate"]
            return data
        except Exception as e:
            debug_log(f"データ解析エラー: {e}")
            return None

# ==========================================
# 2. 予測ロジック
# ==========================================
def predict_single(model, config, scraper, course, rno, date_str):
    try:
        data = scraper.fetch_race_data(course, rno, date_str)
        if not data: 
            debug_log(f"   -> {course}{rno}R: 展示データ未確定")
            return None, -1
        
        ex_cols = [f"ex_time_{i}" for i in range(1, 7)]
        ex_vals = [data[c] for c in ex_cols]
        ex_mean = np.mean(ex_vals)
        rank_map = {"A1": 4, "A2": 3, "B1": 2, "B2": 1}
        input_dict = {"wind_speed": data["wind_speed"], "wave": data["wave"]}
        ex_ranks = pd.Series(ex_vals).rank(method="min").tolist()
        
        for i in range(1, 7):
            idx = i - 1
            input_dict[f"rank_val_{i}"] = rank_map.get(data[f"rank_{i}"], 2)
            input_dict[f"win_rate_{i}"] = data[f"win_rate_{i}"]
            input_dict[f"ex_time_{i}"] = data[f"ex_time_{i}"]
            input_dict[f"ex_diff_{i}"] = data[f"ex_time_{i}"] - ex_mean
            input_dict[f"ex_rank_{i}"] = ex_ranks[idx]
            input_dict[f"st_{i}"] = data[f"st_{i}"]
        input_dict["is_debuff_1"] = 1 if (input_dict["rank_val_1"] <= 2 and input_dict["ex_rank_1"] >= 4) else 0
        
        input_df = pd.DataFrame([input_dict])[config["features"]]
        probs = model.predict(input_df)[0]
        
        boat_probs = {i+1: p for i, p in enumerate(probs)}
        ranking = sorted({k: v for k, v in boat_probs.items() if k != 1}.items(), key=lambda x: x[1], reverse=True)
        top1 = ranking[0]
        in_jump_prob = 1 - probs[0]
        
        # 判定
        if in_jump_prob >= 0.55:
            strategy = "FOCUS" if top1[1] >= 0.35 else "STANDARD" if top1[1] >= 0.25 else "WIDE"
            debug_log(f"   -> {course}{rno}R: 【🎯投資対象】 イン飛び:{in_jump_prob:.1%} 本命:{top1[1]:.1%}")
            return {"戦略": strategy, "イン飛び率": in_jump_prob, "1位": top1, "2位": ranking[1], "3位": ranking[2], "根拠": f"1号艇級別:{data['rank_1']}"}, 1
        else:
            # debug_log(f"   -> {course}{rno}R: 【見送り】 イン飛び率不足 ({in_jump_prob:.1%})")
            return None, 0
    except Exception as e:
        debug_log(f"予測エラー: {e}")
        return None, -2

# ==========================================
# 3. メイン実行
# ==========================================
def run_live_patrol():
    debug_log("=== パトロール開始 ===")
    
    if not MODEL_PATH.exists():
        debug_log(f"致命的エラー: モデルファイル {MODEL_PATH} がありません")
        return

    with open(MODEL_PATH, "rb") as f: model = pickle.load(f)
    with open(CONFIG_PATH, "rb") as f: config = pickle.load(f)
    debug_log("モデル読み込み完了")

    scraper = BoatRaceScraperV5()
    now = datetime.now(JST)
    date_str = now.strftime("%Y%m%d")
    
    courses = scraper.fetch_active_courses(date_str)
    debug_log(f"開催中の場: {courses}")

    if not courses:
        debug_log("現在開催中の場がありません。")
        return

    hit_count = 0
    for course in courses:
        targets = scraper.get_target_races_for_course(course, date_str, now)
        if targets:
            debug_log(f"{course} のチェック対象: {targets}R")
        
        for rno in targets:
            race_id = f"{date_str}_{course}_{rno}"
            if is_already_notified(race_id):
                continue

            res, status = predict_single(model, config, scraper, course, rno, date_str)
            
            if status == 1:
                hit_count += 1
                # Discord通知省略 (既存のままでOK)
                if DISCORD_WEBHOOK_URL:
                    content = f"🎯 ** 投資チャンス！ {course} {rno}R**\nイン飛び:{res['イン飛び率']:.1%} 戦略:{res['戦略']}\n推奨:`{res['1位'][0]}-全-全`"
                    requests.post(DISCORD_WEBHOOK_URL, json={"content": content})
                save_notified_race(race_id)
            time.sleep(0.5)

    debug_log(f"パトロール完了。今回見つかった対象: {hit_count}件")

if __name__ == "__main__":
    run_live_patrol()
