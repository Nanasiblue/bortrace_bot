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

# モデルファイル（学習済みモデルをここに入れてください）
MODEL_PATH = Path("final_model_v4.pkl")
CONFIG_PATH = Path("model_config_v4.pkl")
# 通知済みレースを記録するログファイル（自動生成されます）
LOG_FILE = Path("notified_races.log")

# ==========================================
# 共通ロジック
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
# 1. スクレイパー (v5: 高精度・全艇対応版)
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
            except:
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
                    # 展示確定から締切前までの10〜25分前を狙い撃ち
                    if 10 <= minutes <= 25: targets.append(current_r)
                except: pass
            current_r += 1
            if current_r > 12: break
        return targets

    def fetch_race_data(self, course, rno, date_str):
        jcd = self.COURSE_MAP[course]
        try:
            # 1. 出走表から級別・勝率を取得
            soup_list = self._get_soup(f"{self.LIST_URL}?rno={rno}&jcd={jcd}&hd={date_str}")
            if not soup_list: return None
            
            deadline = "00:00"
            m_time = re.search(r"締切予定.*?(\d{1,2}:\d{2})", soup_list.get_text())
            if m_time: deadline = m_time.group(1).zfill(5)
            
            bodies = soup_list.select("tbody.is-fs12") or soup_list.select("tbody")
            boat_info = {}
            for i in range(1, 7):
                rank, win_rate = "B2", 0.0
                for b in bodies:
                    if b.select_one(f".is-ladder{i}") or str(i) in b.text[:5]:
                        r_m = re.search(r"([AB][12])", b.get_text())
                        if r_m: rank = r_m.group(1)
                        rates = re.findall(r"(\d\.\d{2})", b.get_text())
                        if rates: win_rate = float(rates[0])
                        break
                boat_info[i] = {"rank": rank, "win_rate": win_rate}

            # 2. 直前情報（展示・気象）
            soup_info = self._get_soup(f"{self.BASE_URL}?rno={rno}&jcd={jcd}&hd={date_str}")
            if not soup_info or "データがありません" in soup_info.text: return None

            weather = soup_info.select_one(".weather1")
            wind, wave = 0, 0
            if weather:
                w_m = re.search(r"風速.*?(\d+)m", weather.text)
                h_m = re.search(r"波高.*?(\d+)cm", weather.text)
                wind, wave = (int(w_m.group(1)), int(h_m.group(1))) if w_m else (0, 0)

            ex_rows = soup_info.select_one(".is-w748").select("tbody")
            data = {"wind_speed": wind, "wave": wave, "deadline": deadline, "rank_1": boat_info[1]["rank"]}
            for i in range(1, 7):
                tds = ex_rows[i-1].select("td")
                ex_val = tds[4].text.strip()
                data[f"ex_time_{i}"] = float(ex_val) if ex_val and ex_val[0].isdigit() else 6.80
                st_text = tds[2].select_one(".is-fs11").text.strip() if tds[2].select_one(".is-fs11") else ".15"
                data[f"st_{i}"] = float("0"+re.search(r"(\.\d+)", st_text).group(1)) if re.search(r"(\.\d+)", st_text) else 0.15
                data[f"rank_{i}"] = boat_info[i]["rank"]
                data[f"win_rate_{i}"] = boat_info[i]["win_rate"]
            return data
        except: return None

# ==========================================
# 2. 予測ロジック (ランキング・フォーメーション対応)
# ==========================================
def predict_single(model, config, scraper, course, rno, date_str):
    try:
        data = scraper.fetch_race_data(course, rno, date_str)
        if not data: return None, -1
        
        # 特徴量生成
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
        
        # 予測実行
        input_df = pd.DataFrame([input_dict])[config["features"]]
        probs = model.predict(input_df)[0]
        
        # ランキング作成 (1号艇を除外した勝率)
        boat_probs = {i+1: p for i, p in enumerate(probs)}
        ranking = sorted({k: v for k, v in boat_probs.items() if k != 1}.items(), key=lambda x: x[1], reverse=True)
        
        top1, top2, top3 = ranking[0], ranking[1], ranking[2]
        in_jump_prob = 1 - probs[0]
        
        # 閾値判定 (ROI 150%超え設定)
        strategy = ""
        if in_jump_prob >= 0.55:
            if top1[1] >= 0.35: strategy = "FOCUS"
            elif top1[1] >= 0.25: strategy = "STANDARD"
            else: strategy = "WIDE"
        
        if not strategy: return None, 0

        # フォーメーション生成
        if strategy == "FOCUS":
            bet_msg = f"{top1[0]} - {top2[0]}{top3[0]} - 全"
        elif strategy == "STANDARD":
            bet_msg = f"{top1[0]}{top2[0]} - {top1[0]}{top2[0]}{top3[0]} - 全"
        else:
            bet_msg = f"{top1[0]},{top2[0]},{top3[0]} BOX推奨 (1抜き広域)"

        res_dict = {
            "場名": course, "レース": f"{rno}R", "締切": data['deadline'],
            "イン飛び率": in_jump_prob, "戦略": strategy,
            "1位": top1, "2位": top2, "3位": top3,
            "根拠": f"1号艇級別:{data['rank_1']} / 展示:{int(input_dict['ex_rank_1'])}位",
            "買い目": bet_msg
        }
        return res_dict, 1
    except: return None, -2

# ==========================================
# 3. メイン実行 (自動パトロール)
# ==========================================
def run_live_patrol():
    if not MODEL_PATH.exists():
        print("Error: Model file not found.")
        return

    with open(MODEL_PATH, "rb") as f: model = pickle.load(f)
    with open(CONFIG_PATH, "rb") as f: config = pickle.load(f)

    scraper = BoatRaceScraperV5()
    now = datetime.now(JST)
    date_str = now.strftime("%Y%m%d")
    
    courses = scraper.fetch_active_courses(date_str)
    
    for course in courses:
        targets = scraper.get_target_races_for_course(course, date_str, now)
        for rno in targets:
            race_id = f"{date_str}_{course}_{rno}"
            if is_already_notified(race_id): continue

            print(f"Analyzing {course} {rno}R...")
            res, status = predict_single(model, config, scraper, course, rno, date_str)
            
            if status == 1:
                content = f"🎯 ** 投資チャンス到来！**\n📍 **{res['場名']} {res['レース']}** (締切 {res['締切']})\n"
                content += f"━━━━━━━━━━━━━━━━━━━━\n🔥 戦略: **{res['戦略']}**\n😱 イン飛び率: `{res['イン飛び率']:.1%}`\n\n"
                content += f"📊 **AI勝率ランキング (1抜き)**\n🥇 **{res['1位'][0]}号艇**: `{res['1位'][1]:.1%}`\n🥈 **{res['2位'][0]}号艇**: `{res['2位'][1]:.1%}`\n🥉 **{res['3位'][0]}号艇**: `{res['3位'][1]:.1%}`\n\n"
                content += f"📝 根拠: {res['根拠']}\n💰 推奨: `{res['買い目']}`\n━━━━━━━━━━━━━━━━━━━━"
                
                if DISCORD_WEBHOOK_URL:
                    requests.post(DISCORD_WEBHOOK_URL, json={"content": content})
                    print(f"Sent notification for {race_id}")
                save_notified_race(race_id)
            time.sleep(1)

if __name__ == "__main__":
    run_live_patrol()
