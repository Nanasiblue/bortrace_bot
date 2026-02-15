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

# モデルファイルのパス
MODEL_PATH = Path("final_model_v4.pkl")
CONFIG_PATH = Path("model_config_v4.pkl")

# 通知済みレースを記録するログファイル
LOG_FILE = Path("notified_races.log")

# ==========================================
# 重複通知防止ロジック
# ==========================================
def is_already_notified(race_id):
    if not LOG_FILE.exists():
        return False
    with open(LOG_FILE, "r") as f:
        notified_races = f.read().splitlines()
    return race_id in notified_races

def save_notified_race(race_id):
    with open(LOG_FILE, "a") as f:
        f.write(race_id + "\n")

# ==========================================
# 1. スクレイパー (v5: ログ強化版)
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
        # Sessionを導入してコネクションを使い回す
        self.session = requests.Session()
        self.session.headers.update(self.headers)

    def _get_soup(self, url, retries=3):
        """強化されたスープ取得メソッド（タイムアウト30秒、指数バックオフ付き）"""
        for i in range(retries):
            try:
                # print(f"  [Request] {url}") # 詳細すぎてうるさい場合はコメントアウト
                res = self.session.get(url, timeout=30)
                res.raise_for_status()
                return BeautifulSoup(res.content, "html.parser")
            except Exception as e:
                wait = (i + 1) * 5
                print(f"[{datetime.now(JST).strftime('%H:%M:%S')}] ⚠️ Retry {i+1}/{retries}: {url} (Wait {wait}s) - {e}")
                time.sleep(wait)
        return None

    def fetch_active_courses(self, date_str):
        print(f"[{datetime.now(JST).strftime('%H:%M:%S')}] 🔍 Fetching active courses...")
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
        if not soup:
            print(f"  ❌ Failed to get race list for {course}")
            return []
        
        bodies = soup.select("tbody") 
        current_r = 1
        for b in bodies:
            text = b.get_text().replace("\n", " ")
            m = re.search(r"締切予定.*?(\d{1,2}:\d{2})", text)
            if m:
                time_str = m.group(1).zfill(5)
                race_dt_str = f"{date_str} {time_str}"
                try:
                    race_dt = datetime.strptime(race_dt_str, "%Y%m%d %H:%M").replace(tzinfo=JST)
                    diff = race_dt - now_dt
                    minutes = diff.total_seconds() / 60
                    # 進行状況の表示
                    if 10 <= minutes <= 45: # ログが多すぎるのを防ぐため、締切に近いレースのみ表示
                        print(f"  - {course} {current_r}R: 締切まで {minutes:.1f}分")

                    # 直前情報（展示）が確定するタイミングに合わせて調整 (10-25分前)
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
            
            deadline_str = "00:00"
            m_time = re.search(r"締切予定.*?(\d{1,2}:\d{2})", soup_list.get_text())
            if m_time: deadline_str = m_time.group(1).zfill(5)
            
            bodies = soup_list.select("tbody.is-fs12") or soup_list.select("tbody")
            
            boat_info = {}
            for i in range(1, 7):
                rank, win_rate = "B2", 0.0
                for b in bodies:
                    is_boat_row = b.select_one(f".is-ladder{i}") or str(i) in b.text[:5]
                    if is_boat_row:
                        r_m = re.search(r"([AB][12])", b.get_text())
                        if r_m: rank = r_m.group(1)
                        rates = re.findall(r"(\d\.\d{2})", b.get_text())
                        if rates: win_rate = float(rates[0])
                        break
                boat_info[i] = {"rank": rank, "win_rate": win_rate}

            soup_info = self._get_soup(f"{self.BASE_URL}?rno={rno}&jcd={jcd}&hd={date_str}")
            if not soup_info or "データがありません" in soup_info.text: return None

            weather = soup_info.select_one(".weather1")
            wind_speed, wave = 0, 0
            if weather:
                txt = weather.text
                w_m = re.search(r"風速.*?(\d+)m", txt)
                h_m = re.search(r"波高.*?(\d+)cm", txt)
                if w_m: wind_speed = int(w_m.group(1))
                if h_m: wave = int(h_m.group(1))

            table = soup_info.select_one(".is-w748")
            if not table: return None
            rows = table.select("tbody")
            
            data = {"wind_speed": wind_speed, "wave": wave, "deadline": deadline_str}
            for i in range(1, 7):
                tds = rows[i-1].select("td")
                ex_val = tds[4].text.strip()
                data[f"ex_time_{i}"] = float(ex_val) if ex_val and ex_val[0].isdigit() else 6.80
                st_text = tds[2].select_one(".is-fs11").text.strip() if tds[2].select_one(".is-fs11") else ".15"
                data[f"st_{i}"] = float("0"+re.search(r"(\.\d+)", st_text).group(1)) if re.search(r"(\.\d+)", st_text) else 0.15
                data[f"rank_{i}"] = boat_info[i]["rank"]
                data[f"win_rate_{i}"] = boat_info[i]["win_rate"]

            return data
        except: return None

# ==========================================
# 2. 予測ロジック
# ==========================================
def predict_single(model, config, scraper, course, rno, date_str):
    try:
        data = scraper.fetch_race_data(course, rno, date_str)
        if not data: 
            print(f"  ⚠️ Failed to fetch detail data for {course} {rno}R")
            return None, -1
        
        ex_cols = [f"ex_time_{i}" for i in range(1, 7)]
        ex_vals = [data[c] for c in ex_cols]
        ex_mean = np.mean(ex_vals)
        rank_map = {"A1": 4, "A2": 3, "B1": 2, "B2": 1}
        
        input_dict = {"wind_speed": data["wind_speed"], "wave": data["wave"]}
        ex_ranks = pd.Series(ex_vals).rank(method="min").tolist()
        
        for i in range(1, 7):
            idx = i - 1
            rv = rank_map.get(data[f"rank_{i}"], 2)
            input_dict[f"rank_val_{i}"] = rv
            input_dict[f"win_rate_{i}"] = data[f"win_rate_{i}"]
            input_dict[f"ex_time_{i}"] = data[f"ex_time_{i}"]
            input_dict[f"ex_diff_{i}"] = data[f"ex_time_{i}"] - ex_mean
            input_dict[f"ex_rank_{i}"] = ex_ranks[idx]
            input_dict[f"st_{i}"] = data[f"st_{i}"]
            
        input_dict["is_debuff_1"] = 1 if (input_dict["rank_val_1"] <= 2 and input_dict["ex_rank_1"] >= 4) else 0
        
        input_df = pd.DataFrame([input_dict])[config["features"]]
        probs = model.predict(input_df)[0]
        
        in_jump_prob = 1 - probs[0]
        ranking = sorted({i+1: p for i, p in enumerate(probs) if i > 0}.items(), key=lambda x: x[1], reverse=True)
        top1, top2, top3 = ranking[0], ranking[1], ranking[2]
        
        strategy = ""
        if in_jump_prob >= 0.55:
            if top1[1] >= 0.35: strategy = "FOCUS"
            elif top1[1] >= 0.25: strategy = "STANDARD"
            else: strategy = "WIDE"
        
        if not strategy: return None, 0

        res_dict = {
            "場名": course, "レース": f"{rno}R", "締切": data['deadline'],
            "イン飛び率": in_jump_prob, "戦略": strategy,
            "1位": top1, "2位": top2, "3位": top3,
            "根拠": f"1号艇:{data['rank_1']} / 展示:{int(input_dict['ex_rank_1'])}位",
            "買い目": f"{top1[0]}-{top2[0]}{top3[0]}-全" if strategy != "WIDE" else "1抜きBOX推奨"
        }
        return res_dict, 1
        
    except Exception as e:
        print(f"Error in prediction: {e}")
        return None, -2

# ==========================================
# 3. メイン実行 (パトロール)
# ==========================================
def run_live_patrol():
    print(f"👮 Smart Patrol Start: {datetime.now(JST).strftime('%Y-%m-%d %H:%M:%S')}")
    
    if not MODEL_PATH.exists():
        print(f"❌ Error: Model file not found at {MODEL_PATH}")
        return

    with open(MODEL_PATH, "rb") as f: model = pickle.load(f)
    with open(CONFIG_PATH, "rb") as f: config = pickle.load(f)
    print("✅ Model loaded successfully.")

    scraper = BoatRaceScraperV5()
    now_jst = datetime.now(JST)
    date_str = now_jst.strftime("%Y%m%d")
    
    courses = scraper.fetch_active_courses(date_str)
    print(f"Active Courses: {courses}")
    
    hit_count = 0
    for course in courses:
        print(f"[{datetime.now(JST).strftime('%H:%M:%S')}] 🏁 Checking {course}...")
        targets = scraper.get_target_races_for_course(course, date_str, now_jst)
        
        if not targets:
            # print(f"  (No target races in {course})")
            pass
            
        for rno in targets:
            race_id = f"{date_str}_{course}_{rno}"
            
            # 通知済みならスキップ
            if is_already_notified(race_id):
                print(f"  - {course} {rno}R: Already notified, skipping.")
                continue

            print(f"  - {course} {rno}R: Analyzing...")
            res, status = predict_single(model, config, scraper, course, rno, date_str)
            
            if status == 1:
                hit_count += 1
                # Discord通知処理 (フォーマットを調整)
                content = f"🎯 **投資チャンス到来！**\n📍 **{res['場名']} {res['レース']}** (締切 {res['締切']})\n"
                content += f"━━━━━━━━━━━━━━━━━━━━\n🔥 戦略: **{res['戦略']}**\n😱 イン飛び率: `{res['イン飛び率']:.1%}`\n\n"
                content += f"📊 **AI勝率ランキング (1抜き)**\n🥇 **{res['1位'][0]}号艇**: `{res['1位'][1]:.1%}`\n🥈 **{res['2位'][0]}号艇**: `{res['2位'][1]:.1%}`\n🥉 **{res['3位'][0]}号艇**: `{res['3位'][1]:.1%}`\n\n"
                content += f"📝 根拠: {res['根拠']}\n💰 推奨: `{res['買い目']}`\n━━━━━━━━━━━━━━━━━━━━"
                
                if DISCORD_WEBHOOK_URL:
                    try:
                        requests.post(DISCORD_WEBHOOK_URL, json={"content": content}, timeout=15)
                        print(f"    ✅ Notification Sent for {race_id}")
                    except Exception as e:
                        print(f"    ❌ Discord Error: {e}")
                
                # 通知済みリストに保存
                save_notified_race(race_id)
            time.sleep(1)

    print(f"👮 Patrol Finished: Found {hit_count} hits.")

if __name__ == "__main__":
    run_live_patrol()
