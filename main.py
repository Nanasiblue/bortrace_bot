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

# モデルファイルのパス (GitHub Actions等での動作を想定し相対パスで定義)
BASE_DIR = Path(__file__).resolve().parent
MODEL_PATH = BASE_DIR / "final_model_v4.pkl"
CONFIG_PATH = BASE_DIR / "model_config_v4.pkl"

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
# ==========================================
# 1. スクレイパー (v5: 指紋偽装・Referer強化版)
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
        # よりブラウザに近いヘッダー設定
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "Accept-Language": "ja,en-US;q=0.9,en;q=0.8",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1"
        }
        self.session = requests.Session()
        self.session.headers.update(self.headers)
        self.course_links = {} # {course_name: list_url}
        self.date_str = ""
        
        # セッションの初期化 (Warm-up)
        try:
            self.session.get("https://www.boatrace.jp/", timeout=15)
        except: pass

    def _get_soup(self, url, referer=None, retries=3):
        for i in range(retries):
            try:
                headers = {"Referer": referer} if referer else {}
                res = self.session.get(url, headers=headers, timeout=20)
                res.raise_for_status()
                return BeautifulSoup(res.content, "html.parser")
            except Exception as e:
                wait = (i + 1) * 3
                print(f"[{datetime.now(JST).strftime('%H:%M:%S')}] ⚠️ Retry {i+1}/{retries}: {url} - {e}")
                time.sleep(wait)
        return None

    def fetch_active_courses(self, date_str):
        self.date_str = date_str
        print(f"[{datetime.now(JST).strftime('%H:%M:%S')}] 🔍 Fetching active courses...")
        index_url = f"{self.INDEX_URL}?hd={date_str}"
        soup = self._get_soup(index_url, referer="https://www.boatrace.jp/")
        if not soup: return []
        
        self.course_links = {}
        active_courses = []
        inv_map = {v: k for k, v in self.COURSE_MAP.items()}
        
        # indexページにある実際のリンク(href)を抽出して保存する
        for link in soup.select("a[href*='jcd=']"):
            href = link.get('href', '')
            m = re.search(r"jcd=(\d{2})", href)
            if m and m.group(1) in inv_map:
                name = inv_map[m.group(1)]
                if href.startswith("/"):
                    href = "https://www.boatrace.jp" + href
                self.course_links[name] = href
                active_courses.append(name)
        
        return sorted(list(set(active_courses)))

    def get_target_races_for_course(self, course, date_str, now_dt):
        # 自分でURLを組み立てず、indexページから抽出したリンクをそのまま使う
        url = self.course_links.get(course)
        if not url:
            jcd = self.COURSE_MAP[course]
            url = f"{self.LIST_URL}?jcd={jcd}&hd={date_str}"
            
        index_url = f"{self.INDEX_URL}?hd={date_str}"
        soup = self._get_soup(url, referer=index_url)
        targets = []
        if not soup:
            print(f"  ❌ Failed to get race list for {course}")
            return []
        
        page_text = soup.get_text().replace("\n", " ").replace("\r", " ").strip()
        all_deadlines = re.findall(r"締切予定.*?(\d{1,2}:\d{2})", page_text)
        
        if not all_deadlines:
            # 取得失敗時にタイトルなどを表示して原因を探る
            title = soup.title.string if soup.title else "No Title"
            print(f"  ⚠️ No deadline found in {course}. (Title: {title})")
            return []

        for i, time_str in enumerate(all_deadlines):
            current_r = i + 1
            if current_r > 12: break
            try:
                race_dt = datetime.strptime(f"{date_str} {time_str.zfill(5)}", "%Y%m%d %H:%M").replace(tzinfo=JST)
                minutes = (race_dt - now_dt).total_seconds() / 60
                if 5 <= minutes <= 45:
                    print(f"  - {course} {current_r}R: 締切まで {minutes:.1f}分 ({time_str})")
                if 5 <= minutes <= 35: 
                    targets.append(current_r)
            except Exception as e:
                print(f"  Error parsing time for {course} {current_r}R: {e}")
        return targets

    def fetch_race_data(self, course, rno, date_str):
        # リストページへの参照も保存されたものを使う
        list_url = self.course_links.get(course, f"{self.LIST_URL}?jcd={self.COURSE_MAP[course]}&hd={date_str}")
        try:
            # 出走表(詳細)のリクエスト
            race_list_url = f"{self.LIST_URL}?rno={rno}&jcd={self.COURSE_MAP[course]}&hd={date_str}"
            soup_list = self._get_soup(race_list_url, referer=list_url)
            if not soup_list: return None
            
            deadline_str = "00:00"
            m_time = re.search(r"締切予定.*?(\d{1,2}:\d{2})", soup_list.get_text())
            if m_time: deadline_str = m_time.group(1).zfill(5)
            
            # 直前情報のURL
            info_url = f"{self.BASE_URL}?rno={rno}&jcd={self.COURSE_MAP[course]}&hd={date_str}"
            soup_info = self._get_soup(info_url, referer=race_list_url)
            if not soup_info or "データがありません" in soup_info.text: return None
            
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
