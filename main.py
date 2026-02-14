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

MODEL_DIR = "boatrace/output_v4"

# ==========================================
# 1. スクレイパー (v5: 全艇の級別・勝率対応)
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
                race_dt_str = f"{date_str} {time_str}"
                try:
                    race_dt = datetime.strptime(race_dt_str, "%Y%m%d %H:%M").replace(tzinfo=JST)
                    diff = race_dt - now_dt
                    minutes = diff.total_seconds() / 60
                    if 10 <= minutes <= 40: # 締切10〜40分前を対象
                        targets.append(current_r)
                except: pass
            current_r += 1
            if current_r > 12: break
        return targets

    def fetch_race_data(self, course, rno, date_str):
        jcd = self.COURSE_MAP[course]
        try:
            # 1. 出走表
            soup_list = self._get_soup(f"{self.LIST_URL}?rno={rno}&jcd={jcd}&hd={date_str}")
            if not soup_list: return None
            
            # 締切時刻
            deadline_str = "00:00"
            m_time = re.search(r"締切予定.*?(\d{1,2}:\d{2})", soup_list.get_text())
            if m_time: deadline_str = m_time.group(1).zfill(5)
            
            # 各艇の級別と勝率
            bodies = soup_list.select("tbody.is-fs12")
            if len(bodies) < 6: bodies = soup_list.select("tbody") # フォールバック
            
            # 艇番ごとの情報を抽出
            boat_info = {}
            for i in range(1, 7):
                target_body = None
                for b in bodies:
                    if str(i) in b.text[:10]: # 艇番がテキストの先頭付近にあるか
                        target_body = b
                        break
                
                rank, win_rate = "B2", 0.0
                if target_body:
                    r_m = re.search(r"/ ([AB][12])", target_body.text)
                    if r_m: rank = r_m.group(1)
                    rates = re.findall(r"(\d\.\d{2})", target_body.get_text())
                    if rates: win_rate = float(rates[0])
                boat_info[i] = {"rank": rank, "win_rate": win_rate}

            # 2. 直前情報
            soup_info = self._get_soup(f"{self.BASE_URL}?rno={rno}&jcd={jcd}&hd={date_str}")
            if not soup_info or "データがありません" in soup_info.text: return None

            weather = soup_info.select_one(".weather1")
            wind_speed, wave = 0, 0
            if weather:
                txt = weather.text
                w_m = re.search(r"風速.*?(\d+)m", txt)
                h_m = re.search(r"波高.*?(\d+)cm", txt)
                if w_m: wind_speed, wave = int(w_m.group(1)), int(h_m.group(1))

            table = soup_info.select_one(".is-w748")
            if not table: return None
            rows = table.select("tbody")
            
            data = {"wind_speed": wind_speed, "wave": wave, "deadline": deadline_str}
            for i in range(1, 7):
                tds = rows[i-1].select("td")
                if len(tds) < 5: return None
                ex_val = tds[4].text.strip()
                data[f"ex_time_{i}"] = float(ex_val) if ex_val else 6.80
                st_text = tds[2].select_one(".is-fs11").text.strip() if tds[2].select_one(".is-fs11") else "0.00"
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
        if not data: return None, -1
        
        # 特徴量エンジニアリング (v4同等)
        ex_cols = [f"ex_time_{i}" for i in range(1, 7)]
        ex_vals = [data[c] for c in ex_cols]
        ex_mean = np.mean(ex_vals)
        rank_map = {"A1": 4, "A2": 3, "B1": 2, "B2": 1}
        
        input_dict = {"wind_speed": data["wind_speed"], "wave": data["wave"]}
        
        # 各艇の特徴量
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
            
        # is_debuff_1 の計算
        is_debuff_1 = 1 if (input_dict["rank_val_1"] <= 2 and input_dict["ex_rank_1"] >= 4) else 0
        input_dict["is_debuff_1"] = is_debuff_1
        
        # 予測
        input_df = pd.DataFrame([input_dict])[config["features"]]
        probs = model.predict(input_df)[0]
        
        in_win_prob = probs[0]
        in_jump_prob = 1 - in_win_prob
        
        # 他艇の分析
        other_probs = probs[1:]
        top_other_idx = np.argmax(other_probs)
        top_other_boat = top_other_idx + 2
        top_other_prob = other_probs[top_other_idx]
        
        # 戦略判定
        strategy = ""
        if in_jump_prob >= 0.55:
            if top_other_prob >= 0.35: strategy = "FOCUS"
            elif top_other_prob >= 0.25: strategy = "STANDARD"
            else: strategy = "WIDE"
        
        if not strategy: return None, 0

        res_dict = {
            "場名": course, "レース": f"{rno}R", "締切": data['deadline'],
            "イン飛び率": in_jump_prob, "戦略": strategy,
            "軸艇": f"{top_other_boat}号艇", "軸確率": top_other_prob,
            "根拠": f"1号艇級別:{data['rank_1']} / 展示:{int(input_dict['ex_rank_1'])}位",
            "買い目": f"{top_other_boat}-全-全" if strategy != "WIDE" else "1抜きBOX推奨"
        }
        return res_dict, 1
        
    except Exception as e:
        print(f"Error in predict_single: {e}")
        return None, -2

# ==========================================
# 3. メイン実行
# ==========================================
def run_live_patrol():
    print("🚀 Starting...")
    
    model_path = "final_model_v4.pkl"
    config_path = "model_config_v4.pkl"
    
    if not model_path.exists():
        print(f"Error: Model files not found at {model_path}")
        return

    with open(model_path, "rb") as f: model = pickle.load(f)
    with open(config_path, "rb") as f: config = pickle.load(f)

    scraper = BoatRaceScraperV5()
    now_jst = datetime.now(JST)
    date_str = now_jst.strftime("%Y%m%d")
    
    courses = scraper.fetch_active_courses(date_str)
    if not courses:
        print("No races today.")
        return
        
    hits = []
    for course in courses:
        targets = scraper.get_target_races_for_course(course, date_str, now_jst)
        for rno in targets:
            print(f"Analyzing {course} {rno}R...")
            res, status = predict_single(model, config, scraper, course, rno, date_str)
            if status == 1:
                hits.append(res)
            time.sleep(1)

    if hits and DISCORD_WEBHOOK_URL:
        for r in hits:
            content = f"🎯 ** 投資チャンス到来！**\n"
            content += f"📍 **{r['場名']} {r['レース']}** (締切 {r['締切']})\n"
            content += f"━━━━━━━━━━━━━━━━━━━━\n"
            content += f"🔥 戦略: **{r['戦略']}**\n"
            content += f"😱 イン飛び確率: `{r['イン飛び率']:.1%}`\n"
            content += f"🏆 注目軸艇: **{r['軸艇']}** (勝率予測: `{r['軸確率']:.1%}`)\n"
            content += f"📝 根拠: {r['根拠']}\n"
            content += f"💰 推奨: `{r['買い目']}`\n"
            content += "━━━━━━━━━━━━━━━━━━━━"
            requests.post(DISCORD_WEBHOOK_URL, json={"content": content})
            print(f"Sent notification for {r['場名']} {r['レース']}")

if __name__ == "__main__":
    run_live_patrol()
