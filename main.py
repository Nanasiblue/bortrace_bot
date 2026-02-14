import os
import pandas as pd
import numpy as np
import pickle
import re
import requests
import time
import concurrent.futures
from bs4 import BeautifulSoup
from pathlib import Path
from datetime import datetime, timedelta, timezone

# ==========================================
# 設定
# ==========================================
DISCORD_WEBHOOK_URL = os.environ.get("DISCORD_WEBHOOK_URL")
JST = timezone(timedelta(hours=9), 'JST')

# ==========================================
# 1. スクレイパー
# ==========================================
class BoatRaceScraperV4:
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
                time.sleep(1) # エラー時は少し待つ
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

    # 時刻表だけを先にチェックする軽量メソッド
    def get_target_races_for_course(self, course, date_str, now_dt):
        jcd = self.COURSE_MAP[course]
        url = f"{self.LIST_URL}?jcd={jcd}&hd={date_str}"
        soup = self._get_soup(url)
        targets = []
        
        if not soup: return []

        # ページ内の全レースの締切時刻を探す
        # テーブル構造から時刻を抽出
        # 通常、racelistページの各Rのヘッダー付近に時刻がある
        # 簡易的にテキスト全体から "1R ... 10:52" のようなパターンを探すのは困難なため
        # HTML構造（tbody）から順番に時間を抜く
        
        bodies = soup.select("tbody") 
        # 出走表は通常12個のtbodyで構成される (Rごとの塊)
        
        current_r = 1
        for b in bodies:
            text = b.get_text().replace("\n", " ")
            # "締切予定 10:30" を探す
            m = re.search(r"締切予定.*?(\d{1,2}:\d{2})", text)
            if m:
                time_str = m.group(1).zfill(5)
                race_dt_str = f"{date_str} {time_str}"
                try:
                    race_dt = datetime.strptime(race_dt_str, "%Y%m%d %H:%M").replace(tzinfo=JST)
                    diff = race_dt - now_dt
                    minutes = diff.total_seconds() / 60
                    
                    # 【重要】 ここでフィルタリング！
                    # 締切まで 10分〜35分 のレースだけをリストに追加
                    if 10 <= minutes <= 35:
                        targets.append(current_r)
                except:
                    pass
            current_r += 1
            if current_r > 12: break
            
        return targets

    def fetch_race_data(self, course, rno, date_str):
        jcd = self.COURSE_MAP[course]
        try:
            # 1. 出走表
            soup_list = self._get_soup(f"{self.LIST_URL}?rno={rno}&jcd={jcd}&hd={date_str}")
            if not soup_list: return None
            
            deadline_str = "00:00"
            text_full = soup_list.get_text()
            match_time = re.search(r"締切予定.*?(\d{1,2}:\d{2})", text_full)
            if match_time: deadline_str = match_time.group(1).zfill(5)
            
            bodies = soup_list.select("tbody.is-fs12")
            if not bodies: bodies = soup_list.select("tbody")
            
            row1 = None
            for b_idx, b in enumerate(bodies):
                if "１" in b.text[:10]: row1 = b; break
            if not row1: return None

            rank_1, win_rate_1 = "B2", 0.0
            rank_match = re.search(r"/ ([AB][12])", row1.text)
            if rank_match: rank_1 = rank_match.group(1)
            
            td_texts = [td.text.strip().replace("\n", " ") for td in row1.find_all("td")]
            all_rates = re.findall(r"(\d\.\d{2})", " ".join(td_texts))
            if all_rates: win_rate_1 = float(all_rates[0])

            # 2. 直前情報
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
            ex_times, st_list = [], []
            for i in range(6):
                tds = rows[i].select("td")
                if len(tds) < 5 or not tds[4].text.strip(): return None
                ex_times.append(float(tds[4].text.strip()))
                st_text = tds[2].select_one(".is-fs11").text.strip() if tds[2].select_one(".is-fs11") else "0.00"
                st_list.append(float(re.search(r"(\.\d+)", st_text).group(1)) if re.search(r"(\.\d+)", st_text) else 0.0)

            ex_rank = pd.Series(ex_times).rank(method="min").tolist()
            
            data = {
                "wind_speed": wind_speed, "wave": wave, 
                "ex_rank_1": ex_rank[0], "rank_1": rank_1, "win_rate_1": win_rate_1,
                "deadline": deadline_str 
            }
            for i in range(6):
                data[f"st_{i+1}"] = st_list[i]
                data[f"ex_time_{i+1}"] = ex_times[i]
            return data
        except: return None

# ==========================================
# 2. 予測ロジック (スナイパーモード)
# ==========================================
def predict_single(model, config, scraper, course, rno, date_str):
    try:
        race_data = scraper.fetch_race_data(course, rno, date_str)
        if not race_data: return None, -1
        
        # モデル入力用データの作成
        rank_map = {"A1": 4, "A2": 3, "B1": 2, "B2": 1}
        rank_val_1 = rank_map.get(race_data["rank_1"], 2)
        # 1号艇が低級別かつ展示が悪い(4位以下)場合にデバフ判定
        is_debuff_1 = 1 if (rank_val_1 <= 2 and race_data["ex_rank_1"] >= 4) else 0
        
        input_data = race_data.copy()
        input_data["rank_val_1"] = rank_val_1
        input_data["is_debuff_1"] = is_debuff_1
        
        # 予測実行 (イン飛び確率を算出)
        input_df = pd.DataFrame([input_data])[config["features"]]
        prob = model.predict(input_df)[0]
        
        # スナイパー(軸)の選定: 2〜6号艇の中で展示タイムが速い順にソート
        ex_times_26 = {i: race_data[f"ex_time_{i}"] for i in range(2, 7)}
        # タイムが速い順（昇順）に並び替えて、艇番号だけのリストを作る
        sorted_boats = sorted(ex_times_26.items(), key=lambda x: x[1])
        top3_list = [x[0] for x in sorted_boats[:3]] # 上位3つの艇番号を取得
        
        sniper_boat = top3_list[0] # 展示1位をメインスナイパーに

        # 根拠の整理
        reason = []
        if is_debuff_1: reason.append("地力デバフ(B級)")
        if race_data["ex_rank_1"] >= 5: reason.append(f"1号艇展示{int(race_data['ex_rank_1'])}位(致命的)")
        if race_data["wind_speed"] >= 5: reason.append(f"強風({race_data['wind_speed']}m)")
        reason_str = " / ".join(reason) if reason else "展示・級別バランス崩壊"

        # レスポンス辞書の作成
        res_dict = {
            "場名": course, 
            "レース": f"{rno}R", 
            "締切": race_data['deadline'],
            "確率": prob, 
            "スナイパー": f"{sniper_boat}号艇",
            "top3": top3_list,          # ←【追加】通知で2位、3位を出すために必要
            "top3_times": [sorted_boats[0][1], sorted_boats[1][1], sorted_boats[2][1]], # ←【追加】タイムも出すなら
            "1級別": race_data["rank_1"],
            "根拠": reason_str,
            "買い目": f"{sniper_boat}-全-全 (万舟狙い)"
        }

        # しきい値（ボーダー 0.570）を超えているか判定
        # configに無い場合は直接 0.570 を使用
        border = config.get("best_threshold", 0.570)
        if prob >= border:
            return res_dict, 1
        return res_dict, 0
        
    except Exception:
        return None, -2
        
# ==========================================
# 3. メイン実行 (超効率化・安全版)
# ==========================================
def run_github_patrol():
    print("👮 Smart Patrol Starting (JST)...")
    
    model_path = Path("boatrace_model_v3.pkl")
    config_path = Path("model_config.pkl")
    
    if not model_path.exists():
        print("Error: Model files not found.")
        return

    with open(model_path, "rb") as f: model = pickle.load(f)
    with open(config_path, "rb") as f: config = pickle.load(f)

    scraper = BoatRaceScraperV4()
    
    now_jst = datetime.now(JST)
    date_str = now_jst.strftime("%Y%m%d")
    print(f"Current Time: {now_jst.strftime('%H:%M')}")

    # 1. 開催場を取得
    courses = scraper.fetch_active_courses(date_str)
    if not courses:
        print("No races today.")
        return
    print(f"Active Courses: {len(courses)} venues")
    
    hits = []

    # 2. 会場ごとに「今やるべきレース」だけをリストアップ
    for course in courses:
        time.sleep(1) # 会場ごとのアクセス間隔は1秒あける（安全策）
        
        # 時刻表をチェックして、対象レース番号(R)を取得
        # ここでアクセスするのは1ページだけ！
        target_races = scraper.get_target_races_for_course(course, date_str, now_jst)
        
        if target_races:
            print(f"Checking {course}: Race {target_races}")
            
            for rno in target_races:
                time.sleep(1) # レースごとのアクセス間隔
                
                # 対象レースだけ詳細データを取得して予測
                res, status = predict_single(model, config, scraper, course, rno, date_str)
                
                if status == 1 and res:
                    print(f"Found HIT! {course} {rno}R")
                    hits.append(res)
        else:
            # 対象レースがない場合はスルー（ログ節約のため表示しないか、ドットだけ出す）
            print(f"{course}: No target races now.")

    # 3. 通知セクション
if hits:
    hits.sort(key=lambda x: x['締切'])
    border = config.get("best_threshold", 0.570)
    
    for r in hits:
        try:
            # 変数の安全な取り出し（辞書にキーがない場合の備え）
            top3 = r.get('top3', [0, 0, 0])
            times = r.get('top3_times', [0.0, 0.0, 0.0])
            
            b1, b2, b3 = top3[0], top3[1], top3[2]
            t1, t2, t3 = times[0], times[1], times[2]
            
            prob = r.get('確率', 0.0)
            rank_label = "SSS" if prob >= 0.65 else "S" if prob >= 0.60 else "A"
            
            # --- メッセージ組み立て ---
            content = f"🚀 **スナイパーモード：多段構え戦略**\n"
            content += f"【{rank_label}ランク】(イン飛び確率: `{prob:.3f}` / Border: {border:.3f})\n"
            content += f"📍 **{r['場名']} {r['レース']}** (締切 {r['締切']})\n"
            content += f"━━━━━━━━━━━━━━━━━━━━\n"
            
            content += f"🕵️ イン不安要素: {r.get('根拠', '展示・級別バランス崩壊')}\n"
            content += f"🥇 **展示1位(軸): {b1}号艇** ({t1})\n"
            content += f"🥈 **展示2位(軸): {b2}号艇** ({t2})\n"
            content += f"🥉 **展示3位(軸): {b3}号艇** ({t3})\n"
            
            content += f"\n💰 **資金配分プラン (予算上限: 6,000円)**\n"
            content += f"①【スナイパー：20点】(2,000円)\n"
            content += f"🎯 `{b1} — 全 — 全` (100円) → **20倍以上で勝ち**\n\n"
            
            content += f"②【ダブル軸：40点】(4,000円)\n"
            content += f"🎯 `{b1},{b2} — 全 — 全` (100円) → **40倍以上で勝ち**\n\n"
            
            content += f"③【フルカバー：60点】(6,000円)\n"
            content += f"🎯 `{b1},{b2},{b3} — 全 — 全` (100円) → **60倍以上で勝ち**\n"
            content += f"⚠️ *低配当時はトリガミ注意！*\n\n"
            
            content += f"🔥 **【厚盛り・絞り：8点】(5,600円)**\n"
            content += f"🎯 `{b1},{b2} — {b1},{b2} — 全` (700円)\n"
            content += f"💡 *1点あたりを厚く張るならこれ！*\n"
            content += "━━━━━━━━━━━━━━━━━━━━\n"
            
            if DISCORD_WEBHOOK_URL:
                requests.post(DISCORD_WEBHOOK_URL, json={"content": content})
                
        except Exception as e:
            print(f"通知生成エラー (スキップします): {e}")
            continue

if __name__ == "__main__":
    run_github_patrol()
