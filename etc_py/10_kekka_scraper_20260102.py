# -*- coding: utf-8 -*-
# 結果収集！

from __future__ import annotations

# ───────────────────────────────
# ▼ ここにパラメータをまとめて設定
# ───────────────────────────────
RACE_DATE = "20260503"    # 取得したい開催日 (YYYYMMDD) ← 必要に応じてここだけ変える
HEADLESS = True           # デバッグ時は False にするとブラウザが見える
SCROLL_PAUSE = 0.8
SCROLL_MAX = 20

# ───────────────────────────────
# ▼ 以下、ライブラリimportや各種設定
# ───────────────────────────────
import os
import time
import re
import sys
import io
from pathlib import Path
from typing import List, Tuple

import pandas as pd
import requests
from bs4 import BeautifulSoup as bs
from configparser import ConfigParser
from tqdm import tqdm

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.edge.options import Options
from selenium.webdriver.edge.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import JavascriptException, TimeoutException

# ───────────────────────────────
# ▼ ベースフォルダ / 出力先フォルダ / credentials.ini の場所
# ───────────────────────────────
# このファイル: my_python_cursor/keiba_yosou_2025/netkeiba_entry_scraper_20251130.py
# BASE_DIR   : my_python_cursor
BASE_DIR = Path(__file__).resolve().parent.parent

# racedata_results.xlsx を保存するフォルダ（固定パス）
OUTPUT_DIR = Path(r"C:\Users\okino\OneDrive\ドキュメント\my_python_cursor\keiba_yosou_2026\data\master")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# credentials.ini の場所（固定パス）
CREDENTIALS_PATH = Path(r"C:\Users\okino\OneDrive\ドキュメント\my_python_cursor\keiba_yosou_2026\config\credentials.ini")

# ───────────────────────────────
# ① 資格情報（パス固定）
# ───────────────────────────────
def load_credentials(path: Path | str = CREDENTIALS_PATH) -> Tuple[str, str]:

    cfg = ConfigParser()
    p = Path(path).resolve()

    # デバッグ用に現在見に行っているパスを表示
    print(f"🔍 credentials.ini を探すパス: {p}")

    if not p.exists():
        raise FileNotFoundError(f"credentials.ini が見つかりません: {p}")

    if not cfg.read(str(p), encoding="utf-8"):
        raise FileNotFoundError(f"credentials.ini を読み込めませんでした: {p}")

    user = cfg.get("netkeiba", "username", fallback=None)
    pw = cfg.get("netkeiba", "password", fallback=None)

    if not user or not pw:
        raise ValueError(
            "credentials.ini の [netkeiba] セクションに "
            "username / password が設定されていません"
        )
    return user, pw

# ───────────────────────────────
# ② ブラウザセットアップ
# ───────────────────────────────
def setup_browser() -> webdriver.Edge:
    # 必要に応じて msedgedriver.exe のパスを変更してください
    edge_path = r"C:\Users\okino\.wdm\drivers\edgedriver\win64\138.0.3351.83\msedgedriver.exe"  # 使わなくてもOK
    opts = Options()
    if HEADLESS:
        opts.add_argument("--headless=new")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--lang=ja")
    # service = Service(edge_path)  # 明示的にServiceを使う場合
    # return webdriver.Edge(service=service, options=opts)
    return webdriver.Edge(options=opts)

# ───────────────────────────────
# ③ netkeiba ログイン（JavaScript 直接操作版）
# ───────────────────────────────
def login(driver: webdriver.Edge, user: str, pw: str) -> None:
    driver.get("https://regist.netkeiba.com/account/?pid=login")
    # ▼JSで直接値を設定してクリック
    driver.execute_script(
        "document.querySelector('input[name=\"login_id\"]').value = arguments[0];",
        user,
    )
    driver.execute_script(
        "document.querySelector('input[name=\"pswd\"]').value = arguments[0];",
        pw,
    )
    driver.execute_script(
        "document.querySelector('input[type=\"image\"]').click();"
    )
    print("✅ netkeiba ログイン完了")

# ───────────────────────────────
# ④-補助: HTMLから race_id を堅牢に抽出
# ───────────────────────────────
def extract_race_ids_from_html(html: str) -> List[str]:
    ids: set[str] = set()
    # 1) <span id="myrace_XXXXXXXXXXXX"> 由来（一覧に必ず出る）
    for m in re.findall(r'id=["\']myrace_(\d{12})["\']', html):
        ids.add(m)
    # 2) <a href="...race/result.html?race_id=XXXXXXXXXXXX..."> 由来
    for m in re.findall(r'race_id=(\d{12})', html):
        ids.add(m)
    return sorted(ids)

# ───────────────────────────────
# ④ レースID 一括取得（待機強化＋抽出ロジック強化）
# ───────────────────────────────
JS_READY_STATE = "return document.readyState === 'complete';"
JS_SCROLL_HEIGHT = "return document.documentElement.scrollHeight;"
JS_SCROLL_TO = "window.scrollTo(0, arguments[0]);"

def get_race_ids_for_date(driver: webdriver.Edge, race_list_url: str) -> List[str]:
    driver.get(race_list_url)

    # ページ読み込み完了待ち
    WebDriverWait(driver, 15).until(lambda d: d.execute_script(JS_READY_STATE))

    # ▼クリック可能なレースリンク(<a>)が現れるまで待機
    anchor_selector = 'a[href*="race_id="]'
    try:
        WebDriverWait(driver, 20).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, anchor_selector))
        )
    except TimeoutException:
        # 念のためスクロールで読み込みを促す
        last_height = 0
        for _ in range(SCROLL_MAX):
            try:
                height = driver.execute_script(JS_SCROLL_HEIGHT)
            except JavascriptException:
                time.sleep(1.0)
                continue
            if height == last_height:
                break
            driver.execute_script(JS_SCROLL_TO, height)
            time.sleep(SCROLL_PAUSE)
            last_height = height
        # 最後にもう一度 element_to_be_clickable を待つ
        WebDriverWait(driver, 5).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, anchor_selector))
        )

    # HTML解析
    soup = bs(driver.page_source, "html.parser")
    html_text = str(soup)

    race_ids = extract_race_ids_from_html(html_text)

    if len(race_ids) < 5:
        print(
            "⚠️ 取得レースIDが少なすぎます。"
            "Cloudflare ブロックや開催日ミスの可能性があります。",
            file=sys.stderr,
        )

    # デバッグ表示（最初の数件）
    print("例:", race_ids[:5])
    return race_ids

# ───────────────────────────────
# ⑤ レース詳細スクレイピング
# ───────────────────────────────
def scrape_one_race(driver: webdriver.Edge, race_id: str) -> pd.DataFrame:
    result_url = (
        f"https://race.netkeiba.com/race/result.html?race_id={race_id}&rf=race_list"
    )
    driver.get(result_url)
    html_result = driver.page_source

    # 結果テーブル
    try:
        df_result = pd.read_html(io.StringIO(html_result), flavor="bs4")[0]
    except Exception as e:
        print(f"[結果] テーブル取得失敗 {race_id}: {e}")
        df_result = pd.DataFrame()
    df_result.reset_index(drop=True, inplace=True)

    # 払戻テーブル
    try:
        headers = {"User-Agent": "Mozilla/5.0"}
        r = requests.get(result_url, headers=headers, timeout=10)
        r.encoding = r.apparent_encoding
        soup_pay = bs(r.text, "html.parser")

        data_pay = []
        pay_targets = {
            "Tansho": "単勝",
            "Fukusho": "複勝",
            "Wakuren": "枠連",
            "Umaren": "馬連",
            "Wide": "ワイド",
            "Umatan": "馬単",
            "Fuku3": "3連複",
            "Tan3": "3連単",
        }

        for cls, name in pay_targets.items():
            tr = soup_pay.find("tr", class_=cls)
            if not tr:
                continue

            nums = [
                n.get_text(strip=True)
                for n in tr.select("td.Result span")
                if n.get_text(strip=True)
            ]

            pays = tr.find("td", class_="Payout")
            pays = pays.get_text("|", strip=True).split("|") if pays else []

            pops = tr.find("td", class_="Ninki")
            pops = pops.get_text("|", strip=True).split("|") if pops else []

            if name == "複勝":
                # 複勝だけはそのまま1頭ずつ
                pairings = nums
            else:
                step = 3 if name in ("3連複", "3連単") else 2
                sep = "→" if name in ("馬単", "3連単") else "-"
                pairings = [
                    sep.join(nums[i : i + step]) for i in range(0, len(nums), step)
                ]

            for i in range(len(pays)):
                data_pay.append(
                    {
                        "払戻種別": name,
                        "組番": pairings[i] if i < len(pairings) else "",
                        "払戻金": pays[i],
                        "人気": pops[i] if i < len(pops) else "",
                    }
                )

        df_pay = pd.DataFrame(data_pay)
        df_pay.reset_index(drop=True, inplace=True)

    except Exception as e:
        print(f"[払戻] 取得失敗 {race_id}: {e}")
        df_pay = pd.DataFrame()
        df_pay.reset_index(drop=True, inplace=True)

    # 出馬表
    shutuba_url = (
        f"https://race.netkeiba.com/race/shutuba.html?race_id={race_id}"
        "&rf=shutuba_submenu"
    )
    driver.get(shutuba_url)
    html_shutuba = driver.page_source
    try:
        tables = pd.read_html(io.StringIO(html_shutuba), flavor="bs4")
        # 「枠」列を持つテーブルを優先的に採用
        df_shutuba = next((t for t in tables if "枠" in t.columns[0]), tables[0])
    except Exception as e:
        print(f"[出馬表] 取得失敗 {race_id}: {e}")
        df_shutuba = pd.DataFrame()
    df_shutuba.reset_index(drop=True, inplace=True)

    # メタ情報
    meta_cols = {}
    try:
        soup_meta = bs(html_result, "html.parser")
        race_name = soup_meta.find("h1", class_="RaceName").get_text(strip=True)
        data01 = soup_meta.find("div", class_="RaceData01").get_text(strip=True)
        data02 = soup_meta.find("div", class_="RaceData02").get_text(strip=True)
        meta_cols = {
            "レース名": race_name,
            "レース情報": f"{data01} {data02} {race_name}",
        }
    except Exception:
        pass

    # 結合
    df_combined = pd.concat([df_result, df_pay, df_shutuba], axis=1)
    for k, v in meta_cols.items():
        df_combined[k] = v
    df_combined.insert(0, "レースID", race_id)
    return df_combined

# ───────────────────────────────
# ⑥ Excel 保存（保存先フォルダを OUTPUT_DIR に変更）
# ───────────────────────────────
def save_to_excel(df: pd.DataFrame, file_name: str, sheet_name: str) -> None:
    # ここで OUTPUT_DIR 配下に保存する
    path = OUTPUT_DIR / file_name
    mode = "a" if path.exists() else "w"
    writer_opts: dict = {"engine": "openpyxl", "mode": mode}
    if mode == "a":
        writer_opts["if_sheet_exists"] = "replace"
    with pd.ExcelWriter(path, **writer_opts) as w:
        df.to_excel(w, sheet_name=sheet_name, index=False)
    print(f"💾 {path} [{sheet_name}] 保存完了")

# ───────────────────────────────
# ⑦ メイン処理
# ───────────────────────────────
def main():
    race_list_url = (
        f"https://race.netkeiba.com/top/race_list.html?kaisai_date={RACE_DATE}"
    )
    user, pw = load_credentials()
    driver = setup_browser()
    try:
        login(driver, user, pw)
        race_ids = get_race_ids_for_date(driver, race_list_url)
        print(f"▶ レースID取得: {len(race_ids)} 件")

        all_dfs: List[pd.DataFrame] = []
        for rid in tqdm(race_ids, desc="各レース取得"):
            try:
                all_dfs.append(scrape_one_race(driver, rid))
            except TimeoutException:
                print(f"❌ Timeout: {rid}")

        if not all_dfs:
            print("⚠ データが1件も取得できませんでした")
            return

        df_all = pd.concat(all_dfs, ignore_index=True)
        # racedata_results.xlsx を OUTPUT_DIR に保存
        save_to_excel(df_all, "racedata_results.xlsx", RACE_DATE)
    finally:
        driver.quit()

if __name__ == "__main__":
    main()
