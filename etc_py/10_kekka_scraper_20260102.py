# -*- coding: utf-8 -*-
# 結果収集！

from __future__ import annotations

# ───────────────────────────────
# ▼ ここにパラメータをまとめて設定
# ───────────────────────────────
# RACE_DATE は main() で実行時にターミナルから入力する
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
from selenium.webdriver.edge.options import Options
from selenium.webdriver.edge.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import JavascriptException, TimeoutException

# ───────────────────────────────
# ▼ ベースフォルダ / 出力先フォルダ / credentials.ini の場所
# ───────────────────────────────
# このファイル: my_python_cursor/keiba_yosou_2025/netkeiba_entry_scraper_20251130.py
# BASE_DIR   : my_python_cursor
BASE_DIR = Path(__file__).resolve().parent.parent

# racedata_results.xlsx を保存するフォルダ
OUTPUT_DIR = BASE_DIR / "data" / "master"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# credentials.ini の場所
CREDENTIALS_PATH = BASE_DIR / "config" / "credentials.ini"

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
    # Edge/Chromium の内部ログが標準エラーに大量出力されるのを抑制する
    opts.add_argument("--log-level=3")
    opts.add_argument("--disable-logging")
    opts.add_argument("--v=0")
    opts.add_experimental_option("excludeSwitches", ["enable-logging"])

    # EdgeDriver 側のログも不要なので OS の null デバイスへ捨てる
    service = Service(log_output=os.devnull)
    # service = Service(edge_path, log_output=os.devnull)  # 明示的にServiceを使う場合
    return webdriver.Edge(service=service, options=opts)

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
# ④-補助: HTMLから race_id を抽出（診断用）
# ───────────────────────────────
def extract_race_ids_from_html(html: str) -> List[str]:
    """HTML全体に含まれる race_id を返す。診断用であり本番選別には使わない。"""
    ids: set[str] = set()
    # 1) <span id="myrace_XXXXXXXXXXXX"> 由来
    for m in re.findall(r'id=["\']myrace_(\d{12})["\']', html):
        ids.add(m)
    # 2) <a href="...race_id=XXXXXXXXXXXX..."> 由来
    for m in re.findall(r'race_id=(\d{12})', html):
        ids.add(m)
    return sorted(ids)


def extract_visible_race_ids(driver: webdriver.Edge) -> List[str]:
    """
    現在ブラウザ上で表示対象になっているレース要素だけから race_id を取得する。

    race.sp.netkeiba.com の page_source には、指定日以外の前後日レースが
    非表示DOMとして同居することがある。そのため page_source 全体への正規表現は
    本番の race_id 選別には使用しない。
    """
    script = r"""
        const ids = new Set();
        const elements = document.querySelectorAll(
            '[id^="myrace_"], a[href*="race_id="]'
        );

        for (const el of elements) {
            const style = window.getComputedStyle(el);
            if (style.display === 'none' || style.visibility === 'hidden') {
                continue;
            }

            // 親要素が display:none の場合も getClientRects() は空になる。
            if (el.getClientRects().length === 0) {
                continue;
            }

            const idAttr = el.getAttribute('id') || '';
            const href = el.getAttribute('href') || '';

            let match = idAttr.match(/^myrace_(\d{12})$/);
            if (!match) {
                match = href.match(/[?&]race_id=(\d{12})(?:&|$)/);
            }
            if (match) {
                ids.add(match[1]);
            }
        }
        return Array.from(ids).sort();
    """
    result = driver.execute_script(script)
    return [str(rid) for rid in (result or []) if re.fullmatch(r"\d{12}", str(rid))]


def validate_visible_race_ids(
    visible_race_ids: List[str],
    all_html_race_ids: List[str],
    race_date: str,
    current_url: str,
) -> None:
    """
    表示対象IDとHTML全体IDの差を検査する。

    36件を固定上限にはしない。JRA開催形態の変更に耐えるため、
    まず「表示対象」と「HTML全体」の差をログ化し、表示対象だけを採用する。
    ただし表示対象が48件を超える場合は、複数日混入の疑いが強いため停止する。
    """
    print(f"[INFO] target_date={race_date}")
    print(f"[INFO] current_url={current_url}")
    print(f"[INFO] html_all_race_count={len(all_html_race_ids)}")
    print(f"[INFO] visible_race_count={len(visible_race_ids)}")
    print(f"[INFO] visible_race_ids_first10={visible_race_ids[:10]}")

    hidden_ids = sorted(set(all_html_race_ids) - set(visible_race_ids))
    if hidden_ids:
        print(
            f"[INFO] hidden_or_other_date_race_count={len(hidden_ids)} "
            f"preview={hidden_ids[:10]}"
        )

    if len(visible_race_ids) > 48:
        raise RuntimeError(
            "表示対象のrace_idが多すぎます。複数開催日が同時に表示対象へ"
            "混入している可能性があるため処理を停止します。"
            f" target_date={race_date}"
            f" visible_count={len(visible_race_ids)}"
            f" html_all_count={len(all_html_race_ids)}"
            f" preview={visible_race_ids[:20]}"
        )


def validate_race_ids_not_used_on_other_dates(
    race_ids: List[str],
    race_date: str,
    workbook_path: Path,
) -> None:
    """同じrace_idが別日付シートですでに保存済みなら停止する。"""
    if not workbook_path.exists() or not race_ids:
        return

    race_id_set = {str(rid) for rid in race_ids}
    xls = pd.ExcelFile(workbook_path, engine="openpyxl")
    conflicts: list[tuple[str, str]] = []
    try:
        for sheet in xls.sheet_names:
            sheet_str = str(sheet)
            if sheet_str == str(race_date) or not re.fullmatch(r"\d{8}", sheet_str):
                continue
            try:
                df = pd.read_excel(xls, sheet_name=sheet, usecols=lambda c: str(c).replace(" ", "") in {"レースID", "ﾚｰｽID"})
            except Exception:
                df = pd.read_excel(xls, sheet_name=sheet)
            race_col = next((c for c in df.columns if str(c).replace(" ", "") in {"レースID", "ﾚｰｽID"}), None)
            if race_col is None:
                continue
            existing = set(df[race_col].dropna().astype(str).str.replace(r"\.0$", "", regex=True))
            for rid in sorted(race_id_set & existing):
                conflicts.append((sheet_str, rid))
    finally:
        xls.close()

    if conflicts:
        preview = ", ".join(f"{date}:{rid}" for date, rid in conflicts[:20])
        raise RuntimeError(
            "取得したrace_idが別開催日シートですでに使用されています。"
            " 誤ったレース一覧を保存する事故を防ぐため処理を停止します。"
            f" target_date={race_date} conflicts={len(conflicts)} preview={preview}"
        )

# ───────────────────────────────
# ④ レースID 一括取得（表示対象DOMだけを採用）
# ───────────────────────────────
JS_READY_STATE = "return document.readyState === 'complete';"
JS_SCROLL_HEIGHT = "return document.documentElement.scrollHeight;"
JS_SCROLL_TO = "window.scrollTo(0, arguments[0]);"

def get_race_ids_for_date(
    driver: webdriver.Edge,
    race_list_url: str,
    race_date: str,
) -> List[str]:
    print(f"[INFO] requested_url={race_list_url}")
    driver.get(race_list_url)

    # レース一覧は広告などの影響で readyState が complete にならない場合がある。
    # タイムアウトしても、取得済みDOMを使って処理を続ける。
    try:
        WebDriverWait(driver, 15).until(
            lambda d: d.execute_script(JS_READY_STATE)
        )
    except TimeoutException:
        print("⚠️ ページ読み込み完了待ちが時間切れになりました。取得済みDOMを確認します。")

    race_ids: List[str] = []
    wait_limit = time.monotonic() + 20
    while time.monotonic() < wait_limit:
        try:
            race_ids = extract_visible_race_ids(driver)
        except JavascriptException:
            race_ids = []
        if race_ids:
            break
        time.sleep(0.5)

    # 遅延読み込み対策。HTML全体の race_id へはフォールバックしない。
    if not race_ids:
        last_height = -1
        for _ in range(SCROLL_MAX):
            try:
                height = driver.execute_script(JS_SCROLL_HEIGHT)
                driver.execute_script(JS_SCROLL_TO, height)
            except JavascriptException:
                break

            time.sleep(SCROLL_PAUSE)
            try:
                race_ids = extract_visible_race_ids(driver)
            except JavascriptException:
                race_ids = []
            if race_ids:
                break
            if height == last_height:
                break
            last_height = height

    if not race_ids:
        page_text = bs(driver.page_source, "html.parser").get_text(
            " ", strip=True
        )
        if "ログイン" in page_text and "パスワード" in page_text:
            reason = "ログイン画面へ戻されています"
        elif "HTTP ERROR 400" in page_text:
            reason = "レース一覧URLがHTTP 400エラーを返しました"
        elif any(word in page_text.lower() for word in ("access denied", "cloudflare")):
            reason = "アクセス制限ページが表示されています"
        else:
            reason = "対象日に開催レースがないか、表示対象レース一覧を取得できませんでした"
        print(f"⚠️ {reason}。URL: {driver.current_url}", file=sys.stderr)
        return []

    # 診断用にHTML全体のIDも数えるが、本番採用は visible_race_ids のみ。
    all_html_race_ids = extract_race_ids_from_html(driver.page_source)
    validate_visible_race_ids(
        race_ids,
        all_html_race_ids,
        race_date,
        driver.current_url,
    )

    if len(race_ids) < 5:
        print(
            "⚠️ 取得レースIDが少なすぎます。"
            "Cloudflare ブロックや開催日ミスの可能性があります。",
            file=sys.stderr,
        )

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
    race_date = input("対象レース日付を YYYYMMDD 形式で入力してください: ").strip()
    if not re.fullmatch(r"\d{8}", race_date):
        raise ValueError("対象レース日付は YYYYMMDD の8桁で入力してください")

    # PC版の旧URLはHTTP 400を返すため、現在利用できるレース一覧URLを使う。
    race_list_url = (
        "https://race.sp.netkeiba.com/"
        f"?kaisai_date={race_date}&pid=race_list"
    )
    user, pw = load_credentials()
    driver = setup_browser()
    try:
        login(driver, user, pw)
        race_ids = get_race_ids_for_date(driver, race_list_url, race_date)
        print(f"▶ レースID取得: {len(race_ids)} 件")

        # 既存の別日付シートで同じrace_idが使われていないことを保存前に強制確認する。
        validate_race_ids_not_used_on_other_dates(
            race_ids,
            race_date,
            OUTPUT_DIR / "racedata_results.xlsx",
        )

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
        save_to_excel(df_all, "racedata_results.xlsx", race_date)
    finally:
        driver.quit()

if __name__ == "__main__":
    main()
