# -*- coding: utf-8 -*-
"""
JRA オッズ一括スクレイパー NAS Docker対応版

取得内容:
    date, racecourse, race, name, bet_type, combination, odds

主な変更点:
    1. Windows固定パスを廃止
    2. NAS Dockerでは /workspace/data/ozzu_csv にCSV保存
    3. 日付をコマンド引数で指定可能
    4. 指定がなければ当日 YYYYMMDD を使用
    5. Docker/Playwright用の起動オプションを追加
    6. 既存のスクレイピング方法は基本そのまま維持

実行例:
    python3 02_scrape_jra_odds_2.py 20260606
    python3 02_scrape_jra_odds_2.py
    python3 02_scrape_jra_odds_2.py 20260606 --show

NAS Docker実行例:
    sudo docker run --rm -v /volume1/docker/keiba_yosou_2026:/workspace -w /workspace/app keiba-yosou-python:latest python3 02_scrape_jra_odds_2.py 20260606
"""

import argparse
import csv
import datetime
import os
import re
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional

from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from playwright.sync_api import sync_playwright


# ============================================================
# 基本設定
# ============================================================

DEFAULT_HEADLESS = True

JRA_TOP_URL = "https://www.jra.go.jp/"

DEFAULT_TIMEOUT_MS = 60_000
SHORT_TIMEOUT_MS = 10_000

CSV_HEADERS = [
    "date",
    "racecourse",
    "race",
    "name",
    "bet_type",
    "combination",
    "odds",
]

# Docker / NASでChromiumを安定させるための引数
CHROMIUM_ARGS = [
    "--no-sandbox",
    "--disable-dev-shm-usage",
    "--disable-gpu",
    "--disable-setuid-sandbox",
    "--window-size=1400,900",
    "--lang=ja-JP",
]


# ============================================================
# パス設定
# ============================================================

def get_base_dir() -> Path:
    """
    実行環境に応じてプロジェクト基準フォルダを決める。

    優先順位:
        1. 環境変数 KEIBA_BASE_DIR
        2. Docker内の /workspace
        3. このファイルの親フォルダの1つ上
           例:
             app/02_scrape_jra_odds_2.py
             → プロジェクトルート
    """
    env_base = os.getenv("KEIBA_BASE_DIR")
    if env_base:
        return Path(env_base).expanduser().resolve()

    docker_base = Path("/workspace")
    if docker_base.exists():
        return docker_base

    # Windowsなどで app フォルダ内から直接実行する場合
    return Path(__file__).resolve().parent.parent


BASE_DIR = get_base_dir()
APP_DIR = BASE_DIR / "app"
DATA_DIR = BASE_DIR / "data"
OZZU_DIR = DATA_DIR / "ozzu_csv"
LOG_DIR = BASE_DIR / "logs"

for directory in [DATA_DIR, OZZU_DIR, LOG_DIR]:
    directory.mkdir(parents=True, exist_ok=True)


# ============================================================
# 引数処理
# ============================================================

def normalize_argv(argv: List[str]) -> List[str]:
    """
    旧コード互換のため、裸の show を --show に置き換える。

    例:
        python3 02_scrape_jra_odds_2.py show
        python3 02_scrape_jra_odds_2.py 20260606 show
    """
    normalized = []
    for arg in argv:
        if arg.lower() == "show":
            normalized.append("--show")
        else:
            normalized.append(arg)
    return normalized


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="JRAオッズを取得してCSVに保存します。NAS Docker対応版。"
    )

    parser.add_argument(
        "raceday",
        nargs="?",
        default=None,
        help="対象日付。例: 20260606。省略時は当日。",
    )

    parser.add_argument(
        "--out-dir",
        default=None,
        help="CSV出力先フォルダ。省略時は /workspace/data/ozzu_csv。",
    )

    parser.add_argument(
        "--show",
        action="store_true",
        help="ブラウザを表示します。NAS Dockerでは通常使いません。",
    )

    parser.add_argument(
        "--headless",
        action="store_true",
        help="明示的にヘッドレス実行します。",
    )

    parser.add_argument(
        "--timeout",
        type=int,
        default=DEFAULT_TIMEOUT_MS,
        help="ページ遷移タイムアウト。ミリ秒。",
    )

    args = parser.parse_args(normalize_argv(sys.argv[1:]))

    # --show と --headless が両方指定された場合は headless を優先
    if args.headless:
        args.show = False

    return args


def get_raceday(raw_raceday: Optional[str]) -> str:
    """
    レース日を YYYYMMDD で返す。

    優先順位:
        1. コマンド引数
        2. 環境変数 RACEDAY
        3. 今日の日付
    """
    if raw_raceday:
        raceday = raw_raceday.strip()
    else:
        raceday = os.getenv("RACEDAY", "").strip()

    if not raceday:
        raceday = datetime.datetime.now().strftime("%Y%m%d")

    if not re.fullmatch(r"\d{8}", raceday):
        raise ValueError(
            f"日付形式が不正です: {raceday}。YYYYMMDD形式で指定してください。例: 20260606"
        )

    return raceday


# ============================================================
# ユーティリティ
# ============================================================

_re_num = re.compile(r"[^\d.\-]")
_re_place = re.compile(r"回([^\d]+?)\d")


def num(text: str) -> str:
    """
    オッズ文字列から数値・ピリオド・ハイフンのみ残す。
    例:
        ' 1.8 - 2.6 ' → '1.8-2.6'
        '---' → '---'
    """
    if text is None:
        return ""
    return _re_num.sub("", str(text))


def normalize_place(raw: str) -> str:
    """
    開催表示を競馬場名へ正規化する。

    例:
        2回函館9日 → 函館
        1回東京1日 → 東京
    """
    raw = raw.strip()
    m = _re_place.search(raw)
    return m.group(1) if m else re.sub(r"[0-9回日]", "", raw)


def safe_inner_text(locator, default: str = "") -> str:
    """
    Playwright locator の inner_text を安全に取得する。
    """
    try:
        if locator.count() == 0:
            return default
        return locator.inner_text().strip()
    except Exception:
        return default


def print_environment(raceday: str, out_csv: Path, headless: bool) -> None:
    print("=" * 80)
    print("[INFO] JRA odds scraper NAS Docker version")
    print(f"[INFO] BASE_DIR  : {BASE_DIR}")
    print(f"[INFO] APP_DIR   : {APP_DIR}")
    print(f"[INFO] DATA_DIR  : {DATA_DIR}")
    print(f"[INFO] OZZU_DIR  : {OZZU_DIR}")
    print(f"[INFO] LOG_DIR   : {LOG_DIR}")
    print(f"[INFO] RACEDAY   : {raceday}")
    print(f"[INFO] OUT_CSV   : {out_csv}")
    print(f"[INFO] HEADLESS  : {headless}")
    print("=" * 80)


# ============================================================
# 単勝・複勝ページパーサ
# ============================================================

def parse_tanpuku(page) -> List[Dict[str, str]]:
    """
    単勝・複勝ページから以下の形式で返す。

    [
        {
            "num": "3",
            "name": "セイウン",
            "tan": "4.2",
            "fuku": "1.8-2.6",
        },
        ...
    ]
    """
    rows = page.locator("table.tanpuku tr")
    result: List[Dict[str, str]] = []

    row_count = rows.count()

    for i in range(row_count):
        row = rows.nth(i)

        # 馬番
        num_cell = row.locator("td.num")
        if num_cell.count() == 0:
            continue

        horse_no = safe_inner_text(num_cell)
        if not horse_no:
            continue

        # 馬名
        name_cell = row.locator("a").first

        if name_cell.count() > 0:
            horse_name = safe_inner_text(name_cell)
        else:
            # fallback:
            # num, odds_tan, odds_fuku 以外のセルを馬名候補として探す
            horse_name = ""
            cells = row.locator("td")
            for j in range(cells.count()):
                cell = cells.nth(j)
                try:
                    class_name = str(cell.evaluate("el => el.className || ''"))
                except Exception:
                    class_name = ""

                if class_name.startswith(("num", "odds_tan", "odds_fuku")):
                    continue

                candidate = safe_inner_text(cell)
                if candidate:
                    horse_name = candidate
                    break

        tan_odds = safe_inner_text(row.locator("td.odds_tan"))
        fuku_odds = safe_inner_text(row.locator("td.odds_fuku"))

        result.append(
            {
                "num": horse_no,
                "name": horse_name,
                "tan": num(tan_odds),
                "fuku": num(fuku_odds),
            }
        )

    return result


# ============================================================
# 3連複ページパーサ
# ============================================================

def parse_trio(page) -> Dict[str, str]:
    """
    3連複ページから以下の形式で返す。

    {
        "1-2-3": "12.3",
        "1-2-4": "15.6",
        ...
    }
    """
    trio: Dict[str, str] = {}

    tables = page.locator("ul.fuku3_list table.basic")
    table_count = tables.count()

    for t in range(table_count):
        table = tables.nth(t)

        cap = safe_inner_text(table.locator("caption"))
        if not cap:
            continue

        rows = table.locator("tr")
        row_count = rows.count()

        for r in range(row_count):
            row = rows.nth(r)
            third = safe_inner_text(row.locator("th"))
            odd = safe_inner_text(row.locator("td"))

            if not third:
                continue

            trio[f"{cap}-{third}"] = num(odd)

    return trio


# ============================================================
# メインスクレイパー
# ============================================================

def scrape_odds(
    raceday: str,
    headless: bool = DEFAULT_HEADLESS,
    out_dir: Path = OZZU_DIR,
    timeout_ms: int = DEFAULT_TIMEOUT_MS,
) -> Path:
    """
    JRA公式サイトからオッズを取得してCSV保存する。

    スクレイピング方法は既存コードを基本維持:
        TOP
        → オッズ
        → 開催場
        → 各レース
        → 単勝・複勝
        → 3連複
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    out_csv = out_dir / f"OZZU_{raceday}.csv"

    print_environment(raceday=raceday, out_csv=out_csv, headless=headless)

    with sync_playwright() as pw:
        browser = pw.chromium.launch(
            headless=headless,
            args=CHROMIUM_ARGS,
        )

        page = browser.new_page(
            viewport={"width": 1400, "height": 900},
            locale="ja-JP",
            timezone_id="Asia/Tokyo",
        )

        page.set_default_timeout(timeout_ms)
        page.set_default_navigation_timeout(timeout_ms)

        try:
            with open(out_csv, "w", newline="", encoding="utf-8-sig") as fp:
                writer = csv.writer(fp)
                writer.writerow(CSV_HEADERS)

                # ------------------------------------------------------------
                # 1. TOP → オッズ
                # ------------------------------------------------------------
                print("[INFO] JRAトップページへアクセスします")
                page.goto(JRA_TOP_URL, timeout=timeout_ms, wait_until="domcontentloaded")

                print("[INFO] オッズページへ移動します")
                page.locator("#quick_menu a[onclick*='accessO.html']").first.click(force=True)
                page.wait_for_load_state("domcontentloaded")

                # ------------------------------------------------------------
                # 2. 開催場リンク
                # ------------------------------------------------------------
                place_links = page.locator("div.link_list a[onclick*='accessO.html']")
                place_count = place_links.count()

                print(f"[INFO] 開催場リンク数: {place_count}")

                if place_count == 0:
                    print("[WARN] 開催場リンクが見つかりません。開催日ではない可能性があります。")

                for idx in range(place_count):
                    # ページを戻るたびにlocatorを取り直す
                    place_links = page.locator("div.link_list a[onclick*='accessO.html']")
                    link = place_links.nth(idx)

                    raw_place = safe_inner_text(link)
                    place_label = normalize_place(raw_place)

                    print("=" * 80)
                    print(f"[INFO] 開催場 {idx + 1}/{place_count}: {raw_place} -> {place_label}")
                    print("=" * 80)

                    link.click()
                    page.wait_for_load_state("domcontentloaded")

                    # --------------------------------------------------------
                    # 3. レース行ループ
                    # --------------------------------------------------------
                    for r in range(1, 13):
                        race_lbl = f"{r}R"

                        row = page.locator(
                            f"tr:has(th.race_num img[alt='{r}レース'])"
                        ).first

                        if row.count() == 0:
                            print(f"[INFO] {place_label} {race_lbl}: 行なし。スキップ")
                            continue

                        print(f"[INFO] {place_label} {race_lbl}: 処理開始")

                        # ----------------------------------------------------
                        # 単勝 + 複勝
                        # ----------------------------------------------------
                        tan_btn = row.locator("div.tanpuku a").first

                        if tan_btn.count() > 0:
                            try:
                                print(f"[INFO] {place_label} {race_lbl}: 単勝・複勝取得")
                                tan_btn.click()
                                page.wait_for_selector(
                                    "table.tanpuku td.num",
                                    timeout=SHORT_TIMEOUT_MS,
                                )

                                horses = parse_tanpuku(page)

                                for h in horses:
                                    writer.writerow(
                                        [
                                            raceday,
                                            place_label,
                                            race_lbl,
                                            h["name"],
                                            "単勝",
                                            h["num"],
                                            h["tan"],
                                        ]
                                    )

                                for h in horses:
                                    writer.writerow(
                                        [
                                            raceday,
                                            place_label,
                                            race_lbl,
                                            h["name"],
                                            "複勝",
                                            h["num"],
                                            h["fuku"],
                                        ]
                                    )

                                print(
                                    f"[INFO] {place_label} {race_lbl}: "
                                    f"単勝・複勝 {len(horses)}頭分 取得"
                                )

                            except PlaywrightTimeoutError as e:
                                print(f"[WARN] {place_label} {race_lbl}: 単勝・複勝タイムアウト: {e}")

                            finally:
                                page.go_back()
                                page.wait_for_load_state("domcontentloaded")
                                time.sleep(0.2)

                        else:
                            print(f"[INFO] {place_label} {race_lbl}: 単勝・複勝ボタンなし")

                        # 戻った後なのでrowを取り直す
                        row = page.locator(
                            f"tr:has(th.race_num img[alt='{r}レース'])"
                        ).first

                        if row.count() == 0:
                            print(f"[WARN] {place_label} {race_lbl}: 戻った後に行が見つかりません")
                            continue

                        # ----------------------------------------------------
                        # 3連複
                        # ----------------------------------------------------
                        trio_btn = row.locator("div.trio a").first

                        if trio_btn.count() > 0:
                            try:
                                print(f"[INFO] {place_label} {race_lbl}: 3連複取得")
                                trio_btn.click()
                                page.wait_for_selector(
                                    "ul.fuku3_list",
                                    timeout=SHORT_TIMEOUT_MS,
                                )

                                trio = parse_trio(page)

                                for comb, odd in trio.items():
                                    writer.writerow(
                                        [
                                            raceday,
                                            place_label,
                                            race_lbl,
                                            "",
                                            "3連複",
                                            comb,
                                            odd,
                                        ]
                                    )

                                print(
                                    f"[INFO] {place_label} {race_lbl}: "
                                    f"3連複 {len(trio)}件 取得"
                                )

                            except PlaywrightTimeoutError as e:
                                print(f"[WARN] {place_label} {race_lbl}: 3連複タイムアウト: {e}")

                            finally:
                                page.go_back()
                                page.wait_for_load_state("domcontentloaded")
                                time.sleep(0.2)

                        else:
                            print(f"[INFO] {place_label} {race_lbl}: 3連複ボタンなし")

                    # 開催場一覧へ戻る
                    page.go_back()
                    page.wait_for_load_state("domcontentloaded")
                    time.sleep(0.3)

        finally:
            browser.close()

    print("=" * 80)
    print(f"[OK] 収集完了: {out_csv}")
    print("=" * 80)

    return out_csv


# ============================================================
# 実行
# ============================================================

def main() -> int:
    args = parse_args()

    try:
        raceday = get_raceday(args.raceday)

        if args.out_dir:
            out_dir = Path(args.out_dir).expanduser().resolve()
        else:
            out_dir = OZZU_DIR

        headless = not args.show

        csv_file = scrape_odds(
            raceday=raceday,
            headless=headless,
            out_dir=out_dir,
            timeout_ms=args.timeout,
        )

        print(f"✅ 収集完了: {csv_file}")
        return 0

    except PlaywrightTimeoutError as e:
        print("🛑 タイムアウト:", e)
        return 1

    except KeyboardInterrupt:
        print("🛑 ユーザー操作により中断されました")
        return 130

    except Exception as e:
        print("🛑 予期せぬエラー:", e)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
