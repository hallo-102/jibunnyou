# -*- coding: utf-8 -*-
"""
JRA-IPAT 自動投票スクリプト
Excel「回収率重視_買い目候補」シートから
3連複・軸2頭ながし・相手3頭を購入予定リストへ入れるコード。

【重要】
- 最初は DRY_RUN = True のまま動作確認してください。
- DRY_RUN = True の場合、「購入する」ボタンは押しません。
- 実購入する場合だけ DRY_RUN = False に変更してください。

必要ライブラリ:
    pip install playwright python-dotenv pandas openpyxl

初回だけ必要:
    python -m playwright install

.env に以下を保存:
    INET_ID=あなたのINET-ID
    USER_ID=あなたの加入者番号
    PASSWORD=あなたの暗証番号
    P_ARS=あなたのP-ARS番号
"""

from __future__ import annotations

import asyncio
import os
import pathlib
import re
import sys
from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional, Sequence, Tuple

import pandas as pd
from dotenv import load_dotenv
from playwright.async_api import Browser, Page, TimeoutError, async_playwright

# ============================================================
# 文字化け対策
# ============================================================
try:
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")
except Exception:
    pass

# ============================================================
# .env 読み込み
# ============================================================
def env_candidates() -> List[pathlib.Path]:
    """実行場所に依存しないよう、スクリプト位置から .env 候補を列挙する。"""
    script_path = pathlib.Path(__file__).resolve()
    candidates: List[pathlib.Path] = []

    for base in [script_path.parent, *script_path.parents]:
        candidates.append(base / ".env")
        candidates.append(base / "env" / ".env")

    cwd = pathlib.Path.cwd().resolve()
    for base in [cwd, *cwd.parents]:
        candidates.append(base / ".env")
        candidates.append(base / "env" / ".env")

    unique: List[pathlib.Path] = []
    seen = set()
    for path in candidates:
        key = str(path).lower()
        if key not in seen:
            unique.append(path)
            seen.add(key)
    return unique


def load_env_file() -> Optional[pathlib.Path]:
    """最初に見つかった .env を読み込み、読み込んだパスを返す。"""
    for path in env_candidates():
        if path.exists():
            load_dotenv(dotenv_path=path)
            return path
    load_dotenv()
    return None


ENV_PATH = load_env_file()

INET_ID = os.getenv("INET_ID", "")
USER_ID = os.getenv("USER_ID", "")
PASSWORD = os.getenv("PASSWORD", "")
P_ARS = os.getenv("P_ARS", "")

# ============================================================
# 設定
# ============================================================
LOGIN_URL = "https://www.ipat.jra.go.jp/"

# ★最重要：最初は True のままテストすること
DRY_RUN = False

# 1点100円なら、JRA画面では「1」と入力する
BET_UNIT_100YEN = 2
BET_UNIT_YEN = BET_UNIT_100YEN * 200

# Excel設定
SHEET_NAME = "回収率重視_買い目候補"
EXCEL_TEMPLATE = r"C:\Users\okino\OneDrive\ドキュメント\my_python_cursor\keiba_yosou_2026\data\output\馬の競走成績_with_feat_{YYYYMMDD}.xlsx"

# 対象列：列位置ではなく列名で指定する
# 生成元の「回収率重視_買い目候補」では、軸=1位・3位、相手=2位・4位・5位。
BET_COLUMN_SETS = [
    (
        ["3連複1点目_馬番1", "3連複1点目_馬番2"],
        ["3連複1点目_馬番3", "3連複2点目_馬番3", "3連複3点目_馬番3"],
        "3連複列",
    ),
    (
        ["軸1", "軸2"],
        ["相手1", "相手2", "相手3"],
        "軸/相手列",
    ),
]

# 購入判定列。列が無い場合は安全のため停止する。
BUY_FLAG_COL = "購入判定"
BUY_FLAG_VALUE = "購入"

# ブラウザ設定
HEADLESS = False
SLOW_MO_MS = 40
RETRY_MAX = 1

# エラー保存先
ERRSHOT = pathlib.Path("errshot_img")
ERRDOM = pathlib.Path("errshot_dom")
ERRSHOT.mkdir(exist_ok=True)
ERRDOM.mkdir(exist_ok=True)

# rid_str は YYYYMMDD + 競馬場コード2桁 + レース番号2桁
KEIBAJO = {
    "01": "札幌",
    "02": "函館",
    "03": "福島",
    "04": "新潟",
    "05": "東京",
    "06": "中山",
    "07": "中京",
    "08": "京都",
    "09": "阪神",
    "10": "小倉",
}


@dataclass
class BetRow:
    rid: str
    axes: List[int]
    opponents: List[int]
    race_name: str = ""
    place: str = ""
    race_no: int = 0
    total_amount: int = 300


@dataclass
class VoteResult:
    success: bool
    retryable: bool = True


# ============================================================
# 小さな共通関数
# ============================================================
def normalize_rid(value) -> str:
    """Excel由来のレースIDを12桁文字列へ寄せる。"""
    if pd.isna(value):
        return ""
    s = str(value).strip()
    if re.fullmatch(r"\d+\.0", s):
        s = s[:-2]
    digits = re.sub(r"\D", "", s)
    if len(digits) > 12:
        digits = digits[-12:]
    return digits.zfill(12) if digits else ""


def to_int_or_none(value) -> Optional[int]:
    if pd.isna(value):
        return None
    try:
        return int(float(str(value).strip()))
    except Exception:
        m = re.search(r"\d+", str(value))
        return int(m.group()) if m else None


def normalize_place_name(value) -> str:
    """Excelの場所列をIPAT画面の競馬場名に寄せる。"""
    if pd.isna(value):
        return ""
    return re.sub(r"[\s\u3000]+", "", str(value)).strip()


def pick_required_column(df: pd.DataFrame, candidates: Sequence[str], label: str) -> str:
    """候補列から1つ選ぶ。見つからない場合は誤購入防止のため停止する。"""
    for col in candidates:
        if col in df.columns:
            return col
    raise RuntimeError(
        f"安全のため停止します。シートに {label} 列がありません。候補={list(candidates)}"
    )


def pick_bet_columns(df: pd.DataFrame) -> Tuple[List[str], List[str]]:
    """軸2頭・相手3頭の列名セットを選ぶ。列名が揃わない場合は停止する。"""
    for axis_cols, opponent_cols, label in BET_COLUMN_SETS:
        if all(c in df.columns for c in axis_cols + opponent_cols):
            print(f"[INFO] 買い目列セット: {label}")
            return list(axis_cols), list(opponent_cols)

    expected = [
        f"{label}: 軸={axis_cols}, 相手={opponent_cols}"
        for axis_cols, opponent_cols, label in BET_COLUMN_SETS
    ]
    raise RuntimeError(
        "安全のため停止します。軸2頭・相手3頭の列名セットが見つかりません。"
        + " / ".join(expected)
    )


def jp_week(yyyymmdd: str) -> str:
    """YYYYMMDD → 月火水木金土日"""
    return "月火水木金土日"[datetime.strptime(yyyymmdd, "%Y%m%d").weekday()]


def validate_env() -> None:
    missing = []
    for key, val in {
        "INET_ID": INET_ID,
        "USER_ID": USER_ID,
        "PASSWORD": PASSWORD,
        "P_ARS": P_ARS,
    }.items():
        if not val:
            missing.append(key)
    if missing:
        searched = "\n".join(f"  - {path}" for path in env_candidates()[:12])
        loaded = str(ENV_PATH) if ENV_PATH else "未検出"
        raise RuntimeError(
            ".env にログイン情報が不足しています: "
            + ", ".join(missing)
            + f"\n読み込んだ.env: {loaded}"
            + "\n.env の探索候補例:\n"
            + searched
        )


async def set_zoom_50(page: Page) -> None:
    try:
        await page.evaluate("document.body.style.zoom='50%'")
    except Exception:
        pass


async def js_fill(page: Page, selector: str, value: str) -> None:
    """Angular画面へ input/change を発火させながら値を入れる。"""
    await page.evaluate(
        """([s, v]) => {
            const e = document.querySelector(s);
            if (!e) return false;
            e.removeAttribute('disabled');
            e.value = v;
            e.dispatchEvent(new Event('input', {bubbles:true}));
            e.dispatchEvent(new Event('change', {bubbles:true}));
            e.dispatchEvent(new Event('blur', {bubbles:true}));
            return true;
        }""",
        [selector, value],
    )


async def select_option_by_keywords(
    page: Page,
    selector: str,
    keywords: Sequence[str],
    label_for_error: str,
) -> None:
    """select の option 文字列を NFKC 正規化し、表記ゆれに強く選択する。"""
    await page.wait_for_selector(selector, timeout=12000)
    ok = await page.evaluate(
        """([selector, keywords]) => {
            const normalize = (s) => (s || '').normalize('NFKC').replace(/\\s+/g, '');
            const wanted = keywords.map(normalize);
            const sel = document.querySelector(selector);
            if (!sel) return false;

            const opt = Array.from(sel.options || []).find((o) => {
                const text = normalize(o.textContent);
                return wanted.every((kw) => text.includes(kw));
            });
            if (!opt) return false;

            sel.value = opt.value;
            sel.dispatchEvent(new Event('input', {bubbles:true}));
            sel.dispatchEvent(new Event('change', {bubbles:true}));
            return true;
        }""",
        [selector, list(keywords)],
    )
    if not ok:
        raise RuntimeError(f"{label_for_error} の選択肢が見つかりません: {list(keywords)}")


def mask_sensitive_html(html: str) -> str:
    """エラー保存DOMから hidden 値やログイン系 input 値を伏せる。"""
    def mask_value_attr(match: re.Match) -> str:
        tag = match.group(0)
        return re.sub(
            r'(value=["\'])([^"\']*)(["\'])',
            r"\1***MASKED***\3",
            tag,
            flags=re.IGNORECASE,
        )

    html = re.sub(
        r'<input\b(?=[^>]*\btype=["\']hidden["\'])[^>]*>',
        mask_value_attr,
        html,
        flags=re.IGNORECASE,
    )
    html = re.sub(
        r'<input\b(?=[^>]*\bname=["\'](?:inetid|uh|u|i|nbc|bac|local|g|p|r)["\'])[^>]*>',
        mask_value_attr,
        html,
        flags=re.IGNORECASE,
    )
    return html


async def safe_shot(page: Page, label: str) -> Optional[pathlib.Path]:
    try:
        if page.is_closed():
            return None
        path = ERRSHOT / f"{label}_{datetime.now():%H%M%S}.png"
        await page.screenshot(path=str(path), full_page=True)
        return path
    except Exception:
        return None


async def safe_dump_dom(page: Page, label: str) -> Optional[pathlib.Path]:
    try:
        if page.is_closed():
            return None
        path = ERRDOM / f"{label}_{datetime.now():%H%M%S}.html"
        html = mask_sensitive_html(await page.content())
        path.write_text(html, encoding="utf-8")
        return path
    except Exception:
        return None


# ============================================================
# Excel 読み込み
# ============================================================
def load_bets_from_excel(raceday: str) -> List[BetRow]:
    excel_path = EXCEL_TEMPLATE.replace("{YYYYMMDD}", raceday)
    if not os.path.exists(excel_path):
        raise FileNotFoundError(f"Excelファイルが見つかりません: {excel_path}")

    df = pd.read_excel(excel_path, sheet_name=SHEET_NAME, engine="openpyxl")
    if df.empty:
        print(f"[WARN] {SHEET_NAME} シートが空です")
        return []

    rid_col = pick_required_column(df, ["レースID", "rid_str", "race_id"], "レースID")
    place_col = pick_required_column(df, ["場所", "競馬場", "place", "place_name"], "場所")

    # 購入判定=購入 の行だけに絞る
    if BUY_FLAG_COL not in df.columns:
        raise RuntimeError(
            f"安全のため停止します。シートに購入判定列 '{BUY_FLAG_COL}' がありません。"
        )
    df = df[df[BUY_FLAG_COL].astype(str).str.strip() == BUY_FLAG_VALUE].copy()

    axis_cols, opponent_cols = pick_bet_columns(df)

    bets: List[BetRow] = []
    for _, row in df.iterrows():
        rid = normalize_rid(row.get(rid_col))
        if not rid:
            continue

        axes: List[int] = []
        opponents: List[int] = []

        for col in axis_cols:
            n = to_int_or_none(row.get(col))
            if n is not None:
                axes.append(n)

        for col in opponent_cols:
            n = to_int_or_none(row.get(col))
            if n is not None:
                opponents.append(n)

        # 重複除去しつつ順序維持
        axes = list(dict.fromkeys(axes))
        opponents = [n for n in dict.fromkeys(opponents) if n not in axes]

        race_name = str(row.get("レース名", "")).strip()
        place = normalize_place_name(row.get(place_col))
        race_no_int = int(rid[-2:])

        if rid[:8] != raceday:
            print(
                f"[WARN] {rid} {race_name}: 入力日付 {raceday} と "
                f"レースID日付 {rid[:8]} が一致しません。場所列={place}, {race_no_int}R を使用します。"
            )

        if not place:
            print(f"[SKIP] {rid} {race_name}: 場所列が空です")
            continue

        if place not in set(KEIBAJO.values()):
            print(f"[SKIP] {rid} {race_name}: 場所列がJRA競馬場名ではありません place={place}")
            continue

        if len(axes) != 2 or len(opponents) != 3:
            print(
                f"[SKIP] {rid} {race_name}: 軸2頭・相手3頭になっていません "
                f"axes={axes}, opponents={opponents}"
            )
            continue

        bets.append(
            BetRow(
                rid=rid,
                axes=axes,
                opponents=opponents,
                race_name=race_name,
                place=place,
                race_no=race_no_int,
                total_amount=len(opponents) * BET_UNIT_YEN,
            )
        )

    print(f"[INFO] Excelから購入候補を読み込みました: {len(bets)} レース")
    for b in bets:
        print(
            f"  - {b.rid} {b.place}{b.race_no}R {b.race_name} "
            f"軸={b.axes} 相手={b.opponents} 合計={b.total_amount}円"
        )
    return bets


# ============================================================
# ログイン
# ============================================================
async def ipat_login(page: Page) -> None:
    await set_zoom_50(page)
    await page.goto(LOGIN_URL, wait_until="domcontentloaded", timeout=20000)
    await set_zoom_50(page)

    await js_fill(page, 'input[name="inetid"]', INET_ID)
    await page.locator('a[onclick*="send()"], input[onclick*="send()"]').first.click()

    await page.wait_for_url("**/pw_080_i.cgi", timeout=15000)
    await js_fill(page, 'input[name="i"]', USER_ID)
    await js_fill(page, 'input[name="p"]', PASSWORD)
    await js_fill(page, 'input[name="r"]', P_ARS)
    await page.locator('a[onclick*="ToModernMenu()"], input[onclick*="ToModernMenu()"]').first.click()

    await page.wait_for_url("**/pw_890_i.cgi", timeout=15000)
    await page.wait_for_selector('button[ui-sref="bet.basic"], button:has-text("通常")', timeout=15000)
    await set_zoom_50(page)
    print("✅ ログイン成功")


# ============================================================
# 競馬場・レース選択
# ============================================================
async def click_normal_vote(page: Page) -> None:
    btn = page.locator('button[ui-sref="bet.basic"], button:has-text("通常")').first
    await btn.wait_for(state="visible", timeout=10000)
    await btn.click()
    await page.wait_for_selector('#bet-basic-type, button:has-text("R"), select-course-race', timeout=10000)
    await set_zoom_50(page)


async def select_place_and_race(page: Page, bet: BetRow, raceday: str) -> Tuple[str, str, str]:
    place_name = normalize_place_name(bet.place)
    race_no = f"{int(bet.race_no)}R"
    week = jp_week(raceday)

    if not place_name:
        raise RuntimeError(f"場所が空です: rid={bet.rid}")

    print(f"▶ {place_name}({week}) {race_no}")

    # 方式1: プルダウン画面
    course_select = page.locator("#select-course-race-course")
    race_select = page.locator("#select-course-race-race")
    if await course_select.count():
        try:
            await course_select.select_option(label=f"{place_name}({week})")
            await page.wait_for_timeout(500)
        except Exception:
            # label完全一致が失敗した場合は、option文字列に競馬場名を含むものを選ぶ
            ok = await page.evaluate(
                """(placeName) => {
                    const sel = document.querySelector('#select-course-race-course');
                    if (!sel) return false;
                    const opt = Array.from(sel.options).find(o => (o.textContent || '').includes(placeName));
                    if (!opt) return false;
                    sel.value = opt.value;
                    sel.dispatchEvent(new Event('change', {bubbles:true}));
                    return true;
                }""",
                place_name,
            )
            if not ok:
                raise RuntimeError(f"競馬場プルダウンに {place_name} がありません")
            await page.wait_for_timeout(700)

        if await race_select.count():
            try:
                await race_select.select_option(label=race_no)
            except Exception:
                ok = await page.evaluate(
                    """(raceNo) => {
                        const sel = document.querySelector('#select-course-race-race');
                        if (!sel) return false;
                        const opt = Array.from(sel.options).find(o => (o.textContent || '').trim().startsWith(raceNo));
                        if (!opt) return false;
                        sel.value = opt.value;
                        sel.dispatchEvent(new Event('change', {bubbles:true}));
                        return true;
                    }""",
                    race_no,
                )
                if not ok:
                    raise RuntimeError(f"レースプルダウンに {race_no} がありません")
            await page.wait_for_timeout(700)
            return place_name, week, race_no

    # 方式2: ボタン画面
    place_btn = page.locator(f'button:has-text("{place_name}")').first
    await place_btn.wait_for(state="visible", timeout=10000)
    await place_btn.click()
    await page.wait_for_timeout(500)

    race_btn = page.locator(f'button:has-text("{race_no}")').first
    await race_btn.wait_for(state="visible", timeout=10000)
    if await race_btn.is_disabled():
        raise RuntimeError(f"締切または選択不可: {place_name} {race_no}")
    await race_btn.click()
    await page.wait_for_timeout(700)
    return place_name, week, race_no


# ============================================================
# 馬券入力：3連複・軸2頭ながし
# ============================================================
async def select_trio_two_axis_mode(page: Page) -> None:
    await select_option_by_keywords(page, "#bet-basic-type", ["3連複"], "式別 3連複")
    await page.wait_for_timeout(500)

    await select_option_by_keywords(page, "#bet-basic-method", ["軸2頭", "ながし"], "方式 軸2頭ながし")
    await page.wait_for_selector('input[id^="horse1_no"]', timeout=12000)
    await set_zoom_50(page)


async def ensure_checkbox_checked(page: Page, input_id: str, label: str) -> bool:
    """指定IDのチェックボックスをONにする。"""
    selector = f"input#{input_id}"
    label_selector = f'label[for="{input_id}"]'

    for i in range(3):
        try:
            loc = page.locator(label_selector).first
            if not await loc.count():
                loc = page.locator(selector).first
            await loc.scroll_into_view_if_needed(timeout=1500)
            await loc.click(force=True, timeout=2500)
            await page.wait_for_function(
                "(sel) => document.querySelector(sel)?.checked === true",
                selector,
                timeout=1500,
            )
            return True
        except Exception:
            await page.wait_for_timeout(300)

    # 最終手段：JSで直接チェック。ただしdisabledなら失敗扱い。
    try:
        ok = await page.evaluate(
            """(sel) => {
                const e = document.querySelector(sel);
                if (!e || e.disabled) return false;
                e.checked = true;
                e.dispatchEvent(new Event('input', {bubbles:true}));
                e.dispatchEvent(new Event('change', {bubbles:true}));
                return e.checked === true;
            }""",
            selector,
        )
        if ok:
            return True
    except Exception:
        pass

    print(f"  └─チェック失敗: {label} ({input_id})")
    await safe_shot(page, f"check_fail_{input_id}")
    await safe_dump_dom(page, f"check_fail_{input_id}")
    return False


async def select_axes_and_opponents(page: Page, axes: List[int], opponents: List[int]) -> bool:
    """
    3連複 軸2頭ながし画面で、
    horse1_no{馬番}=軸、horse2_no{馬番}=相手 を選択する。
    """
    print(f"  軸2頭: {axes} / 相手3頭: {opponents}")

    for n in axes:
        ok = await ensure_checkbox_checked(page, f"horse1_no{n}", f"軸 {n}")
        if not ok:
            return False
        await page.wait_for_timeout(300)

    for n in opponents:
        ok = await ensure_checkbox_checked(page, f"horse2_no{n}", f"相手 {n}")
        if not ok:
            return False
        await page.wait_for_timeout(200)

    # 3連複 軸2頭 + 相手3頭 = 3組
    try:
        await page.wait_for_function(
            """() => {
                const el = document.querySelector('div.selection-match strong');
                const n = Number((el?.innerText || '').replace(/[^0-9]/g, ''));
                return n === 3;
            }""",
            timeout=4000,
        )
        print("  組数確認: 3組")
    except Exception:
        print("  └─組数3組を確認できませんでした")
        await safe_shot(page, "match_count_not_3")
        await safe_dump_dom(page, "match_count_not_3")
        return False

    return True


async def set_unit_and_add_to_list(page: Page, unit_100yen: int = BET_UNIT_100YEN) -> bool:
    """金額1=100円を入れてセットし、購入予定リストを開く。"""
    await js_fill(page, 'div.selection-amount input[ng-model="vm.nUnit"]', str(unit_100yen))
    await page.wait_for_timeout(300)

    set_btn = page.locator('div.selection-buttons button:has-text("セット"):not([disabled])').first
    await set_btn.wait_for(state="visible", timeout=8000)
    await set_btn.click()
    await page.wait_for_timeout(700)

    # 入力終了 → 購入予定リスト
    end_btn = page.get_by_role("button", name="入力終了")
    if await end_btn.count():
        await end_btn.first.click(force=True)
        await page.wait_for_timeout(700)

    # 購入予定リストのDOMが存在しても hidden/ng-hide の場合があるため、表示状態で判定する。
    vote_list = page.locator(".ipat-vote-list").first
    try:
        visible = await vote_list.is_visible()
    except Exception:
        visible = False

    # 右上の購入予定リストボタンを押す必要がある場合に対応する。
    if not visible:
        list_btn = page.locator('button.btn-vote-list, button:has-text("購入予定リスト")').first
        if await list_btn.count():
            await list_btn.click(force=True)
            await page.wait_for_timeout(700)

    await vote_list.wait_for(state="visible", timeout=8000)
    await set_zoom_50(page)
    return True


# ============================================================
# 購入予定リスト → 購入
# ============================================================
async def read_purchase_limit(page: Page) -> Optional[int]:
    try:
        val = await page.evaluate(
            """() => {
                const rows = Array.from(document.querySelectorAll('tr'));
                for (const tr of rows) {
                    const txt = (tr.textContent || '').replace(/\\s+/g, '');
                    if (txt.includes('購入限度額')) {
                        const td = tr.querySelector('td.text-right') || tr.querySelector('td:last-child');
                        const s = (td?.textContent || '').replace(/[^0-9]/g, '');
                        return s ? Number(s) : null;
                    }
                }
                return null;
            }"""
        )
        return int(val) if val is not None else None
    except Exception:
        return None


async def verify_cart_total(page: Page, expected_total: int) -> bool:
    """購入予定リスト上の表示合計が想定金額と一致するか確認する。"""
    try:
        total = await page.evaluate(
            """(expected) => {
                const root = document.querySelector('.ipat-vote-list');
                if (!root) return null;

                const toNumber = (s) => Number((s || '').replace(/[^0-9]/g, '')) || 0;
                const normalize = (s) => (s || '').replace(/\\s+/g, '');

                const numberValues = Array.from(root.querySelectorAll('.number'))
                    .map((e) => toNumber(e.textContent))
                    .filter((v) => v > 0);
                if (numberValues.includes(Number(expected))) return Number(expected);

                const text = normalize(root.textContent);
                const patterns = [
                    /合計金額[:：]?([0-9,]+)円/g,
                    /合計[:：]?([0-9,]+)円/g,
                    /([0-9,]+)円/g
                ];
                for (const pattern of patterns) {
                    const matches = Array.from(text.matchAll(pattern));
                    for (const match of matches) {
                        const value = toNumber(match[1]);
                        if (value === Number(expected)) return value;
                    }
                }
                return numberValues.length ? numberValues[numberValues.length - 1] : null;
            }""",
            int(expected_total),
        )
        return int(total) == int(expected_total)
    except Exception:
        return False


async def ensure_total_and_buy(page: Page, total_amount: int) -> bool:
    """購入予定リストで合計金額を入れる。DRY_RUN=Trueなら購入ボタンは押さない。"""
    modal = page.locator(".ipat-vote-list").first
    await modal.wait_for(state="visible", timeout=8000)

    # 合計金額入力
    total_selector = '.ipat-vote-list input[ng-model="vm.cAmountTotal"], .ipat-vote-list input[name="amountTotal"]'
    try:
        inp = page.locator(total_selector).first
        await inp.wait_for(state="visible", timeout=8000)
        try:
            await inp.fill(str(total_amount))
        except Exception:
            await js_fill(page, total_selector, str(total_amount))
        await page.keyboard.press("Tab")
        await page.wait_for_timeout(300)
    except Exception:
        print("  └─合計金額入力に失敗")
        await safe_shot(page, "total_input_failed")
        await safe_dump_dom(page, "total_input_failed")
        return False

    # 入っている値を確認
    v = await page.evaluate(
        """(sel) => {
            const e = document.querySelector(sel);
            return Number((e?.value || '').replace(/[^0-9]/g, '')) || 0;
        }""",
        total_selector,
    )
    if int(v) != int(total_amount):
        print(f"  └─合計金額が一致しません: input={v}, expected={total_amount}")
        return False

    if not await verify_cart_total(page, total_amount):
        print(f"  └─購入予定リスト上の合計金額を確認できません: expected={total_amount}")
        await safe_shot(page, "cart_total_mismatch")
        await safe_dump_dom(page, "cart_total_mismatch")
        return False

    # 購入ボタン確認
    buy_btn = modal.get_by_role("button", name="購入する").first
    await buy_btn.wait_for(state="visible", timeout=8000)

    if DRY_RUN:
        print(f"  [DRY_RUN] ここで購入直前停止: 合計金額 {total_amount}円")
        return True

    for _ in range(50):
        if await buy_btn.is_enabled():
            break
        await page.wait_for_timeout(200)

    if not await buy_btn.is_enabled():
        print("  └─購入するボタンが有効になりません")
        await safe_shot(page, "buy_disabled")
        await safe_dump_dom(page, "buy_disabled")
        return False

    await buy_btn.click(force=True)
    return True


async def click_final_ok_strict(page: Page, timeout_ms: int = 5000) -> bool:
    """購入する後の最終確認OKを押す。"""
    selectors = [
        'button.btn-ok:has-text("OK")',
        'button:has-text("OK")',
        '.modal button.btn-ok',
        '[role="dialog"] button.btn-ok',
    ]

    for sel in selectors:
        try:
            loc = page.locator(sel).first
            await loc.wait_for(state="visible", timeout=timeout_ms)
            await loc.click(force=True)
            print("  最終確認OKを押しました")
            return True
        except Exception:
            pass

    try:
        await page.keyboard.press("Enter")
        print("  最終確認OK: Enterで送信")
        return True
    except Exception:
        return False


SUCCESS_KEYWORDS = ("投票を受け付けました", "購入を受け付けました", "受付番号")
ERROR_KEYWORDS = (
    "購入できません",
    "エラー",
    "時間外",
    "投票締切",
    "発売締切",
    "有効期限切れ",
    "ログイン",
)


async def wait_result_modal(page: Page, timeout_ms: int = 10000) -> Tuple[str, str]:
    modal = page.locator('.modal, .modal-dialog, .ipat-modal, .ngdialog, .ui-dialog, [role="dialog"]')
    body = modal.locator('.modal-body, .ngdialog-content, .body, .content, .ui-dialog-content, [role="document"]')
    try:
        await modal.first.wait_for(state="visible", timeout=timeout_ms)
        try:
            txt = (await body.first.inner_text()).strip()
        except Exception:
            txt = (await modal.first.inner_text()).strip()
        tnorm = txt.replace("\n", " ").replace("\r", " ")
        if any(k in tnorm for k in SUCCESS_KEYWORDS):
            return "success", tnorm
        if any(k in tnorm for k in ERROR_KEYWORDS):
            return "error", tnorm
        return "unknown", tnorm
    except TimeoutError:
        return "timeout", ""


async def detect_purchase_success_on_page(page: Page, timeout_ms: int = 12000) -> Tuple[bool, str]:
    """
    購入完了後の成功判定を画面全体から行う。

    成功とみなす条件:
    1. 「続けて投票する」ボタンが見える
    2. 画面全体に受付番号/投票受付/購入受付の文字がある
    3. 投票結果っぽい画面になっている
    """
    end_time = datetime.now().timestamp() + timeout_ms / 1000
    success_words = [
        "受付番号",
        "投票を受け付けました",
        "購入を受け付けました",
        "投票結果",
        "購入結果",
    ]

    while datetime.now().timestamp() < end_time:
        try:
            cont_btn = page.get_by_role("button", name="続けて投票する").first
            if await cont_btn.count() and await cont_btn.is_visible():
                return True, "続けて投票するボタンを確認"
        except Exception:
            pass

        try:
            txt = await page.locator("body").inner_text(timeout=1500)
            tnorm = txt.replace("\n", " ").replace("\r", " ")
            for word in success_words:
                if word in tnorm:
                    return True, f"画面内に成功語句「{word}」を確認"
        except Exception:
            pass

        await page.wait_for_timeout(500)

    return False, "成功画面を確認できませんでした"


async def wait_limit_decreased(page: Page, before: int, delta: int, timeout_ms: int = 12000) -> bool:
    end_time = datetime.now().timestamp() + timeout_ms / 1000
    while datetime.now().timestamp() < end_time:
        after = await read_purchase_limit(page)
        if after is not None and after <= before - delta:
            return True
        await page.wait_for_timeout(500)
    return False


async def close_result_or_continue(page: Page) -> None:
    """
    投票結果画面の『続けて投票する』を押す。
    見つからない場合は、OK/閉じる/投票メニューへ戻るも試す。
    """
    for name in ["続けて投票する", "通常投票へ", "OK", "閉じる"]:
        try:
            btn = page.get_by_role("button", name=name).first
            if await btn.count() and await btn.is_visible():
                await btn.click(force=True, timeout=3000)
                await page.wait_for_timeout(1000)
                await set_zoom_50(page)
                return
        except Exception:
            pass

    try:
        normal = page.locator(
            'a:has-text("通常投票"), button[ui-sref="bet.basic"], button:has-text("通常")'
        ).first
        if await normal.count():
            await normal.click(force=True, timeout=3000)
            await page.wait_for_timeout(1000)
            await set_zoom_50(page)
            return
    except Exception:
        pass


# ============================================================
# 1レース処理
# ============================================================
async def vote_one(page: Page, bet: BetRow, raceday: str) -> VoteResult:
    final_ok_sent = False
    try:
        print("\n" + "=" * 70)
        print(
            f"対象: {bet.rid} {bet.place}{bet.race_no}R {bet.race_name} "
            f"/ 軸={bet.axes} / 相手={bet.opponents}"
        )

        if not await page.locator('button[ui-sref="bet.basic"], button:has-text("通常")').count():
            await ipat_login(page)

        await click_normal_vote(page)
        await select_place_and_race(page, bet, raceday)
        await select_trio_two_axis_mode(page)

        ok = await select_axes_and_opponents(page, bet.axes, bet.opponents)
        if not ok:
            return VoteResult(False)

        await set_unit_and_add_to_list(page, BET_UNIT_100YEN)

        limit_before = await read_purchase_limit(page)
        ok = await ensure_total_and_buy(page, bet.total_amount)
        if not ok:
            return VoteResult(False)

        if DRY_RUN:
            print("  [DRY_RUN] 実購入はしていません。購入予定リストまで確認済みです。")
            return VoteResult(True)

        # 最終確認OK
        html_ok = await click_final_ok_strict(page, timeout_ms=5000)
        final_ok_sent = html_ok
        if not html_ok:
            try:
                dlg = await page.wait_for_event("dialog", timeout=2500)
                await dlg.accept()
                final_ok_sent = True
                print("  ネイティブ確認ダイアログOK")
            except Exception:
                print("  └─確認ダイアログを検出できませんでした")

        if not final_ok_sent:
            print("  └─最終確認OKを送信できたか確認できないため、購入後判定へ進みません")
            return VoteResult(False)

        # ====================================================
        # 購入後の結果確認
        # ====================================================
        try:
            loading = page.locator(".ipat-loading").first
            await loading.wait_for(state="visible", timeout=3000)
            print("  送信中オーバーレイ表示")
            await loading.wait_for(state="hidden", timeout=15000)
            await page.wait_for_timeout(1000)
        except Exception:
            await page.wait_for_timeout(1500)

        # 1) 画面全体から成功判定する。
        ok_page, reason = await detect_purchase_success_on_page(page, timeout_ms=12000)
        if ok_page:
            print(f"  ✅ 購入完了判定: {reason}")
            await close_result_or_continue(page)
            return VoteResult(True)

        # 2) 購入限度額の減少で成功判定する。
        if limit_before is not None:
            ok_limit = await wait_limit_decreased(page, limit_before, bet.total_amount, timeout_ms=8000)
            if ok_limit:
                after_limit = await read_purchase_limit(page)
                print(f"  ✅ 購入完了判定: 購入限度額 {limit_before:,} → {after_limit:,}")
                await close_result_or_continue(page)
                return VoteResult(True)

        # 3) 最後にモーダル文字で判定する。
        label, txt = await wait_result_modal(page, timeout_ms=5000)
        if label == "success":
            print("  ✅ 購入完了:", (txt or "")[:160], "...")
            await close_result_or_continue(page)
            return VoteResult(True)

        if label == "error":
            print("  ❌ 購入エラー:", (txt or "")[:200], "...")
            await safe_shot(page, "after_buy_error")
            await safe_dump_dom(page, "after_buy_error")
            await close_result_or_continue(page)
            return VoteResult(False, retryable=not final_ok_sent)

        # 4) それでも不明なら、自動リトライせず停止する。
        print("  └─購入完了画面を確認できません")
        await close_result_or_continue(page)
        print("  [STOP] 購入操作後の結果が不確実なため、この買い目は自動リトライしません。")
        await safe_shot(page, "after_buy_unknown")
        await safe_dump_dom(page, "after_buy_unknown")
        return VoteResult(False, retryable=False)

    except Exception as e:
        print(f"  ❌ 例外: {e}")
        await safe_shot(page, "fatal")
        await safe_dump_dom(page, "fatal")
        return VoteResult(False, retryable=not final_ok_sent)


# ============================================================
# 全レース処理
# ============================================================
async def vote_all(raceday: str) -> None:
    validate_env()
    bets = load_bets_from_excel(raceday)
    if not bets:
        print("[INFO] 購入対象がありません")
        return

    print("\n" + "#" * 70)
    print(f"DRY_RUN = {DRY_RUN}")
    if DRY_RUN:
        print("実購入はしません。購入予定リスト確認までで止まります。")
    else:
        print("実購入モードです。購入するボタンと最終OKを押します。")
    print("#" * 70 + "\n")

    async with async_playwright() as pw:
        browser: Browser = await pw.chromium.launch(
            headless=HEADLESS,
            slow_mo=SLOW_MO_MS,
            args=["--start-maximized"],
        )
        ctx = await browser.new_context(viewport=None, locale="ja-JP")
        page = await ctx.new_page()

        await ipat_login(page)

        stop_all = False
        for bet in bets:
            attempts = 0
            while attempts <= RETRY_MAX:
                result = await vote_one(page, bet, raceday)
                if result.success:
                    break

                if not result.retryable:
                    print(
                        f"[STOP] 自動リトライ停止: {bet.rid} {bet.place}{bet.race_no}R {bet.race_name}"
                    )
                    stop_all = True
                    break

                attempts += 1
                if attempts <= RETRY_MAX:
                    print(f"  ↺ リトライ {attempts}/{RETRY_MAX}: 再ログインしてやり直します")
                    try:
                        if page.is_closed():
                            page = await ctx.new_page()
                        await ipat_login(page)
                    except Exception as e:
                        print(f"  └─再ログイン失敗: {e}")
                    await asyncio.sleep(1)

            if attempts > RETRY_MAX:
                print(f"[FAIL] 最終失敗: {bet.rid} {bet.race_name}")

            if stop_all:
                print("[STOP] 購入結果が不確実なため、以降のレース処理も停止します。")
                break

            if DRY_RUN:
                print("[DRY_RUN] 1レースだけ確認する設定のため、ここで停止します。")
                break

        # DRY_RUNのときは画面確認しやすいよう少し待つ
        if DRY_RUN:
            print("\n[DRY_RUN] 10秒後にブラウザを閉じます。画面を確認してください。")
            await page.wait_for_timeout(10000)

        await ctx.close()
        await browser.close()


# ============================================================
# main
# ============================================================
def main() -> None:
    raceday = input("対象レース日付を YYYYMMDD 形式で入力してください: ").strip()
    if not re.fullmatch(r"\d{8}", raceday):
        raise ValueError("YYYYMMDD形式で入力してください。例: 20260524")

    asyncio.run(vote_all(raceday))


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("中断しました", file=sys.stderr)
