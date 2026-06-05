# -*- coding: utf-8 -*-
"""
keibayosou_training_v7_full_training_history.py

目的:
- netkeiba の馬別「調教」ページから調教データを取得する
- 対象レースの登録馬ごとに training_score を付ける
- 既存の keibayosou_* 系コードへ後から組み込みやすいように、
  rid_str / 馬番 / 馬名 をキーにした DataFrame を返す

想定する使い方:
1) まず単体で実行して調教スコアExcelを作る
2) 後で keibayosou_pipeline.py 側で TARGET / 今走レース情報 に merge する

注意:
- netkeiba はページ構造が変わることがあります。
- ログインが必要なページの場合は、環境変数 NETKEIBA_COOKIE にブラウザのCookie文字列を入れると
  requests で取得できる可能性が上がります。
- まずは「落ちにくく、既存コードへ組み込みやすい」ことを優先した版です。
"""

from __future__ import annotations

import argparse
import configparser
import os
import re
import time
import unicodedata
from datetime import datetime
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import urljoin, urlparse, parse_qs, urlencode, urlunparse

import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup

# ============================================================
# 既存pyから流用する設定・関数
# ============================================================
try:
    from keibayosou_config import NOW_SHEET, HORSES_SHEET
except Exception:
    NOW_SHEET = "今走レース情報"
    HORSES_SHEET = "horses"

try:
    from keibayosou_utils import _retry_session, _ensure_rid_str
except Exception:
    _retry_session = None

    def _ensure_rid_str(df: pd.DataFrame, label: str = "") -> pd.DataFrame:
        if "rid_str" in df.columns:
            df["rid_str"] = df["rid_str"].astype(str)
            return df
        for c in ["レースID", "race_id", "raceid", "rid", "RaceID"]:
            if c in df.columns:
                df["rid_str"] = df[c].astype(str)
                return df
        return df

try:
    from keibayosou_features import _normalize_rid_series, _normalize_umaban_series
except Exception:
    def _normalize_rid_series(s: pd.Series) -> pd.Series:
        def one(v: Any) -> str:
            if pd.isna(v):
                return ""
            text = str(v).strip()
            m = re.fullmatch(r"(\d+)(?:\.0+)?", text)
            if m:
                digits = m.group(1)
            else:
                digits = re.sub(r"\D", "", text)
            return digits[-12:] if len(digits) > 12 else digits
        return s.map(one).astype(str)

    def _normalize_umaban_series(s: pd.Series) -> pd.Series:
        return pd.to_numeric(s, errors="coerce").astype("Int64")


# ============================================================
# 基本設定
# ============================================================
NETKEIBA_RACE_URL = "https://race.netkeiba.com/race/shutuba.html?race_id={race_id}"
NETKEIBA_DB_RACE_URL = "https://db.netkeiba.com/race/{race_id}/"
NETKEIBA_TRAINING_URL = "https://db.netkeiba.com/horse/training.html?id={horse_id}"

DEFAULT_SLEEP_SEC = 1.0
DEFAULT_TIMEOUT = 20

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)


# ============================================================
# 小さな共通関数
# ============================================================
def _norm_text(x: Any) -> str:
    """全角半角・空白ゆらぎを吸収した文字列にする。"""
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return ""
    s = unicodedata.normalize("NFKC", str(x))
    s = s.replace("\u3000", " ")
    s = re.sub(r"\s+", " ", s)
    return s.strip()


def _norm_name(x: Any) -> str:
    """馬名照合用。空白を消して比較する。"""
    s = _norm_text(x)
    s = s.replace(" ", "")
    return s.strip()


def _clean_colname(x: Any) -> str:
    s = _norm_text(x)
    s = s.replace(" ", "")
    return s


def _pick_col(df: pd.DataFrame, candidates: Iterable[str]) -> Optional[str]:
    """列名ゆらぎを吸収して候補列を探す。"""
    norm_to_raw = {_clean_colname(c): c for c in df.columns}
    for cand in candidates:
        key = _clean_colname(cand)
        if key in norm_to_raw:
            return norm_to_raw[key]
    return None


def _to_float(x: Any, default: float = np.nan) -> float:
    try:
        if x is None or pd.isna(x):
            return default
        s = str(x).replace(",", "").strip()
        m = re.search(r"[-+]?\d+(?:\.\d+)?", s)
        if not m:
            return default
        return float(m.group(0))
    except Exception:
        return default


def _parse_date(x: Any) -> Optional[pd.Timestamp]:
    """日付を pandas Timestamp にする。年が無い場合は NaT 扱い。"""
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return None
    s = _norm_text(x)
    # 2026/05/28, 2026.5.28, 20260528 など
    digits = re.sub(r"\D", "", s)
    if len(digits) >= 8:
        try:
            return pd.to_datetime(digits[:8], format="%Y%m%d")
        except Exception:
            return None
    return None


def _extract_horse_id_from_url(url: str) -> str:
    m = re.search(r"/horse/(\d+)/?", str(url))
    if m:
        return m.group(1)
    m = re.search(r"[?&]id=(\d+)", str(url))
    if m:
        return m.group(1)
    return ""


def _make_session() -> requests.Session:
    """requests セッションを作成。既存 utils の _retry_session があれば流用。"""
    if _retry_session is not None:
        session = _retry_session(total=3, backoff=0.5)
    else:
        session = requests.Session()

    session.headers.update(
        {
            "User-Agent": USER_AGENT,
            "Accept-Language": "ja,en-US;q=0.9,en;q=0.8",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Connection": "keep-alive",
        }
    )

    # 任意: ブラウザからコピーした Cookie を環境変数に入れておくとログイン状態で取得できる場合がある
    # PowerShell例:
    # $env:NETKEIBA_COOKIE="netkeiba=xxxxx; other=yyyyy"
    cookie_text = os.environ.get("NETKEIBA_COOKIE", "").strip()
    if cookie_text:
        session.headers.update({"Cookie": cookie_text})

    return session



def _find_credentials_ini(explicit_path: str = "") -> Optional[Path]:
    """
    netkeibaログイン情報を書いた credentials.ini を探す。

    優先順位:
    1) --credentials-ini で指定されたパス
    2) カレント直下 credentials.ini
    3) スクリプト直下 credentials.ini
    4) ./ini/credentials.ini
    5) keibayosou_config.INI_DIR / credentials.ini があればそこ
    """
    candidates: List[Path] = []

    if explicit_path:
        candidates.append(Path(explicit_path))

    candidates.extend(
        [
            Path.cwd() / "credentials.ini",
            Path(__file__).resolve().parent / "credentials.ini",
            Path.cwd() / "ini" / "credentials.ini",
            Path(__file__).resolve().parent / "ini" / "credentials.ini",
        ]
    )

    try:
        from keibayosou_config import INI_DIR  # type: ignore
        candidates.append(Path(INI_DIR) / "credentials.ini")
    except Exception:
        pass

    for path in candidates:
        try:
            if path.exists() and path.is_file():
                return path
        except Exception:
            continue
    return None


def _load_netkeiba_credentials(credentials_ini: str = "") -> Tuple[str, str, str]:
    """
    netkeibaログインID/パスワードを取得する。

    優先順位:
    1) 環境変数 NETKEIBA_LOGIN_ID / NETKEIBA_PASSWORD
    2) 環境変数 NETKEIBA_ID / NETKEIBA_PASS
    3) credentials.ini

    credentials.ini の例:

    [netkeiba]
    login_id = your_mail@example.com
    password = your_password
    """
    env_id = (
        os.environ.get("NETKEIBA_LOGIN_ID")
        or os.environ.get("NETKEIBA_ID")
        or os.environ.get("NETKEIBA_USER")
        or ""
    ).strip()
    env_pw = (
        os.environ.get("NETKEIBA_PASSWORD")
        or os.environ.get("NETKEIBA_PASS")
        or os.environ.get("NETKEIBA_PW")
        or ""
    ).strip()

    if env_id and env_pw:
        return env_id, env_pw, "environment"

    ini_path = _find_credentials_ini(credentials_ini)
    if ini_path is None:
        return "", "", ""

    parser = configparser.ConfigParser()
    try:
        parser.read(ini_path, encoding="utf-8")
    except UnicodeDecodeError:
        parser.read(ini_path, encoding="cp932")

    section_candidates = [
        "netkeiba",
        "NETKEIBA",
        "Netkeiba",
        "login",
        "LOGIN",
        "credentials",
        "CREDENTIALS",
    ]
    id_keys = ["login_id", "id", "user", "username", "email", "mail", "NETKEIBA_ID"]
    pw_keys = ["password", "pass", "pw", "NETKEIBA_PASSWORD", "NETKEIBA_PASS"]

    for sec in section_candidates:
        if not parser.has_section(sec):
            continue
        login_id = ""
        password = ""
        for key in id_keys:
            if parser.has_option(sec, key):
                login_id = parser.get(sec, key).strip()
                break
        for key in pw_keys:
            if parser.has_option(sec, key):
                password = parser.get(sec, key).strip()
                break
        if login_id and password:
            return login_id, password, str(ini_path)

    return "", "", str(ini_path)


def _wait_document_ready(driver, timeout_sec: int = 30) -> None:
    """ページ読み込み完了を待つ。失敗しても処理継続。"""
    try:
        from selenium.webdriver.support.ui import WebDriverWait
        WebDriverWait(driver, timeout_sec).until(
            lambda d: d.execute_script("return document.readyState") in {"interactive", "complete"}
        )
    except Exception:
        pass


def _safe_driver_get(driver, url: str, wait_seconds: int = 30, retry: int = 2) -> None:
    """SeleniumでURLを開く。ConnectionReset系が出た場合に短くリトライする。"""
    last_err = None
    for i in range(max(1, int(retry))):
        try:
            driver.get(url)
            _wait_document_ready(driver, timeout_sec=wait_seconds)
            return
        except Exception as e:
            last_err = e
            print(f"[WARN] Seleniumでページを開けませんでした。retry={i + 1}/{retry} url={url} err={type(e).__name__}: {e}")
            time.sleep(2.0)
    if last_err:
        raise last_err


def _save_selenium_debug(driver, debug_dir: str = "data/output/debug_training_html", prefix: str = "selenium_login") -> None:
    """Seleniumの現在HTMLとスクリーンショットを保存する。ログイン画面変更の確認用。"""
    try:
        out_dir = Path(debug_dir)
        out_dir.mkdir(parents=True, exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        html_path = out_dir / f"{prefix}_{ts}.html"
        png_path = out_dir / f"{prefix}_{ts}.png"
        try:
            html_path.write_text(driver.page_source or "", encoding="utf-8", errors="ignore")
            print(f"[DEBUG] Selenium HTMLを保存しました: {html_path}")
        except Exception as e:
            print(f"[WARN] Selenium HTML保存失敗: {e}")
        try:
            driver.save_screenshot(str(png_path))
            print(f"[DEBUG] Selenium screenshotを保存しました: {png_path}")
        except Exception as e:
            print(f"[WARN] Selenium screenshot保存失敗: {e}")
    except Exception as e:
        print(f"[WARN] Selenium debug保存に失敗しました: {e}")


def _list_visible_inputs(driver) -> list[dict]:
    """現在ページのinput一覧を取得する。フォーム特定のデバッグ用。"""
    try:
        from selenium.webdriver.common.by import By
        inputs = driver.find_elements(By.CSS_SELECTOR, "input")
        rows = []
        for idx, elem in enumerate(inputs):
            try:
                rows.append({
                    "idx": idx,
                    "type": elem.get_attribute("type") or "",
                    "name": elem.get_attribute("name") or "",
                    "id": elem.get_attribute("id") or "",
                    "class": elem.get_attribute("class") or "",
                    "placeholder": elem.get_attribute("placeholder") or "",
                    "value": elem.get_attribute("value") or "",
                    "displayed": elem.is_displayed(),
                    "enabled": elem.is_enabled(),
                })
            except Exception:
                continue
        return rows
    except Exception:
        return []


def _print_input_debug(driver) -> None:
    """input一覧をログに出す。パスワード値は出さない。"""
    rows = _list_visible_inputs(driver)
    print(f"[DEBUG] 現在ページのinput数: {len(rows)}")
    for r in rows[:30]:
        print(
            "[DEBUG] input "
            f"idx={r.get('idx')} type={r.get('type')} name={r.get('name')} "
            f"id={r.get('id')} placeholder={r.get('placeholder')} "
            f"displayed={r.get('displayed')} enabled={r.get('enabled')}"
        )


def _switch_to_frame_containing_login(driver) -> bool:
    """
    ログインフォームがiframe内にある場合に備え、password inputを含むframeへ移動する。
    見つからない場合はdefault_contentへ戻す。
    """
    from selenium.webdriver.common.by import By
    try:
        driver.switch_to.default_content()
    except Exception:
        pass

    try:
        if driver.find_elements(By.CSS_SELECTOR, 'input[type="password"]'):
            return True
    except Exception:
        pass

    try:
        frames = driver.find_elements(By.CSS_SELECTOR, "iframe, frame")
    except Exception:
        frames = []

    for frame in frames:
        try:
            driver.switch_to.default_content()
            driver.switch_to.frame(frame)
            if driver.find_elements(By.CSS_SELECTOR, 'input[type="password"]'):
                print("[INFO] ログインフォームをiframe内で検出しました")
                return True
        except Exception:
            continue

    try:
        driver.switch_to.default_content()
    except Exception:
        pass
    return False


def _find_first_visible_element(driver, selectors: List[str], timeout_sec: int = 10):
    """候補CSSセレクタから、最初に見つかる表示要素を返す。"""
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC

    _switch_to_frame_containing_login(driver)

    last_err: Optional[Exception] = None
    for selector in selectors:
        try:
            return WebDriverWait(driver, timeout_sec).until(
                EC.visibility_of_element_located((By.CSS_SELECTOR, selector))
            )
        except Exception as e:
            last_err = e
            continue
    if last_err:
        raise last_err
    raise RuntimeError("要素候補が空です")


def _find_login_id_input(driver, timeout_sec: int = 20):
    """ログインID入力欄を候補セレクタ＋input属性から探す。"""
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait

    _switch_to_frame_containing_login(driver)

    id_selectors = [
        'input[name="login_id"]',
        'input#login_id',
        'input[name="email"]',
        'input#email',
        'input[name="mail"]',
        'input[name="login"]',
        'input[name="account"]',
        'input[name="user"]',
        'input[name="username"]',
        'input[name="id"]',
        'input#id',
        'input[type="email"]',
        'input[type="text"]',
    ]
    try:
        return _find_first_visible_element(driver, id_selectors, timeout_sec=3)
    except Exception:
        pass

    def _predicate(d):
        elems = d.find_elements(By.CSS_SELECTOR, "input")
        for e in elems:
            try:
                typ = (e.get_attribute("type") or "text").lower()
                if typ in {"hidden", "submit", "button", "checkbox", "radio", "password"}:
                    continue
                if not e.is_displayed() or not e.is_enabled():
                    continue
                attrs = " ".join([
                    e.get_attribute("name") or "",
                    e.get_attribute("id") or "",
                    e.get_attribute("class") or "",
                    e.get_attribute("placeholder") or "",
                    e.get_attribute("autocomplete") or "",
                ]).lower()
                if any(k in attrs for k in ["mail", "email", "login", "account", "user", "id"]):
                    return e
            except Exception:
                continue
        return False

    return WebDriverWait(driver, timeout_sec).until(_predicate)


def _find_password_input(driver, timeout_sec: int = 20):
    """パスワード入力欄を探す。"""
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait

    _switch_to_frame_containing_login(driver)

    pw_selectors = [
        'input[name="pswd"]',
        'input[name="password"]',
        'input#password',
        'input[name="passwd"]',
        'input#passwd',
        'input[name="pass"]',
        'input#pass',
        'input[type="password"]',
    ]
    try:
        return _find_first_visible_element(driver, pw_selectors, timeout_sec=3)
    except Exception:
        pass

    def _predicate(d):
        elems = d.find_elements(By.CSS_SELECTOR, 'input[type="password"], input')
        for e in elems:
            try:
                typ = (e.get_attribute("type") or "").lower()
                attrs = " ".join([
                    e.get_attribute("name") or "",
                    e.get_attribute("id") or "",
                    e.get_attribute("class") or "",
                    e.get_attribute("placeholder") or "",
                    e.get_attribute("autocomplete") or "",
                ]).lower()
                if not e.is_displayed() or not e.is_enabled():
                    continue
                if typ == "password" or any(k in attrs for k in ["password", "passwd", "pass"]):
                    return e
            except Exception:
                continue
        return False

    return WebDriverWait(driver, timeout_sec).until(_predicate)


def _click_first_available(driver, selectors: List[str], timeout_sec: int = 10) -> bool:
    """候補CSSセレクタから、最初にクリックできる要素をクリックする。"""
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC

    _switch_to_frame_containing_login(driver)

    for selector in selectors:
        try:
            elem = WebDriverWait(driver, timeout_sec).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, selector))
            )
            elem.click()
            return True
        except Exception:
            continue
    return False


def _is_netkeiba_logged_in_html(html: str) -> bool:
    """Seleniumで開いているページがログイン済みに見えるか簡易判定する。"""
    text = re.sub(r"\s+", " ", BeautifulSoup(html or "", "html.parser").get_text(" "))
    if "ログアウト" in text:
        return True
    if "お気に入り馬" in text and "アカウント" in text and "ログイン" not in text:
        return True
    if "header_nickname" in (html or ""):
        return True
    return False


def _selenium_auto_login_netkeiba(
    driver,
    login_id: str,
    password: str,
    login_url: str = "https://regist.netkeiba.com/account/?pid=login",
    after_login_url: str = "https://db.netkeiba.com/horse/training.html?id=2022105396",
    wait_seconds: int = 30,
    debug_html_dir: str = "data/output/debug_training_html",
) -> None:
    """
    Seleniumでnetkeibaへ自動ログインする。

    v5:
    - ログインフォームのセレクタ候補を拡張
    - iframe内フォームにも対応
    - 失敗時にHTML/スクリーンショット/input一覧を保存・表示
    - 既にログイン済みの場合は入力処理をスキップ
    """
    from selenium.webdriver.common.keys import Keys
    from selenium.webdriver.support.ui import WebDriverWait

    print(f"[INFO] netkeibaログインページを開きます: {login_url}")
    _safe_driver_get(driver, login_url, wait_seconds=max(10, min(wait_seconds, 60)), retry=2)
    time.sleep(2.0)

    # すでにログイン済みの場合は確認URLへ進む
    if _is_netkeiba_logged_in_html(driver.page_source or ""):
        print("[INFO] すでにログイン済みに見えるため、ログイン入力をスキップします")
    else:
        submit_selectors = [
            'form[action*="account"] input[type="image"]',
            'input[type="image"][alt*="ログイン"]',
            'input[type="image"]',
            'button[type="submit"]',
            'input[type="submit"]',
            'button.loginBtn',
            '.loginBtn',
            '.Login_Btn',
            '.Submit_Btn',
            'input[value*="ログイン"]',
            'button[class*="login"]',
            'a[class*="login"]',
        ]

        try:
            _print_input_debug(driver)
            id_input = _find_login_id_input(driver, timeout_sec=max(10, min(wait_seconds, 60)))
            pw_input = _find_password_input(driver, timeout_sec=max(10, min(wait_seconds, 60)))
        except Exception as e:
            print("[ERROR] ログインID/パスワード入力欄を見つけられませんでした")
            print(f"[ERROR] err={type(e).__name__}: {e}")
            _print_input_debug(driver)
            _save_selenium_debug(driver, debug_dir=debug_html_dir, prefix="netkeiba_login_form_not_found")
            raise RuntimeError(
                "netkeibaログインフォームを自動検出できませんでした。"
                "保存された debug HTML / PNG を確認してください。"
            ) from e

        id_input.clear()
        id_input.send_keys(login_id)
        pw_input.clear()
        pw_input.send_keys(password)

        # netkeibaのログインボタンは input type=image のことがあるため、
        # クリック → Enter → form.submit() の順で確実に送信する。
        clicked = _click_first_available(driver, submit_selectors, timeout_sec=5)
        if not clicked:
            print("[WARN] ログインボタンをクリックできなかったため、Enter送信を試します")
            try:
                pw_input.send_keys(Keys.ENTER)
                clicked = True
            except Exception as e:
                print(f"[WARN] Enter送信に失敗しました: {type(e).__name__}: {e}")

        if not clicked:
            print("[WARN] Enter送信もできなかったため、form.submit() を試します")
            try:
                driver.execute_script("arguments[0].closest('form').submit();", pw_input)
                clicked = True
            except Exception as e:
                print(f"[ERROR] form.submit() も失敗しました: {type(e).__name__}: {e}")
                _save_selenium_debug(driver, debug_dir=debug_html_dir, prefix="netkeiba_login_submit_failed")
                raise

        # ログイン後の画面遷移待ち
        time.sleep(5.0)

        # まだログインフォームが残っている場合は、ID/PW誤り・追加認証・送信失敗の可能性が高い
        try:
            current_html = driver.page_source or ""
            if 'name="login_id"' in current_html and 'name="pswd"' in current_html:
                print("[WARN] ログイン送信後もログインフォームが残っています。ID/PW誤り・追加認証・送信失敗の可能性があります")
                _save_selenium_debug(driver, debug_dir=debug_html_dir, prefix="netkeiba_login_still_form")
        except Exception:
            pass

    # 調教ページへ移動して、ログイン済みCookieが有効か確認する
    print(f"[INFO] ログイン後の確認URLを開きます: {after_login_url}")
    try:
        driver.switch_to.default_content()
    except Exception:
        pass
    _safe_driver_get(driver, after_login_url, wait_seconds=max(10, min(wait_seconds, 60)), retry=2)
    time.sleep(2.0)

    try:
        WebDriverWait(driver, wait_seconds).until(lambda d: d.execute_script("return document.readyState") == "complete")
    except Exception:
        pass

    page_html = driver.page_source or ""
    status = _classify_training_html(page_html)
    print(f"[INFO] Selenium確認ページ判定: {status}")
    if _is_netkeiba_logged_in_html(page_html):
        print("[INFO] Selenium上ではログイン済みに見えます")
    else:
        print("[WARN] Selenium上でログイン済み判定ができませんでした。フォーム変更・認証・ログイン失敗の可能性があります")
        _save_selenium_debug(driver, debug_dir=debug_html_dir, prefix="netkeiba_after_login_not_logged_in")

def _copy_selenium_cookies_to_requests_session(
    session: requests.Session,
    start_url: str = "https://db.netkeiba.com/horse/training.html?id=2022105396",
    wait_seconds: int = 180,
    auto_login: bool = False,
    credentials_ini: str = "",
    login_url: str = "https://regist.netkeiba.com/account/?pid=login",
    keep_browser_open: bool = False,
) -> requests.Session:
    """
    Seleniumでブラウザを開き、ログイン済みCookieをrequests.Sessionへコピーする。

    方式1: 手動ログイン
      --selenium-login のみ指定。
      ブラウザでログイン後、ターミナルでEnter。

    方式2: 自動ログイン
      --selenium-login --auto-login を指定。
      NETKEIBA_LOGIN_ID / NETKEIBA_PASSWORD または credentials.ini からID/PWを読み込む。

    credentials.ini の例:

      [netkeiba]
      login_id = your_mail@example.com
      password = your_password
    """
    try:
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
    except Exception as e:
        raise RuntimeError(
            "Seleniumが使えません。先に `pip install selenium webdriver-manager` を実行してください。"
        ) from e

    chrome_options = Options()
    chrome_options.add_argument("--start-maximized")
    chrome_options.add_argument(f"--user-agent={USER_AGENT}")
    chrome_options.add_experimental_option("excludeSwitches", ["enable-logging"])

    driver = webdriver.Chrome(options=chrome_options)
    try:
        if auto_login:
            login_id, password, source = _load_netkeiba_credentials(credentials_ini)
            if not login_id or not password:
                raise RuntimeError(
                    "自動ログイン用のID/PWが見つかりません。"
                    "環境変数 NETKEIBA_LOGIN_ID / NETKEIBA_PASSWORD、"
                    "または credentials.ini の [netkeiba] login_id/password を設定してください。"
                )
            print(f"[INFO] netkeiba自動ログイン情報を読み込みました: {source}")
            _selenium_auto_login_netkeiba(
                driver=driver,
                login_id=login_id,
                password=password,
                login_url=login_url,
                after_login_url=start_url,
                wait_seconds=min(max(wait_seconds, 10), 120),
                debug_html_dir="data/output/debug_training_html",
            )
        else:
            print("[INFO] Seleniumでnetkeibaログイン用ブラウザを起動します")
            print("[INFO] ブラウザで調教ページが見える状態にしてください")
            print("[INFO] ログイン後、このターミナルで Enter を押すとCookieを取り込みます")
            driver.get(start_url)
            print(f"[INFO] ログイン確認URLを開きました: {start_url}")
            print(f"[INFO] 最大待機目安: {wait_seconds}秒")
            input("[ACTION] ブラウザでログイン・調教ページ表示ができたら Enter を押してください: ")

        cookies = driver.get_cookies()
        if not cookies:
            print("[WARN] SeleniumからCookieを取得できませんでした")
            return session

        copied = 0
        for c in cookies:
            name = c.get("name")
            value = c.get("value")
            domain = c.get("domain") or ".netkeiba.com"
            path = c.get("path") or "/"
            if not name or value is None:
                continue
            try:
                session.cookies.set(name, value, domain=domain, path=path)
                copied += 1
            except Exception:
                session.cookies.set(name, value)
                copied += 1

        cookie_header = "; ".join(
            f"{c.get('name')}={c.get('value')}"
            for c in cookies
            if c.get("name") and c.get("value") is not None
        )
        if cookie_header:
            session.headers.update({"Cookie": cookie_header})

        print(f"[INFO] Selenium Cookieをrequestsへコピーしました: {copied}件")
        return session
    finally:
        if keep_browser_open:
            print("[INFO] --keep-browser-open が指定されたため、ブラウザは開いたままにします")
        else:
            try:
                driver.quit()
            except Exception:
                pass

def _make_session_with_optional_browser_login(
    use_selenium_login: bool = False,
    selenium_login_url: str = "https://db.netkeiba.com/horse/training.html?id=2022105396",
    selenium_wait_seconds: int = 180,
    auto_login: bool = False,
    credentials_ini: str = "",
    netkeiba_login_url: str = "https://regist.netkeiba.com/account/?pid=login",
    keep_browser_open: bool = False,
) -> requests.Session:
    """
    通常requestsセッションを作り、必要ならSeleniumでログインCookieを取り込む。
    """
    session = _make_session()
    if use_selenium_login:
        session = _copy_selenium_cookies_to_requests_session(
            session=session,
            start_url=selenium_login_url,
            wait_seconds=selenium_wait_seconds,
            auto_login=auto_login,
            credentials_ini=credentials_ini,
            login_url=netkeiba_login_url,
            keep_browser_open=keep_browser_open,
        )
    return session


def _fetch_html(session: requests.Session, url: str, timeout: int = DEFAULT_TIMEOUT) -> str:
    """HTMLを取得する。netkeibaは文字コードが揺れるため apparent_encoding を優先する。"""
    res = session.get(url, timeout=timeout)
    res.raise_for_status()

    # netkeiba は EUC-JP / CP932 系になることがあるため apparent_encoding を優先
    if not res.encoding or res.encoding.lower() in {"iso-8859-1", "ascii"}:
        res.encoding = res.apparent_encoding or "utf-8"
    return res.text


def _classify_training_html(html: str) -> str:
    """取得したHTMLが調教ページとして使えるかをざっくり判定する。"""
    text = re.sub(r"\s+", " ", BeautifulSoup(html or "", "html.parser").get_text(" "))
    if not text.strip():
        return "empty_html"
    if any(w in text for w in ["ログイン", "会員登録", "メールアドレス", "パスワード"]):
        return "login_or_cookie_required"
    if any(w in text for w in ["プレミアム", "プレミアムサービス", "有料", "netkeibaTV"]):
        return "premium_or_permission_required"
    if any(w in text for w in ["データがありません", "該当するデータ", "表示できません"]):
        return "no_training_data_on_page"
    if any(w in text for w in ["調教", "追い切", "追切", "坂路", "栗東", "美浦", "CW", "南W"]):
        return "looks_like_training_page"
    return "unknown_layout"


def _save_debug_html(html: str, debug_html_dir: Optional[str], horse_id: str, reason: str) -> None:
    """調教抽出失敗時にHTMLを保存する。原因確認用。"""
    if not debug_html_dir:
        return
    try:
        out_dir = Path(debug_html_dir)
        out_dir.mkdir(parents=True, exist_ok=True)
        safe_reason = re.sub(r"[^0-9A-Za-z_\-]+", "_", str(reason))[:60]
        path = out_dir / f"training_{horse_id}_{safe_reason}.html"
        path.write_text(html or "", encoding="utf-8", errors="ignore")
        print(f"[DEBUG] 調教HTMLを保存しました: {path}")
    except Exception as e:
        print(f"[WARN] 調教HTMLの保存に失敗しました: horse_id={horse_id} err={e}")


# ============================================================
# 今走Excelから対象馬を作る
# ============================================================
def load_entries_from_excel(src_excel_path: str, raceday: str = "") -> pd.DataFrame:
    """
    既存Excelの「今走レース情報」シートから、対象レースの登録馬を読み込む。

    返す列:
    - rid_str
    - 馬番
    - 馬名
    - レースID
    - レース名
    - 場所
    - 頭数
    - horse_id  ※Excelにあれば入る。無ければ後でnetkeiba出馬表から補完。
    """
    book = pd.read_excel(src_excel_path, sheet_name=None, engine="openpyxl")
    if NOW_SHEET not in book:
        raise RuntimeError(f"今走シートが見つかりません: {NOW_SHEET} / {src_excel_path}")

    now = book[NOW_SHEET].copy()

    if raceday:
        date_col = _pick_col(now, ["日付", "開催日", "日付(月日)", "年月日", "raceday"])
        if date_col is not None:
            s = now[date_col].astype(str).str.replace("/", "", regex=False).str.replace("-", "", regex=False)
            now = now.loc[s.str.contains(str(raceday), na=False)].copy()

    now = _ensure_rid_str(now, label="training(NOW)")
    if "rid_str" not in now.columns and "レースID" in now.columns:
        now["rid_str"] = now["レースID"]
    if "rid_str" not in now.columns:
        raise RuntimeError("今走レース情報に rid_str / レースID が見つかりません")

    now["rid_str"] = _normalize_rid_series(now["rid_str"])

    if "馬番" not in now.columns:
        for cand in ["馬 番", "umaban", "馬番 "]:
            if cand in now.columns:
                now["馬番"] = now[cand]
                break
    if "馬番" not in now.columns:
        now["馬番"] = pd.NA
    now["馬番"] = _normalize_umaban_series(now["馬番"])

    if "馬名" not in now.columns:
        for cand in ["horse_name", "name", "馬 名"]:
            if cand in now.columns:
                now["馬名"] = now[cand]
                break
    if "馬名" not in now.columns:
        raise RuntimeError("今走レース情報に馬名列が見つかりません")

    # horse_id が既にExcelにあれば使う
    horse_id_col = _pick_col(now, ["horse_id", "馬ID", "netkeiba_horse_id", "netkeiba_id"])
    if horse_id_col is not None:
        now["horse_id"] = now[horse_id_col].astype(str).str.extract(r"(\d+)", expand=False).fillna("")
    else:
        now["horse_id"] = ""

    keep_cols = [
        "rid_str", "馬番", "馬名", "horse_id",
        "レースID", "レース名", "場所", "頭数", "人気", "単勝オッズ", "複勝オッズ",
    ]
    for c in keep_cols:
        if c not in now.columns:
            now[c] = pd.NA

    out = now[keep_cols].copy()
    out["馬名_norm"] = out["馬名"].map(_norm_name)
    out = out[out["rid_str"].astype(str).str.len() >= 10].copy()
    out = out[out["馬名_norm"] != ""].copy()
    return out.reset_index(drop=True)


# ============================================================
# 出馬表から horse_id を取得
# ============================================================
def scrape_horse_ids_from_race(
    race_id: str,
    session: Optional[requests.Session] = None,
    sleep_sec: float = DEFAULT_SLEEP_SEC,
) -> pd.DataFrame:
    """
    race.netkeiba の出馬表から、馬名→horse_id を取得する。

    返す列:
    - rid_str
    - 馬名
    - 馬名_norm
    - horse_id
    - horse_url
    """
    session = session or _make_session()
    race_id = str(race_id).strip()
    rows: List[Dict[str, Any]] = []

    urls = [
        NETKEIBA_RACE_URL.format(race_id=race_id),
        NETKEIBA_DB_RACE_URL.format(race_id=race_id),
    ]

    last_err: Optional[Exception] = None
    html = ""
    used_url = ""
    for url in urls:
        try:
            html = _fetch_html(session, url)
            used_url = url
            if html:
                break
        except Exception as e:
            last_err = e
            continue

    if not html:
        print(f"[WARN] 出馬表HTML取得失敗: race_id={race_id} err={last_err}")
        return pd.DataFrame(columns=["rid_str", "馬名", "馬名_norm", "horse_id", "horse_url"])

    soup = BeautifulSoup(html, "html.parser")

    # 馬リンクを広く拾う
    for a in soup.select('a[href*="/horse/"]'):
        href = a.get("href") or ""
        horse_id = _extract_horse_id_from_url(href)
        name = _norm_text(a.get_text(" ", strip=True))
        if not horse_id or not name:
            continue
        # 「血統」などのリンク文字が混ざる可能性を避ける
        if len(name) <= 1 or name in {"血統", "掲示板", "調教", "厩舎"}:
            continue
        rows.append(
            {
                "rid_str": race_id,
                "馬名": name,
                "馬名_norm": _norm_name(name),
                "horse_id": horse_id,
                "horse_url": urljoin(used_url, href),
            }
        )

    if sleep_sec > 0:
        time.sleep(float(sleep_sec))

    df = pd.DataFrame(rows)
    if df.empty:
        print(f"[WARN] horse_id を出馬表から取得できませんでした: race_id={race_id}")
        return pd.DataFrame(columns=["rid_str", "馬名", "馬名_norm", "horse_id", "horse_url"])

    df = df.drop_duplicates(subset=["rid_str", "horse_id"], keep="first")
    return df.reset_index(drop=True)


def attach_horse_ids(
    entries_df: pd.DataFrame,
    session: Optional[requests.Session] = None,
    sleep_sec: float = DEFAULT_SLEEP_SEC,
) -> pd.DataFrame:
    """今走登録馬に horse_id を付ける。Excelに無ければ出馬表から補完する。"""
    session = session or _make_session()
    out = entries_df.copy()
    out["horse_id"] = out.get("horse_id", "").astype(str).str.extract(r"(\d+)", expand=False).fillna("")
    out["馬名_norm"] = out["馬名"].map(_norm_name)

    need = out["horse_id"].astype(str).str.strip().eq("")
    if not need.any():
        return out

    maps: List[pd.DataFrame] = []
    for rid in sorted(out.loc[need, "rid_str"].dropna().astype(str).unique()):
        print(f"[INFO] 出馬表から horse_id を取得します: race_id={rid}")
        m = scrape_horse_ids_from_race(rid, session=session, sleep_sec=sleep_sec)
        if not m.empty:
            maps.append(m)

    if not maps:
        return out

    id_map = pd.concat(maps, ignore_index=True)
    id_map = id_map.drop_duplicates(subset=["rid_str", "馬名_norm"], keep="first")

    out = out.merge(
        id_map[["rid_str", "馬名_norm", "horse_id", "horse_url"]].rename(
            columns={"horse_id": "horse_id_scraped"}
        ),
        on=["rid_str", "馬名_norm"],
        how="left",
    )
    out["horse_id"] = out["horse_id"].where(out["horse_id"].astype(str).str.strip().ne(""), out["horse_id_scraped"])
    out["horse_id"] = out["horse_id"].fillna("").astype(str)
    out = out.drop(columns=["horse_id_scraped"], errors="ignore")
    return out


# ============================================================
# 調教ページのスクレイピング
# ============================================================


def _parse_training_rows_from_html(html: str, horse_id: str, source_url: str = "") -> pd.DataFrame:
    """
    netkeiba馬別「調教」ページ専用のHTMLパーサー。

    今回アップロードされたHTMLでは、調教タイムは以下の構造です。
    - table summary="調教タイム" class="race_table_01 nk_tb_common"
    - caption に 対象レース名・race_id・結果
    - td.TrainingTimeData 内の li が 5本の時計
    - p.TrainingHeisou に併せ馬情報

    pandas.read_html でも読めますが、併せ馬情報やcaptionを安定して拾うため、
    BeautifulSoupで直接読む処理を先に使います。
    """
    soup = BeautifulSoup(html, "html.parser")
    out_rows: List[Dict[str, Any]] = []

    tables = soup.select('table[summary="調教タイム"], table.race_table_01.nk_tb_common')
    for table in tables:
        caption_text = ""
        race_id = ""
        race_name = ""
        race_result = ""

        cap = table.find("caption")
        if cap is not None:
            caption_text = _norm_text(cap.get_text(" ", strip=True))
            a = cap.find("a", href=True)
            if a is not None:
                race_name = _norm_text(a.get_text(" ", strip=True))
                m = re.search(r"/race/(\d{12})", a.get("href", ""))
                if m:
                    race_id = m.group(1)
            mres = re.search(r"結果\s*[：:]\s*([^\s]+)", caption_text)
            if mres:
                race_result = mres.group(1)

        short_comment = ""
        for tr in table.find_all("tr"):
            cells = tr.find_all(["td", "th"])
            if not cells:
                continue

            # [短評] 行
            row_text = _norm_text(tr.get_text(" ", strip=True))
            if "[短評]" in row_text or "短評" in row_text:
                short_comment = row_text
                continue

            # 調教時計行だけ対象にする
            time_td = tr.select_one("td.TrainingTimeData")
            if time_td is None:
                continue

            tds = tr.find_all("td")
            if len(tds) < 7:
                continue

            date_text = _norm_text(tds[0].get_text(" ", strip=True))
            training_date = _parse_date(date_text)
            if training_date is None:
                continue

            course = _norm_text(tds[1].get_text(" ", strip=True)) if len(tds) > 1 else ""
            baba = _norm_text(tds[2].get_text(" ", strip=True)) if len(tds) > 2 else ""
            rider = _norm_text(tds[3].get_text(" ", strip=True)) if len(tds) > 3 else ""
            position = _norm_text(tds[5].get_text(" ", strip=True)) if len(tds) > 5 else ""
            footwork = _norm_text(tds[6].get_text(" ", strip=True)) if len(tds) > 6 else ""
            eval_text = _norm_text(tds[7].get_text(" ", strip=True)) if len(tds) > 7 else ""
            eval_grade = _norm_text(tds[8].get_text(" ", strip=True)) if len(tds) > 8 else ""

            raw_time_values: List[float] = []
            for li in time_td.select("ul.TrainingTimeDataList li"):
                val = _to_float(li.get_text(" ", strip=True))
                if pd.notna(val):
                    raw_time_values.append(float(val))

            if not raw_time_values:
                continue

            # 表示は長い距離→短い距離の順。
            # 5個なら 5F,4F,3F,2F,1F として扱う。
            times = {
                "time_6f": np.nan,
                "time_5f": np.nan,
                "time_4f": np.nan,
                "time_3f": np.nan,
                "time_2f": np.nan,
                "time_1f": np.nan,
            }
            tail = raw_time_values[-6:]
            keys = ["time_6f", "time_5f", "time_4f", "time_3f", "time_2f", "time_1f"][-len(tail):]
            for k, v in zip(keys, tail):
                times[k] = v

            heisou_text = " ".join(_norm_text(p.get_text(" ", strip=True)) for p in time_td.select("p.TrainingHeisou"))
            text_for_partner = heisou_text or row_text
            if "先着" in text_for_partner:
                partner_result = "先着"
            elif "同入" in text_for_partner or "併入" in text_for_partner:
                partner_result = "同入"
            elif "遅" in text_for_partner:
                partner_result = "遅れ"
            else:
                partner_result = ""

            if "馬也" in footwork or "馬なり" in footwork:
                footwork_norm = "馬なり"
            elif "強" in footwork:
                footwork_norm = "強め"
            elif "一杯" in footwork or "一ぱい" in footwork:
                footwork_norm = "一杯"
            elif "仕掛" in footwork:
                footwork_norm = "仕掛け"
            else:
                footwork_norm = footwork

            raw_text = " ".join(
                x for x in [
                    date_text, course, baba, rider,
                    " ".join(str(x) for x in raw_time_values),
                    position, footwork, eval_text, eval_grade, heisou_text,
                    short_comment, caption_text,
                ] if x
            )

            out_rows.append(
                {
                    "horse_id": str(horse_id),
                    "training_date": training_date,
                    "course": course,
                    "baba": baba,
                    "rider": rider,
                    "position": position,
                    "footwork": footwork_norm,
                    "partner_result": partner_result,
                    "eval_text": eval_text,
                    "eval_grade": eval_grade,
                    "heisou_text": heisou_text,
                    "short_comment": short_comment,
                    "race_id": race_id,
                    "race_name": race_name,
                    "race_result": race_result,
                    **times,
                    "raw_text": raw_text,
                    "source_url": source_url,
                }
            )

    out = pd.DataFrame(out_rows)
    if out.empty:
        return out
    out = out.drop_duplicates(subset=["horse_id", "training_date", "raw_text"], keep="first")
    out = out.sort_values(["training_date"], ascending=False, na_position="last", kind="mergesort")
    return out.reset_index(drop=True)


def _read_training_tables_from_html(html: str) -> List[pd.DataFrame]:
    """HTML内のテーブルから、調教っぽいテーブルだけを返す。"""
    tables: List[pd.DataFrame] = []
    try:
        raw_tables = pd.read_html(html)
    except Exception:
        raw_tables = []

    for t in raw_tables:
        if t is None or t.empty:
            continue
        df = t.copy()

        # MultiIndex列対策
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = ["_".join([str(x) for x in col if str(x) != "nan"]).strip("_") for col in df.columns]
        else:
            df.columns = [str(c) for c in df.columns]

        col_text = " ".join(map(str, df.columns))
        body_text = " ".join(df.astype(str).head(3).to_numpy().ravel().tolist())
        text = col_text + " " + body_text

        # 調教テーブル判定。ページ構造変更に備えて広めに拾う。
        has_training_word = any(w in text for w in ["調教", "追切", "追い切", "坂", "CW", "Ｗ", "南W", "栗東", "美浦"])
        has_time_like = bool(re.search(r"\d{1,2}\.\d", text))
        has_date_like = any(w in col_text for w in ["日付", "年月日", "日時"])

        if has_time_like and (has_training_word or has_date_like):
            tables.append(df)

    return tables


def _extract_times_from_row(row: pd.Series) -> Dict[str, float]:
    """
    1行の中から 6F/5F/4F/3F/2F/1F っぽいタイムを抽出する。
    テーブル列が明確なら列名から拾い、無ければ行全体の数値列から後ろを1F扱いする。
    """
    result = {"time_6f": np.nan, "time_5f": np.nan, "time_4f": np.nan, "time_3f": np.nan, "time_2f": np.nan, "time_1f": np.nan}

    # 列名から拾う
    for col, val in row.items():
        c = _clean_colname(col).upper()
        v = _to_float(val)
        if pd.isna(v):
            continue
        if any(k in c for k in ["6F", "６Ｆ", "6ハロン"]):
            result["time_6f"] = v
        elif any(k in c for k in ["5F", "５Ｆ", "5ハロン"]):
            result["time_5f"] = v
        elif any(k in c for k in ["4F", "４Ｆ", "800"]):
            result["time_4f"] = v
        elif any(k in c for k in ["3F", "３Ｆ", "600"]):
            result["time_3f"] = v
        elif any(k in c for k in ["2F", "２Ｆ", "400"]):
            result["time_2f"] = v
        elif any(k in c for k in ["1F", "１Ｆ", "200", "ラスト"]):
            result["time_1f"] = v

    if not pd.isna(result["time_1f"]):
        return result

    # 行全体から秒っぽい数字を拾う。
    row_text = " ".join(_norm_text(x) for x in row.to_list())
    nums = [float(x) for x in re.findall(r"\d{1,3}\.\d", row_text)]

    # 調教によく出る 11.0〜99.9 の範囲だけ残す
    nums = [x for x in nums if 10.0 <= x <= 120.0]
    if not nums:
        return result

    # 一般的な表示は長い距離→短い距離の順。最後を1Fとして扱う。
    tail = nums[-6:]
    keys = ["time_6f", "time_5f", "time_4f", "time_3f", "time_2f", "time_1f"][-len(tail):]
    for k, v in zip(keys, tail):
        result[k] = v
    return result


def _extract_course(row: pd.Series) -> str:
    for cand in ["コース", "場所", "調教コース", "馬場"]:
        col = _pick_col(pd.DataFrame(columns=row.index), [cand])
        if col is not None:
            val = _norm_text(row.get(col))
            if val:
                return val
    text = " ".join(_norm_text(x) for x in row.to_list())
    for pat in ["栗坂", "美坂", "栗CW", "栗ＣＷ", "美W", "美Ｗ", "南W", "南Ｗ", "CW", "ＣＷ", "坂路", "ウッド", "ポリ"]:
        if pat in text:
            return pat
    return ""


def _extract_footwork(row: pd.Series) -> str:
    text = " ".join(_norm_text(x) for x in row.to_list())
    if "馬なり" in text or "馬也" in text:
        return "馬なり"
    if "強め" in text:
        return "強め"
    if "一杯" in text or "一ぱい" in text:
        return "一杯"
    if "仕掛" in text:
        return "仕掛け"
    return ""


def _extract_partner_result(row: pd.Series) -> str:
    text = " ".join(_norm_text(x) for x in row.to_list())
    if "先着" in text:
        return "先着"
    if "同入" in text or "併入" in text:
        return "同入"
    if "遅" in text:
        return "遅れ"
    return ""


def scrape_training_by_horse_id(
    horse_id: str,
    session: Optional[requests.Session] = None,
    sleep_sec: float = DEFAULT_SLEEP_SEC,
    debug_html_dir: Optional[str] = None,
) -> pd.DataFrame:
    """
    1頭分の調教データを取得する。

    返す列:
    - horse_id
    - training_date
    - course
    - footwork
    - partner_result
    - time_6f, time_5f, time_4f, time_3f, time_2f, time_1f
    - raw_text
    - source_url
    """
    session = session or _make_session()
    horse_id = str(horse_id).strip()
    if not horse_id:
        return pd.DataFrame()

    url = NETKEIBA_TRAINING_URL.format(horse_id=horse_id)

    try:
        html = _fetch_html(session, url)
    except Exception as e:
        print(f"[WARN] 調教ページ取得失敗: horse_id={horse_id} url={url} err={e}")
        return pd.DataFrame()

    page_status = _classify_training_html(html)

    # まず、今回確認したnetkeiba調教ページ専用のHTML構造で直接抽出する。
    direct_df = _parse_training_rows_from_html(html, horse_id=horse_id, source_url=url)
    if direct_df is not None and not direct_df.empty:
        if sleep_sec > 0:
            time.sleep(float(sleep_sec))
        return direct_df

    # 直接抽出で取れない場合だけ、従来の pandas.read_html 方式にフォールバックする。
    tables = _read_training_tables_from_html(html)
    rows: List[Dict[str, Any]] = []

    for df in tables:
        date_col = _pick_col(df, ["日付", "年月日", "日時", "Date"])
        for _, r in df.iterrows():
            times = _extract_times_from_row(r)
            if all(pd.isna(v) for v in times.values()):
                continue

            raw_text = " ".join(_norm_text(x) for x in r.to_list())
            training_date = _parse_date(r.get(date_col)) if date_col is not None else _parse_date(raw_text)

            rows.append(
                {
                    "horse_id": horse_id,
                    "training_date": training_date,
                    "course": _extract_course(r),
                    "footwork": _extract_footwork(r),
                    "partner_result": _extract_partner_result(r),
                    **times,
                    "raw_text": raw_text,
                    "source_url": url,
                }
            )

    if sleep_sec > 0:
        time.sleep(float(sleep_sec))

    out = pd.DataFrame(rows)
    if out.empty:
        print(f"[WARN] 調教データを抽出できませんでした: horse_id={horse_id} status={page_status} tables={len(tables)} url={url}")
        _save_debug_html(html, debug_html_dir, horse_id, page_status)
        return pd.DataFrame(
            columns=[
                "horse_id", "training_date", "course", "footwork", "partner_result",
                "time_6f", "time_5f", "time_4f", "time_3f", "time_2f", "time_1f",
                "raw_text", "source_url",
            ]
        )

    out = out.drop_duplicates(subset=["horse_id", "training_date", "raw_text"], keep="first")
    if "training_date" in out.columns:
        out = out.sort_values(["training_date"], ascending=False, na_position="last", kind="mergesort")
    return out.reset_index(drop=True)


# ============================================================
# 調教スコア計算
# ============================================================
@dataclass
class TrainingScoreConfig:
    """調教スコアの設定。最初は控えめなルールベース。"""

    recent_n: int = 5
    good_1f: float = 12.4
    very_good_1f: float = 12.1
    bad_1f: float = 13.4
    good_4f: float = 53.0
    good_5f: float = 66.5
    enough_count: int = 3


def _score_one_training_row(row: pd.Series, cfg: TrainingScoreConfig) -> Tuple[float, List[str]]:
    """調教1本ごとの点数。"""
    score = 0.0
    reasons: List[str] = []

    t1 = _to_float(row.get("time_1f"))
    t4 = _to_float(row.get("time_4f"))
    t5 = _to_float(row.get("time_5f"))
    footwork = _norm_text(row.get("footwork"))
    partner = _norm_text(row.get("partner_result"))

    # ラスト1F
    if not pd.isna(t1):
        if t1 <= cfg.very_good_1f:
            score += 2.0
            reasons.append(f"ラスト1F優秀({t1:.1f})")
        elif t1 <= cfg.good_1f:
            score += 1.0
            reasons.append(f"ラスト1F良好({t1:.1f})")
        elif t1 >= cfg.bad_1f:
            score -= 2.0
            reasons.append(f"ラスト1F失速気味({t1:.1f})")

    # 全体時計。坂路4F or ウッド5F をざっくり評価。
    if not pd.isna(t4) and t4 <= cfg.good_4f:
        score += 1.0
        reasons.append(f"4F好時計({t4:.1f})")
    if not pd.isna(t5) and t5 <= cfg.good_5f:
        score += 1.0
        reasons.append(f"5F好時計({t5:.1f})")

    # 脚色。同じ時計なら馬なりの方を高く見る。
    if footwork == "馬なり":
        if (not pd.isna(t1) and t1 <= cfg.good_1f) or (not pd.isna(t4) and t4 <= cfg.good_4f):
            score += 1.5
            reasons.append("馬なりで好時計")
        else:
            score += 0.3
            reasons.append("馬なり")
    elif footwork == "強め":
        score += 0.3
        reasons.append("強め")
    elif footwork == "一杯":
        if (pd.isna(t1) or t1 > cfg.good_1f) and (pd.isna(t4) or t4 > cfg.good_4f):
            score -= 1.5
            reasons.append("一杯で時計平凡")

    # 併せ馬結果
    if partner == "先着":
        score += 1.0
        reasons.append("併せ先着")
    elif partner == "同入":
        score += 0.5
        reasons.append("併せ同入")
    elif partner == "遅れ":
        score -= 1.5
        reasons.append("併せ遅れ")

    return float(score), reasons


def calc_training_score(training_df: pd.DataFrame, cfg: Optional[TrainingScoreConfig] = None) -> Dict[str, Any]:
    """
    1頭分の調教DataFrameから総合調教スコアを作る。

    出力する主な値:
    - training_score_raw
    - training_score
    - training_count
    - training_recent_count
    - training_best_1f
    - training_best_4f
    - training_last_1f
    - training_last_4f
    - training_judge
    - training_reason
    """
    cfg = cfg or TrainingScoreConfig()

    if training_df is None or training_df.empty:
        return {
            "training_score_raw": 0.0,
            "training_score": 50.0,
            "training_count": 0,
            "training_recent_count": 0,
            "training_best_1f": np.nan,
            "training_best_4f": np.nan,
            "training_last_1f": np.nan,
            "training_last_4f": np.nan,
            "training_judge": "調教データなし",
            "training_reason": "調教ページからデータを取得できませんでした",
        }

    work = training_df.copy()
    if "training_date" in work.columns:
        work = work.sort_values("training_date", ascending=False, na_position="last", kind="mergesort")

    recent = work.head(int(cfg.recent_n)).copy()

    row_scores: List[float] = []
    reason_list: List[str] = []
    for _, r in recent.iterrows():
        s, reasons = _score_one_training_row(r, cfg)
        row_scores.append(s)
        reason_list.extend(reasons)

    raw_score = float(np.nansum(row_scores)) if row_scores else 0.0

    # 本数評価
    training_count = int(len(work))
    recent_count = int(len(recent))
    if recent_count >= cfg.enough_count:
        raw_score += 1.0
        reason_list.append(f"調教本数十分({recent_count}本)")
    elif recent_count <= 1:
        raw_score -= 1.0
        reason_list.append(f"調教本数少なめ({recent_count}本)")

    # 0〜100に寄せる。50が普通、70以上が良い、30以下が不安。
    score_100 = 50.0 + raw_score * 5.0
    score_100 = float(max(0.0, min(100.0, score_100)))

    best_1f = pd.to_numeric(work.get("time_1f"), errors="coerce").min() if "time_1f" in work.columns else np.nan
    best_4f = pd.to_numeric(work.get("time_4f"), errors="coerce").min() if "time_4f" in work.columns else np.nan
    last_1f = pd.to_numeric(recent.get("time_1f"), errors="coerce").iloc[0] if "time_1f" in recent.columns and not recent.empty else np.nan
    last_4f = pd.to_numeric(recent.get("time_4f"), errors="coerce").iloc[0] if "time_4f" in recent.columns and not recent.empty else np.nan

    if score_100 >= 75:
        judge = "かなり良い"
    elif score_100 >= 62:
        judge = "良い"
    elif score_100 >= 45:
        judge = "普通"
    elif score_100 >= 35:
        judge = "やや不安"
    else:
        judge = "不安"

    # 理由は多すぎるとExcelで見にくいので重複削除して先頭だけ
    unique_reasons = list(dict.fromkeys([r for r in reason_list if r]))
    reason_text = " / ".join(unique_reasons[:8]) if unique_reasons else "目立つ加減点なし"

    return {
        "training_score_raw": round(raw_score, 3),
        "training_score": round(score_100, 2),
        "training_count": training_count,
        "training_recent_count": recent_count,
        "training_best_1f": round(float(best_1f), 2) if pd.notna(best_1f) else np.nan,
        "training_best_4f": round(float(best_4f), 2) if pd.notna(best_4f) else np.nan,
        "training_last_1f": round(float(last_1f), 2) if pd.notna(last_1f) else np.nan,
        "training_last_4f": round(float(last_4f), 2) if pd.notna(last_4f) else np.nan,
        "training_judge": judge,
        "training_reason": reason_text,
    }


# ============================================================
# 対象レース全馬に調教スコアを付けるメイン関数
# ============================================================
def build_training_scores_for_excel(
    src_excel_path: str,
    raceday: str = "",
    sleep_sec: float = DEFAULT_SLEEP_SEC,
    output_raw_training_csv: Optional[str] = None,
    max_horses: int = 0,
    stop_after_no_data: int = 20,
    debug_html_dir: Optional[str] = None,
    session: Optional[requests.Session] = None,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    既存の今走Excelから対象馬を読み、全馬に調教スコアを付ける。

    戻り値:
    - score_df: 1行=今走出走馬。pipelineにmergeしやすい調教スコア表。
    - raw_df:   1行=調教1本。確認・デバッグ用。
    """
    session = session or _make_session()

    entries = load_entries_from_excel(src_excel_path, raceday=raceday)
    print(f"[INFO] 今走登録馬を読み込みました: {len(entries)}頭")

    if max_horses and int(max_horses) > 0:
        entries = entries.head(int(max_horses)).copy()
        print(f"[INFO] テスト用に取得対象を先頭 {len(entries)} 頭に制限します")

    entries = attach_horse_ids(entries, session=session, sleep_sec=sleep_sec)
    missing = entries[entries["horse_id"].astype(str).str.strip().eq("")]
    if not missing.empty:
        print(f"[WARN] horse_id を取得できない馬がいます: {len(missing)}頭")
        for _, r in missing.head(20).iterrows():
            print(f"  - race_id={r.get('rid_str')} 馬番={r.get('馬番')} 馬名={r.get('馬名')}")

    raw_list: List[pd.DataFrame] = []
    cache: Dict[str, pd.DataFrame] = {}
    score_rows: List[Dict[str, Any]] = []
    consecutive_no_data = 0

    for i, r in entries.iterrows():
        horse_id = str(r.get("horse_id") or "").strip()
        horse_name = str(r.get("馬名") or "").strip()
        rid = str(r.get("rid_str") or "").strip()
        umaban = r.get("馬番")

        print(f"[INFO] 調教取得: {i + 1}/{len(entries)} race_id={rid} 馬番={umaban} 馬名={horse_name} horse_id={horse_id}")

        if horse_id:
            if horse_id in cache:
                train_df = cache[horse_id]
            else:
                train_df = scrape_training_by_horse_id(
                    horse_id,
                    session=session,
                    sleep_sec=sleep_sec,
                    debug_html_dir=debug_html_dir,
                )
                cache[horse_id] = train_df
        else:
            train_df = pd.DataFrame()

        if not train_df.empty:
            consecutive_no_data = 0
            tmp = train_df.copy()
            tmp["rid_str"] = rid
            tmp["馬番"] = umaban
            tmp["馬名"] = horse_name
            raw_list.append(tmp)
        else:
            consecutive_no_data += 1
            if stop_after_no_data and int(stop_after_no_data) > 0 and consecutive_no_data >= int(stop_after_no_data):
                print(
                    f"[ERROR] 調教データなしが {consecutive_no_data} 頭連続しました。"
                    "Cookie不足・ログインページ取得・ページ構造変更の可能性が高いため停止します。"
                )
                print("[HINT] まず --max-horses 5 --debug-html-dir を付けてHTMLの中身を確認してください。")
                break

        score_info = calc_training_score(train_df)
        score_rows.append(
            {
                "rid_str": rid,
                "馬番": umaban,
                "馬名": horse_name,
                "horse_id": horse_id,
                "training_url": NETKEIBA_TRAINING_URL.format(horse_id=horse_id) if horse_id else "",
                **score_info,
            }
        )

    score_df = pd.DataFrame(score_rows)
    if not score_df.empty:
        score_df["rid_str"] = _normalize_rid_series(score_df["rid_str"])
        score_df["馬番"] = _normalize_umaban_series(score_df["馬番"])
        score_df = score_df.sort_values(["rid_str", "馬番"], kind="mergesort").reset_index(drop=True)

    raw_df = pd.concat(raw_list, ignore_index=True) if raw_list else pd.DataFrame()
    if output_raw_training_csv and not raw_df.empty:
        Path(output_raw_training_csv).parent.mkdir(parents=True, exist_ok=True)
        raw_df.to_csv(output_raw_training_csv, index=False, encoding="utf-8-sig")
        print(f"[INFO] 調教明細CSVを保存しました: {output_raw_training_csv}")

    return score_df, raw_df


def append_training_scores_to_excel(
    src_excel_path: str,
    out_excel_path: str,
    score_df: pd.DataFrame,
    sheet_name: str = "調教スコア",
) -> None:
    """
    調教スコアをExcelに書き込む。
    既存pipelineを壊さないよう、まずは別シート「調教スコア」として追加・置換する。
    """
    if score_df is None:
        score_df = pd.DataFrame()

    src = Path(src_excel_path)
    out = Path(out_excel_path)
    out.parent.mkdir(parents=True, exist_ok=True)

    if src.resolve() != out.resolve():
        import shutil
        shutil.copy2(src, out)

    with pd.ExcelWriter(out, engine="openpyxl", mode="a", if_sheet_exists="replace") as writer:
        score_df.to_excel(writer, sheet_name=sheet_name, index=False)

    print(f"[INFO] 調教スコアシートを書き込みました: {out} / sheet={sheet_name}")


# ============================================================
# 後でpipelineへ組み込むためのmerge用関数
# ============================================================
def merge_training_scores(base_df: pd.DataFrame, training_score_df: pd.DataFrame) -> pd.DataFrame:
    """
    TARGET や 今走レース情報 に調教スコア列を結合するための関数。
    後で keibayosou_pipeline.py から import して使える形にしている。
    """
    if base_df is None or base_df.empty:
        return base_df
    if training_score_df is None or training_score_df.empty:
        out = base_df.copy()
        for c in ["training_score", "training_judge", "training_reason"]:
            if c not in out.columns:
                out[c] = pd.NA
        return out

    base = base_df.copy()
    tr = training_score_df.copy()

    base = _ensure_rid_str(base, label="merge_training_scores(base)")
    tr = _ensure_rid_str(tr, label="merge_training_scores(training)")
    base["rid_str"] = _normalize_rid_series(base["rid_str"])
    tr["rid_str"] = _normalize_rid_series(tr["rid_str"])

    if "馬番" in base.columns:
        base["馬番"] = _normalize_umaban_series(base["馬番"])
    if "馬番" in tr.columns:
        tr["馬番"] = _normalize_umaban_series(tr["馬番"])

    use_cols = [
        "rid_str", "馬番", "馬名", "horse_id", "training_url",
        "training_score_raw", "training_score", "training_count", "training_recent_count",
        "training_best_1f", "training_best_4f", "training_last_1f", "training_last_4f",
        "training_judge", "training_reason",
    ]
    use_cols = [c for c in use_cols if c in tr.columns]

    # 馬番があるなら race_id + 馬番 優先。無ければ race_id + 馬名で結合。
    if "馬番" in base.columns and "馬番" in tr.columns:
        return base.merge(tr[use_cols].drop_duplicates(["rid_str", "馬番"]), on=["rid_str", "馬番"], how="left", suffixes=("", "_training"))

    if "馬名" in base.columns and "馬名" in tr.columns:
        base["__馬名_norm__"] = base["馬名"].map(_norm_name)
        tr["__馬名_norm__"] = tr["馬名"].map(_norm_name)
        use_cols2 = [c for c in use_cols if c != "馬名"] + ["__馬名_norm__"]
        out = base.merge(tr[use_cols2].drop_duplicates(["rid_str", "__馬名_norm__"]), on=["rid_str", "__馬名_norm__"], how="left")
        return out.drop(columns=["__馬名_norm__"], errors="ignore")

    return base




# ============================================================
# v7追加：調教データをできるだけ全項目取得し、自己過去比較も出す
# ============================================================


def _parse_finish_order_from_result_text(x: Any) -> float:
    """'1着' / '14着' / '中止' などから着順数値を返す。"""
    s = _norm_text(x)
    m = re.search(r"(\d+)\s*着", s)
    if m:
        return float(m.group(1))
    return np.nan


def _safe_numeric_series(df: pd.DataFrame, col: str) -> pd.Series:
    if df is None or df.empty or col not in df.columns:
        return pd.Series(dtype="float64")
    return pd.to_numeric(df[col], errors="coerce")


def _safe_mean(df: pd.DataFrame, col: str) -> float:
    s = _safe_numeric_series(df, col).dropna()
    return float(s.mean()) if not s.empty else np.nan


def _safe_min(df: pd.DataFrame, col: str) -> float:
    s = _safe_numeric_series(df, col).dropna()
    return float(s.min()) if not s.empty else np.nan


def _safe_latest_value(df: pd.DataFrame, col: str) -> Any:
    if df is None or df.empty or col not in df.columns:
        return np.nan
    v = df.iloc[0].get(col)
    return v


def _round_or_nan(v: Any, ndigits: int = 2) -> Any:
    try:
        if pd.isna(v):
            return np.nan
        return round(float(v), ndigits)
    except Exception:
        return v


def _parse_training_caption(caption_text: str) -> Dict[str, str]:
    """captionからレース日・場所・Rなどをなるべく抽出。"""
    text = _norm_text(caption_text)
    out = {"race_date_text": "", "race_place": "", "race_no": ""}
    m = re.search(r"(\d{4}/\d{1,2}/\d{1,2})", text)
    if m:
        out["race_date_text"] = m.group(1)
    m = re.search(r"(札幌|函館|福島|新潟|東京|中山|中京|京都|阪神|小倉)(\d{1,2})R", text)
    if m:
        out["race_place"] = m.group(1)
        out["race_no"] = m.group(2)
    return out


def _make_training_page_url(current_url: str, page_no: str) -> str:
    """
    netkeiba調教ページの JavaScript ページャー用URLを作る。

    調教ページのページャーは、HTML上では以下のように出ることがあります。
      href="javascript:paging('2')"

    これはそのまま requests.get() できないため、
    現在URLの id / mode を引き継ぎ、page=N のURLへ変換します。
    """
    try:
        parsed = urlparse(current_url)
        qs = parse_qs(parsed.query)
        qs["page"] = [str(page_no)]
        new_query = urlencode(qs, doseq=True)
        return urlunparse((
            parsed.scheme,
            parsed.netloc,
            parsed.path,
            parsed.params,
            new_query,
            parsed.fragment,
        ))
    except Exception:
        sep = "&" if "?" in current_url else "?"
        return f"{current_url}{sep}page={page_no}"


def _find_next_training_urls(html: str, current_url: str) -> List[str]:
    """
    調教ページのページャーから次ページ候補URLを拾う。

    v8修正:
    - href="javascript:paging('2')" をそのままURL化しない
    - page=2 の通常URLへ変換してから queue に追加する
    - これにより「No connection adapters were found for javascript:paging('2')」を防ぐ
    """
    soup = BeautifulSoup(html, "html.parser")
    urls: List[str] = []

    for a in soup.select("ul.pager a[href], .pager a[href]"):
        label = _norm_text(a.get_text(" ", strip=True))
        href = (a.get("href", "") or "").strip()
        if not href:
            continue

        full = ""

        # netkeiba調教ページのページャー: javascript:paging('2')
        m = re.search(r"paging\(['\"]?(\d+)['\"]?\)", href)
        if m:
            full = _make_training_page_url(current_url, m.group(1))
        elif href.lower().startswith("javascript:"):
            # javascriptリンクはURLではないのでスキップ
            continue
        elif "次" in label or "page=" in href:
            full = urljoin(current_url, href)

        if full and full not in urls:
            urls.append(full)

    return urls


def _parse_training_rows_from_html(html: str, horse_id: str, source_url: str = "") -> pd.DataFrame:
    """
    v7版HTMLパーサー。

    取得できるものはできるだけ raw_df に残す。
    - caption情報
    - レースID/レース名/レース結果
    - 日付/コース/馬場/乗り役/位置/脚色/評価/評価ランク
    - 6F〜1Fタイム
    - 時計の色クラス
    - 併せ馬テキスト
    - 短評
    - 動画URL
    - 行全文
    """
    soup = BeautifulSoup(html, "html.parser")
    out_rows: List[Dict[str, Any]] = []

    tables = soup.select('table[summary="調教タイム"], table.race_table_01.nk_tb_common')
    for table_index, table in enumerate(tables, start=1):
        caption_text = ""
        race_id = ""
        race_name = ""
        race_result = ""
        race_link = ""

        cap = table.find("caption")
        if cap is not None:
            caption_text = _norm_text(cap.get_text(" ", strip=True))
            a = cap.find("a", href=True)
            if a is not None:
                race_name = _norm_text(a.get_text(" ", strip=True))
                race_link = urljoin(source_url or "https://db.netkeiba.com/", a.get("href", ""))
                m = re.search(r"/race/(\d{12})", a.get("href", ""))
                if m:
                    race_id = m.group(1)
            mres = re.search(r"結果\s*[：:]\s*([^\s]+)", caption_text)
            if mres:
                race_result = mres.group(1)
        caption_info = _parse_training_caption(caption_text)

        short_comment = ""
        training_row_no = 0
        for tr in table.find_all("tr"):
            cells = tr.find_all(["td", "th"])
            if not cells:
                continue

            row_text = _norm_text(tr.get_text(" ", strip=True))
            if "[短評]" in row_text or "短評" in row_text:
                short_comment = row_text
                continue

            time_td = tr.select_one("td.TrainingTimeData")
            if time_td is None:
                continue

            tds = tr.find_all("td")
            if len(tds) < 7:
                continue

            date_text = _norm_text(tds[0].get_text(" ", strip=True))
            training_date = _parse_date(date_text)
            if training_date is None:
                continue

            training_row_no += 1
            course = _norm_text(tds[1].get_text(" ", strip=True)) if len(tds) > 1 else ""
            baba = _norm_text(tds[2].get_text(" ", strip=True)) if len(tds) > 2 else ""
            rider = _norm_text(tds[3].get_text(" ", strip=True)) if len(tds) > 3 else ""
            position = _norm_text(tds[5].get_text(" ", strip=True)) if len(tds) > 5 else ""
            footwork_raw = _norm_text(tds[6].get_text(" ", strip=True)) if len(tds) > 6 else ""
            eval_text = _norm_text(tds[7].get_text(" ", strip=True)) if len(tds) > 7 else ""
            eval_grade = _norm_text(tds[8].get_text(" ", strip=True)) if len(tds) > 8 else ""

            raw_time_values: List[float] = []
            time_text_values: List[str] = []
            time_class_values: List[str] = []
            for li in time_td.select("ul.TrainingTimeDataList li"):
                li_text = _norm_text(li.get_text(" ", strip=True))
                time_text_values.append(li_text)
                time_class_values.append(" ".join(li.get("class", []) or []))
                val = _to_float(li_text)
                if pd.notna(val):
                    raw_time_values.append(float(val))

            if not raw_time_values:
                continue

            times = {
                "time_6f": np.nan,
                "time_5f": np.nan,
                "time_4f": np.nan,
                "time_3f": np.nan,
                "time_2f": np.nan,
                "time_1f": np.nan,
            }
            tail = raw_time_values[-6:]
            keys = ["time_6f", "time_5f", "time_4f", "time_3f", "time_2f", "time_1f"][-len(tail):]
            for k, v in zip(keys, tail):
                times[k] = v

            heisou_parts = [_norm_text(p.get_text(" ", strip=True)) for p in time_td.select("p.TrainingHeisou")]
            heisou_text = " ".join([x for x in heisou_parts if x])
            text_for_partner = heisou_text or row_text
            if "先着" in text_for_partner:
                partner_result = "先着"
            elif "同入" in text_for_partner or "併入" in text_for_partner:
                partner_result = "同入"
            elif "遅" in text_for_partner:
                partner_result = "遅れ"
            else:
                partner_result = ""

            if "馬也" in footwork_raw or "馬なり" in footwork_raw:
                footwork_norm = "馬なり"
            elif "強" in footwork_raw:
                footwork_norm = "強め"
            elif "一杯" in footwork_raw or "一ぱい" in footwork_raw:
                footwork_norm = "一杯"
            elif "仕掛" in footwork_raw:
                footwork_norm = "仕掛け"
            else:
                footwork_norm = footwork_raw

            movie_url = ""
            movie_a = tds[-1].find("a", href=True) if tds else None
            if movie_a is not None:
                movie_url = urljoin(source_url or "https://db.netkeiba.com/", movie_a.get("href", ""))

            raw_text = " ".join(
                x for x in [
                    date_text, course, baba, rider,
                    " ".join(str(x) for x in raw_time_values),
                    position, footwork_raw, eval_text, eval_grade, heisou_text,
                    short_comment, caption_text,
                ] if x
            )

            out_rows.append(
                {
                    "horse_id": str(horse_id),
                    "source_url": source_url,
                    "table_index": table_index,
                    "training_row_no_in_table": training_row_no,
                    "caption_text": caption_text,
                    **caption_info,
                    "race_id": race_id,
                    "race_name": race_name,
                    "race_link": race_link,
                    "race_result": race_result,
                    "race_finish_order": _parse_finish_order_from_result_text(race_result),
                    "short_comment": short_comment,
                    "training_date_text": date_text,
                    "training_date": training_date,
                    "course": course,
                    "baba": baba,
                    "rider": rider,
                    "position": position,
                    "footwork_raw": footwork_raw,
                    "footwork": footwork_norm,
                    "partner_result": partner_result,
                    "eval_text": eval_text,
                    "eval_grade": eval_grade,
                    "heisou_text": heisou_text,
                    "heisou_count": len(heisou_parts),
                    "time_values_text": "|".join(time_text_values),
                    "time_classes_text": "|".join(time_class_values),
                    "time_values_count": len(raw_time_values),
                    **times,
                    "movie_url": movie_url,
                    "raw_text": raw_text,
                }
            )

    out = pd.DataFrame(out_rows)
    if out.empty:
        return out
    out = out.drop_duplicates(subset=["horse_id", "training_date", "race_id", "raw_text"], keep="first")
    out = out.sort_values(["training_date"], ascending=False, na_position="last", kind="mergesort")
    return out.reset_index(drop=True)


def scrape_training_by_horse_id(
    horse_id: str,
    session: Optional[requests.Session] = None,
    sleep_sec: float = DEFAULT_SLEEP_SEC,
    debug_html_dir: Optional[str] = None,
    max_pages: int = 5,
) -> pd.DataFrame:
    """
    v7版：1頭分の調教データを取得する。
    1ページ目だけでなく、ページャーの次ページがあれば追跡する。
    """
    session = session or _make_session()
    horse_id = str(horse_id).strip()
    if not horse_id:
        return pd.DataFrame()

    first_url = NETKEIBA_TRAINING_URL.format(horse_id=horse_id)
    visited: set[str] = set()
    queue: List[str] = [first_url]
    all_pages: List[pd.DataFrame] = []
    last_status = "unknown"
    table_count = 0

    while queue and len(visited) < int(max_pages):
        url = queue.pop(0)
        if url in visited:
            continue
        visited.add(url)
        try:
            html = _fetch_html(session, url)
        except Exception as e:
            print(f"[WARN] 調教ページ取得失敗: horse_id={horse_id} url={url} err={e}")
            continue

        last_status = _classify_training_html(html)
        df = _parse_training_rows_from_html(html, horse_id=horse_id, source_url=url)
        if df is not None and not df.empty:
            all_pages.append(df)
            table_count += int(df.get("table_index", pd.Series(dtype=int)).nunique()) if "table_index" in df.columns else 1

        for next_url in _find_next_training_urls(html, url):
            if next_url not in visited and next_url not in queue:
                queue.append(next_url)

        if sleep_sec > 0:
            time.sleep(float(sleep_sec))

    if all_pages:
        out = pd.concat(all_pages, ignore_index=True)
        out = out.drop_duplicates(subset=["horse_id", "training_date", "race_id", "raw_text"], keep="first")
        out = out.sort_values(["training_date"], ascending=False, na_position="last", kind="mergesort")
        return out.reset_index(drop=True)

    # 取れない場合だけ旧read_htmlフォールバックを試す
    try:
        html = _fetch_html(session, first_url)
    except Exception:
        html = ""
    tables = _read_training_tables_from_html(html) if html else []
    if html:
        print(f"[WARN] 調教データを抽出できませんでした: horse_id={horse_id} status={last_status} tables={len(tables)} url={first_url}")
        _save_debug_html(html, debug_html_dir, horse_id, last_status)
    return pd.DataFrame()


def calc_training_score(training_df: pd.DataFrame, cfg: Optional[TrainingScoreConfig] = None) -> Dict[str, Any]:
    """
    v7版：自己過去比較を含む調教スコア。

    重要：
    - training_score は従来通り 0〜100
    - latest_* は一番新しい調教
    - best/avg は自身の過去調教全体
    - good_run_* は過去3着以内時の調教
    - latest_vs_* は「今回が自分の過去と比べて速い/遅い」を見る差分
      ※タイムなので、差分がマイナスなら今回の方が速い
    """
    cfg = cfg or TrainingScoreConfig()

    empty = {
        "training_score_raw": 0.0,
        "training_score": 50.0,
        "training_count": 0,
        "training_recent_count": 0,
        "training_best_1f": np.nan,
        "training_best_4f": np.nan,
        "training_last_1f": np.nan,
        "training_last_4f": np.nan,
        "training_judge": "調教データなし",
        "training_reason": "調教ページからデータを取得できませんでした",
        "self_compare_judge": "比較不可",
        "self_compare_reason": "過去調教データなし",
    }
    if training_df is None or training_df.empty:
        return empty

    work = training_df.copy()
    if "training_date" in work.columns:
        work = work.sort_values("training_date", ascending=False, na_position="last", kind="mergesort")

    if "race_finish_order" not in work.columns:
        work["race_finish_order"] = work.get("race_result", pd.Series(dtype=object)).map(_parse_finish_order_from_result_text)
    else:
        work["race_finish_order"] = pd.to_numeric(work["race_finish_order"], errors="coerce")

    recent = work.head(int(cfg.recent_n)).copy()
    good_run = work[pd.to_numeric(work["race_finish_order"], errors="coerce").between(1, 3)].copy()
    win_run = work[pd.to_numeric(work["race_finish_order"], errors="coerce").eq(1)].copy()

    row_scores: List[float] = []
    reason_list: List[str] = []
    for _, r in recent.iterrows():
        s, reasons = _score_one_training_row(r, cfg)
        row_scores.append(s)
        reason_list.extend(reasons)

    raw_score = float(np.nansum(row_scores)) if row_scores else 0.0
    training_count = int(len(work))
    recent_count = int(len(recent))

    if recent_count >= cfg.enough_count:
        raw_score += 1.0
        reason_list.append(f"調教本数十分({recent_count}本)")
    elif recent_count <= 1:
        raw_score -= 1.0
        reason_list.append(f"調教本数少なめ({recent_count}本)")

    latest = recent.iloc[0] if not recent.empty else pd.Series(dtype=object)
    latest_1f = _to_float(latest.get("time_1f"))
    latest_4f = _to_float(latest.get("time_4f"))
    latest_5f = _to_float(latest.get("time_5f"))
    best_1f = _safe_min(work, "time_1f")
    best_4f = _safe_min(work, "time_4f")
    best_5f = _safe_min(work, "time_5f")
    avg_1f = _safe_mean(work, "time_1f")
    avg_4f = _safe_mean(work, "time_4f")
    avg_5f = _safe_mean(work, "time_5f")
    recent_avg_1f = _safe_mean(recent, "time_1f")
    recent_avg_4f = _safe_mean(recent, "time_4f")
    recent_avg_5f = _safe_mean(recent, "time_5f")
    good_avg_1f = _safe_mean(good_run, "time_1f")
    good_avg_4f = _safe_mean(good_run, "time_4f")
    good_avg_5f = _safe_mean(good_run, "time_5f")
    good_best_1f = _safe_min(good_run, "time_1f")
    good_best_4f = _safe_min(good_run, "time_4f")
    good_best_5f = _safe_min(good_run, "time_5f")

    latest_vs_best_1f = latest_1f - best_1f if pd.notna(latest_1f) and pd.notna(best_1f) else np.nan
    latest_vs_best_4f = latest_4f - best_4f if pd.notna(latest_4f) and pd.notna(best_4f) else np.nan
    latest_vs_best_5f = latest_5f - best_5f if pd.notna(latest_5f) and pd.notna(best_5f) else np.nan
    latest_vs_good_avg_1f = latest_1f - good_avg_1f if pd.notna(latest_1f) and pd.notna(good_avg_1f) else np.nan
    latest_vs_good_avg_4f = latest_4f - good_avg_4f if pd.notna(latest_4f) and pd.notna(good_avg_4f) else np.nan
    latest_vs_good_avg_5f = latest_5f - good_avg_5f if pd.notna(latest_5f) and pd.notna(good_avg_5f) else np.nan

    self_reasons: List[str] = []
    if pd.notna(latest_vs_best_1f):
        if latest_vs_best_1f <= 0.2:
            raw_score += 1.0
            self_reasons.append(f"最新1Fが自己ベスト級({latest_1f:.1f}/差{latest_vs_best_1f:+.1f})")
        elif latest_vs_best_1f >= 0.8:
            raw_score -= 1.0
            self_reasons.append(f"最新1Fが自己ベストより遅い(差{latest_vs_best_1f:+.1f})")
    if pd.notna(latest_vs_good_avg_1f):
        if latest_vs_good_avg_1f <= 0:
            raw_score += 1.0
            self_reasons.append(f"3着内時平均1Fより速い(差{latest_vs_good_avg_1f:+.1f})")
        elif latest_vs_good_avg_1f >= 0.5:
            raw_score -= 1.0
            self_reasons.append(f"3着内時平均1Fより遅い(差{latest_vs_good_avg_1f:+.1f})")
    if pd.notna(latest_vs_good_avg_4f):
        if latest_vs_good_avg_4f <= -0.5:
            raw_score += 0.7
            self_reasons.append(f"3着内時平均4Fより速い(差{latest_vs_good_avg_4f:+.1f})")
        elif latest_vs_good_avg_4f >= 1.5:
            raw_score -= 0.7
            self_reasons.append(f"3着内時平均4Fより遅い(差{latest_vs_good_avg_4f:+.1f})")

    latest_grade = _norm_text(latest.get("eval_grade"))
    latest_eval = _norm_text(latest.get("eval_text"))
    latest_comment = _norm_text(latest.get("short_comment"))
    if latest_grade == "A":
        raw_score += 2.0
        self_reasons.append("最新評価A")
    elif latest_grade == "B":
        raw_score += 1.0
        self_reasons.append("最新評価B")
    elif latest_grade and latest_grade not in {"A", "B", "C"}:
        raw_score -= 0.5
        self_reasons.append(f"最新評価{latest_grade}")

    pos_words = ["万全", "文句なし", "良化", "好調", "軽快", "上々", "キビキビ", "鋭", "動き"]
    neg_words = ["平凡", "重い", "遅れ", "ひと息", "物足", "モタ", "不安"]
    latest_text_for_words = latest_eval + " " + latest_comment
    if any(w in latest_text_for_words for w in pos_words):
        raw_score += 0.8
        self_reasons.append("最新コメント前向き")
    if any(w in latest_text_for_words for w in neg_words):
        raw_score -= 0.8
        self_reasons.append("最新コメントに不安語")

    score_100 = 50.0 + raw_score * 5.0
    score_100 = float(max(0.0, min(100.0, score_100)))

    if score_100 >= 75:
        judge = "かなり良い"
    elif score_100 >= 62:
        judge = "良い"
    elif score_100 >= 45:
        judge = "普通"
    elif score_100 >= 35:
        judge = "やや不安"
    else:
        judge = "不安"

    if self_reasons:
        self_compare_judge = "自己比較あり"
        self_compare_reason = " / ".join(list(dict.fromkeys(self_reasons))[:8])
    else:
        self_compare_judge = "比較材料少なめ"
        self_compare_reason = "最新時計と好走時平均との差が明確ではありません"

    unique_reasons = list(dict.fromkeys([r for r in reason_list + self_reasons if r]))
    reason_text = " / ".join(unique_reasons[:12]) if unique_reasons else "目立つ加減点なし"

    return {
        "training_score_raw": round(raw_score, 3),
        "training_score": round(score_100, 2),
        "training_count": training_count,
        "training_recent_count": recent_count,
        "training_good_run_count": int(len(good_run)),
        "training_win_run_count": int(len(win_run)),
        "training_best_1f": _round_or_nan(best_1f),
        "training_best_4f": _round_or_nan(best_4f),
        "training_best_5f": _round_or_nan(best_5f),
        "training_avg_1f": _round_or_nan(avg_1f),
        "training_avg_4f": _round_or_nan(avg_4f),
        "training_avg_5f": _round_or_nan(avg_5f),
        "training_recent_avg_1f": _round_or_nan(recent_avg_1f),
        "training_recent_avg_4f": _round_or_nan(recent_avg_4f),
        "training_recent_avg_5f": _round_or_nan(recent_avg_5f),
        "good_run_avg_1f": _round_or_nan(good_avg_1f),
        "good_run_avg_4f": _round_or_nan(good_avg_4f),
        "good_run_avg_5f": _round_or_nan(good_avg_5f),
        "good_run_best_1f": _round_or_nan(good_best_1f),
        "good_run_best_4f": _round_or_nan(good_best_4f),
        "good_run_best_5f": _round_or_nan(good_best_5f),
        "training_last_1f": _round_or_nan(latest_1f),
        "training_last_4f": _round_or_nan(latest_4f),
        "training_last_5f": _round_or_nan(latest_5f),
        "latest_vs_best_1f": _round_or_nan(latest_vs_best_1f),
        "latest_vs_best_4f": _round_or_nan(latest_vs_best_4f),
        "latest_vs_best_5f": _round_or_nan(latest_vs_best_5f),
        "latest_vs_good_run_avg_1f": _round_or_nan(latest_vs_good_avg_1f),
        "latest_vs_good_run_avg_4f": _round_or_nan(latest_vs_good_avg_4f),
        "latest_vs_good_run_avg_5f": _round_or_nan(latest_vs_good_avg_5f),
        "latest_training_date": _safe_latest_value(recent, "training_date"),
        "latest_course": _safe_latest_value(recent, "course"),
        "latest_baba": _safe_latest_value(recent, "baba"),
        "latest_rider": _safe_latest_value(recent, "rider"),
        "latest_position": _safe_latest_value(recent, "position"),
        "latest_footwork": _safe_latest_value(recent, "footwork"),
        "latest_partner_result": _safe_latest_value(recent, "partner_result"),
        "latest_eval_text": _safe_latest_value(recent, "eval_text"),
        "latest_eval_grade": _safe_latest_value(recent, "eval_grade"),
        "latest_heisou_text": _safe_latest_value(recent, "heisou_text"),
        "latest_short_comment": _safe_latest_value(recent, "short_comment"),
        "latest_race_id": _safe_latest_value(recent, "race_id"),
        "latest_race_name": _safe_latest_value(recent, "race_name"),
        "latest_race_result": _safe_latest_value(recent, "race_result"),
        "training_judge": judge,
        "training_reason": reason_text,
        "self_compare_judge": self_compare_judge,
        "self_compare_reason": self_compare_reason,
    }


def append_training_scores_to_excel(
    src_excel_path: str,
    out_excel_path: str,
    score_df: pd.DataFrame,
    sheet_name: str = "調教スコア",
    raw_df: Optional[pd.DataFrame] = None,
    raw_sheet_name: str = "調教明細",
) -> None:
    """
    v7版：調教スコアに加えて、スクレイピングできた調教明細もExcelへ保存する。
    """
    if score_df is None:
        score_df = pd.DataFrame()
    if raw_df is None:
        raw_df = pd.DataFrame()

    src = Path(src_excel_path)
    out = Path(out_excel_path)
    out.parent.mkdir(parents=True, exist_ok=True)

    if src.resolve() != out.resolve():
        import shutil
        shutil.copy2(src, out)

    # Excelで見やすいように日時は文字列化する。
    score_out = score_df.copy()
    raw_out = raw_df.copy()
    for df in [score_out, raw_out]:
        for c in df.columns:
            if "date" in str(c).lower() or "日付" in str(c):
                try:
                    df[c] = pd.to_datetime(df[c], errors="coerce").dt.strftime("%Y-%m-%d")
                except Exception:
                    pass

    with pd.ExcelWriter(out, engine="openpyxl", mode="a", if_sheet_exists="replace") as writer:
        score_out.to_excel(writer, sheet_name=sheet_name, index=False)
        if not raw_out.empty:
            raw_out.to_excel(writer, sheet_name=raw_sheet_name, index=False)

    print(f"[INFO] 調教スコアシートを書き込みました: {out} / sheet={sheet_name}")
    if not raw_out.empty:
        print(f"[INFO] 調教明細シートを書き込みました: {out} / sheet={raw_sheet_name} rows={len(raw_out)}")




# ============================================================
# v9追加：今回調教 × 好走時調教 × 凡走時調教 × 調教師パターン
# ============================================================
# ここから下は、v8までの関数を「上書き」する追加版です。
# 既存の関数名を変えずに再定義しているため、main() からはこのv9版が使われます。


def _mode_text(s: pd.Series) -> str:
    """文字列Seriesの最頻値を返す。"""
    try:
        t = s.dropna().astype(str).map(_norm_text)
        t = t[t != ""]
        if t.empty:
            return ""
        return str(t.mode().iloc[0])
    except Exception:
        return ""


def _clip_score_0_100(v: float) -> float:
    try:
        return float(max(0.0, min(100.0, float(v))))
    except Exception:
        return 50.0


def _judge_from_score(score_100: float) -> str:
    if score_100 >= 78:
        return "かなり良い"
    if score_100 >= 65:
        return "良い"
    if score_100 >= 48:
        return "普通"
    if score_100 >= 38:
        return "やや不安"
    return "不安"


def _get_current_training_block(work: pd.DataFrame) -> pd.DataFrame:
    """
    その馬の「今回調教」とみなすブロックを取り出す。

    netkeibaの馬別調教ページは、基本的に最新レースの調教テーブルが先頭に来ます。
    そのため、table_index が最小のブロックを「今回調教」として扱います。
    pageをまたぐ場合でも、source_urlにpage=が無い先頭ページのtable_index=1を優先します。
    """
    if work is None or work.empty:
        return pd.DataFrame()
    df = work.copy()
    if "table_index" in df.columns:
        ti = pd.to_numeric(df["table_index"], errors="coerce")
        if ti.notna().any():
            min_ti = int(ti.min())
            cur = df.loc[ti == min_ti].copy()
            if not cur.empty:
                if "training_date" in cur.columns:
                    cur = cur.sort_values("training_date", ascending=False, na_position="last", kind="mergesort")
                return cur
    if "race_date_text" in df.columns:
        race_dt = pd.to_datetime(df["race_date_text"], errors="coerce")
        if race_dt.notna().any():
            latest_dt = race_dt.max()
            cur = df.loc[race_dt == latest_dt].copy()
            if "training_date" in cur.columns:
                cur = cur.sort_values("training_date", ascending=False, na_position="last", kind="mergesort")
            return cur
    if "training_date" in df.columns:
        return df.sort_values("training_date", ascending=False, na_position="last", kind="mergesort").head(5).copy()
    return df.head(5).copy()


def _exclude_current_block(work: pd.DataFrame, current: pd.DataFrame) -> pd.DataFrame:
    """過去比較用に、今回調教ブロックを除外したDataFrameを返す。"""
    if work is None or work.empty:
        return pd.DataFrame()
    if current is None or current.empty:
        return work.copy()
    df = work.copy()
    # race_id が取れる場合は、同じrace_idのブロックを除外
    cur_race_ids = set(current.get("race_id", pd.Series(dtype="object")).dropna().astype(str))
    cur_race_ids = {x for x in cur_race_ids if x and x.lower() != "nan"}
    if cur_race_ids and "race_id" in df.columns:
        return df.loc[~df["race_id"].astype(str).isin(cur_race_ids)].copy()
    # 取れない場合は table_index で除外
    if "table_index" in df.columns and "table_index" in current.columns:
        cur_ti = set(pd.to_numeric(current["table_index"], errors="coerce").dropna().astype(int).tolist())
        if cur_ti:
            return df.loc[~pd.to_numeric(df["table_index"], errors="coerce").astype("Int64").isin(cur_ti)].copy()
    return df.iloc[len(current):].copy()


def _summarize_training_block(df: pd.DataFrame, prefix: str) -> Dict[str, Any]:
    """調教ブロックの本数・時計・主コースなどを要約する。"""
    if df is None or df.empty:
        return {
            f"{prefix}_count": 0,
            f"{prefix}_course_main": "",
            f"{prefix}_avg_1f": np.nan,
            f"{prefix}_avg_4f": np.nan,
            f"{prefix}_avg_5f": np.nan,
            f"{prefix}_best_1f": np.nan,
            f"{prefix}_best_4f": np.nan,
            f"{prefix}_best_5f": np.nan,
        }
    return {
        f"{prefix}_count": int(len(df)),
        f"{prefix}_course_main": _mode_text(df["course"]) if "course" in df.columns else "",
        f"{prefix}_avg_1f": _round_or_nan(_safe_mean(df, "time_1f")),
        f"{prefix}_avg_4f": _round_or_nan(_safe_mean(df, "time_4f")),
        f"{prefix}_avg_5f": _round_or_nan(_safe_mean(df, "time_5f")),
        f"{prefix}_best_1f": _round_or_nan(_safe_min(df, "time_1f")),
        f"{prefix}_best_4f": _round_or_nan(_safe_min(df, "time_4f")),
        f"{prefix}_best_5f": _round_or_nan(_safe_min(df, "time_5f")),
    }


def _avg_training_count_per_race(df: pd.DataFrame) -> float:
    """レース単位の平均調教本数を返す。"""
    if df is None or df.empty:
        return np.nan
    if "race_id" in df.columns:
        tmp = df[df["race_id"].astype(str).str.strip() != ""].copy()
        if not tmp.empty:
            return float(tmp.groupby("race_id").size().mean())
    return float(len(df))


def _score_current_vs_history(current: pd.DataFrame, good: pd.DataFrame, bad: pd.DataFrame, cfg: TrainingScoreConfig) -> Tuple[float, List[str], Dict[str, Any]]:
    """
    今回調教を、その馬自身の好走時・凡走時と比較して加減点する。
    時計は小さいほど良いため、差分がマイナスなら良化方向です。
    """
    raw = 0.0
    reasons: List[str] = []
    cols: Dict[str, Any] = {}

    cur = _summarize_training_block(current, "current")
    good_s = _summarize_training_block(good, "good_run")
    bad_s = _summarize_training_block(bad, "bad_run")
    cols.update(cur)
    cols.update(good_s)
    cols.update(bad_s)

    # 直近ブロックそのものの点数
    row_scores: List[float] = []
    row_reasons: List[str] = []
    for _, r in current.iterrows():
        s, rs = _score_one_training_row(r, cfg)
        row_scores.append(s)
        row_reasons.extend(rs)
    if row_scores:
        raw += float(np.nansum(row_scores))
        reasons.extend(row_reasons)

    # 本数比較
    cur_count = cur.get("current_count", 0) or 0
    good_avg_count = _avg_training_count_per_race(good)
    bad_avg_count = _avg_training_count_per_race(bad)
    cols["good_run_avg_count_per_race"] = _round_or_nan(good_avg_count)
    cols["bad_run_avg_count_per_race"] = _round_or_nan(bad_avg_count)
    cols["current_vs_good_count"] = _round_or_nan(float(cur_count) - good_avg_count) if pd.notna(good_avg_count) else np.nan
    cols["current_vs_bad_count"] = _round_or_nan(float(cur_count) - bad_avg_count) if pd.notna(bad_avg_count) else np.nan

    if cur_count >= cfg.enough_count:
        raw += 0.8
        reasons.append(f"今回調教本数十分({cur_count}本)")
    elif cur_count <= 1:
        raw -= 1.0
        reasons.append(f"今回調教本数少なめ({cur_count}本)")

    if pd.notna(good_avg_count):
        if cur_count >= good_avg_count:
            raw += 0.7
            reasons.append(f"好走時平均本数以上({cur_count}本 vs {good_avg_count:.1f}本)")
        elif cur_count <= max(1.0, good_avg_count - 2.0):
            raw -= 0.7
            reasons.append(f"好走時より本数不足({cur_count}本 vs {good_avg_count:.1f}本)")

    # 時計比較 1F/4F/5F
    for f, label, good_bonus, bad_penalty in [
        ("1f", "1F", 1.6, 1.5),
        ("4f", "4F", 1.0, 1.0),
        ("5f", "5F", 0.8, 0.8),
    ]:
        cur_avg = cur.get(f"current_avg_{f}")
        good_avg = good_s.get(f"good_run_avg_{f}")
        bad_avg = bad_s.get(f"bad_run_avg_{f}")
        good_diff_col = f"current_vs_good_avg_{f}"
        bad_diff_col = f"current_vs_bad_avg_{f}"
        cols[good_diff_col] = _round_or_nan(float(cur_avg) - float(good_avg)) if pd.notna(cur_avg) and pd.notna(good_avg) else np.nan
        cols[bad_diff_col] = _round_or_nan(float(cur_avg) - float(bad_avg)) if pd.notna(cur_avg) and pd.notna(bad_avg) else np.nan

        gd = cols[good_diff_col]
        bd = cols[bad_diff_col]
        if pd.notna(gd):
            if gd <= -0.3:
                raw += good_bonus
                reasons.append(f"今回平均{label}が好走時より速い(差{gd:+.1f})")
            elif gd <= 0.2:
                raw += good_bonus * 0.6
                reasons.append(f"今回平均{label}が好走時水準(差{gd:+.1f})")
            elif gd >= 0.8:
                raw -= bad_penalty
                reasons.append(f"今回平均{label}が好走時より遅い(差{gd:+.1f})")

        if pd.notna(bd):
            if bd <= -0.3:
                raw += 0.7
                reasons.append(f"今回平均{label}が凡走時より速い(差{bd:+.1f})")
            elif bd >= 0.5:
                raw -= 0.7
                reasons.append(f"今回平均{label}が凡走時より遅い(差{bd:+.1f})")

    # コースパターン比較
    current_course = cur.get("current_course_main", "")
    good_course = good_s.get("good_run_course_main", "")
    bad_course = bad_s.get("bad_run_course_main", "")
    cols["current_course_matches_good_run"] = int(bool(current_course and good_course and current_course == good_course))
    cols["current_course_matches_bad_run"] = int(bool(current_course and bad_course and current_course == bad_course))
    if current_course and good_course and current_course == good_course:
        raw += 1.0
        reasons.append(f"今回主コースが好走時と一致({current_course})")
    if current_course and bad_course and current_course == bad_course and current_course != good_course:
        raw -= 0.6
        reasons.append(f"今回主コースが凡走時寄り({current_course})")

    # 最新評価・コメント
    latest = current.iloc[0] if current is not None and not current.empty else pd.Series(dtype="object")
    latest_grade = _norm_text(latest.get("eval_grade"))
    latest_eval = _norm_text(latest.get("eval_text"))
    latest_comment = _norm_text(latest.get("short_comment"))
    latest_heisou = _norm_text(latest.get("heisou_text"))
    cols.update({
        "latest_training_date": latest.get("training_date", np.nan),
        "latest_course": latest.get("course", ""),
        "latest_baba": latest.get("baba", ""),
        "latest_rider": latest.get("rider", ""),
        "latest_position": latest.get("position", ""),
        "latest_footwork": latest.get("footwork", ""),
        "latest_partner_result": latest.get("partner_result", ""),
        "latest_eval_text": latest_eval,
        "latest_eval_grade": latest_grade,
        "latest_heisou_text": latest_heisou,
        "latest_short_comment": latest_comment,
        "latest_race_id": latest.get("race_id", ""),
        "latest_race_name": latest.get("race_name", ""),
        "latest_race_result": latest.get("race_result", ""),
    })
    if latest_grade == "A":
        raw += 1.5
        reasons.append("最新評価A")
    elif latest_grade == "B":
        raw += 0.8
        reasons.append("最新評価B")

    pos_words = ["万全", "文句なし", "良化", "好調", "軽快", "上々", "キビキビ", "鋭", "動き", "仕上"]
    neg_words = ["平凡", "重い", "遅れ", "ひと息", "物足", "モタ", "不安"]
    word_text = latest_eval + " " + latest_comment + " " + latest_heisou
    if any(w in word_text for w in pos_words):
        raw += 0.7
        reasons.append("今回コメント前向き")
    if any(w in word_text for w in neg_words):
        raw -= 0.7
        reasons.append("今回コメントに不安語")

    return raw, reasons, cols


def _calc_trainer_pattern_for_entry(raw_df: pd.DataFrame, entry: pd.Series, current_summary: Dict[str, Any]) -> Dict[str, Any]:
    """
    調教師の好走パターンに、今回対象馬が合っているかを見る。

    現時点では「同じ実行日に取得した対象馬たちの過去調教」から算出します。
    将来的には、training_rawを蓄積したマスターCSVを使うと精度が上がります。
    """
    trainer_key = _norm_text(entry.get("trainer_key"))
    trainer_name = _norm_text(entry.get("調教師"))
    trainer_code = _norm_text(entry.get("調教師コード"))
    rid = _norm_text(entry.get("rid_str"))

    base = {
        "trainer_key": trainer_key,
        "調教師": trainer_name,
        "調教師コード": trainer_code,
        "trainer_good_run_count": 0,
        "trainer_bad_run_count": 0,
        "trainer_good_course_main": "",
        "trainer_good_avg_1f": np.nan,
        "trainer_good_avg_4f": np.nan,
        "trainer_good_avg_5f": np.nan,
        "trainer_course_match": 0,
        "trainer_pattern_score": 50.0,
        "trainer_pattern_judge": "材料不足",
        "trainer_pattern_reason": "調教師の好走調教サンプルが不足しています",
    }
    if raw_df is None or raw_df.empty or not trainer_key:
        return base
    if "trainer_key" not in raw_df.columns:
        return base

    tr = raw_df[raw_df["trainer_key"].astype(str) == trainer_key].copy()
    if tr.empty:
        return base
    # 現在対象レースの結果を使うと検証時に未来情報になるため除外する。
    if rid and "race_id" in tr.columns:
        tr = tr[tr["race_id"].astype(str) != rid].copy()
    if tr.empty:
        return base

    finish = pd.to_numeric(tr.get("race_finish_order"), errors="coerce")
    good = tr[finish.between(1, 3, inclusive="both")].copy()
    bad = tr[finish >= 6].copy()
    base["trainer_good_run_count"] = int(len(good))
    base["trainer_bad_run_count"] = int(len(bad))
    if good.empty:
        return base

    good_course = _mode_text(good["course"]) if "course" in good.columns else ""
    base["trainer_good_course_main"] = good_course
    base["trainer_good_avg_1f"] = _round_or_nan(_safe_mean(good, "time_1f"))
    base["trainer_good_avg_4f"] = _round_or_nan(_safe_mean(good, "time_4f"))
    base["trainer_good_avg_5f"] = _round_or_nan(_safe_mean(good, "time_5f"))

    raw = 0.0
    reasons: List[str] = []
    current_course = _norm_text(current_summary.get("current_course_main"))
    if current_course and good_course and current_course == good_course:
        raw += 2.0
        base["trainer_course_match"] = 1
        reasons.append(f"調教師の好走主コースと一致({current_course})")
    elif current_course and good_course:
        raw -= 0.5
        reasons.append(f"調教師好走主コースは{good_course}、今回は{current_course}")

    for f, label in [("1f", "1F"), ("4f", "4F"), ("5f", "5F")]:
        cur_avg = current_summary.get(f"current_avg_{f}")
        tr_avg = base.get(f"trainer_good_avg_{f}")
        diff_col = f"current_vs_trainer_good_avg_{f}"
        base[diff_col] = _round_or_nan(float(cur_avg) - float(tr_avg)) if pd.notna(cur_avg) and pd.notna(tr_avg) else np.nan
        d = base[diff_col]
        if pd.notna(d):
            if d <= -0.3:
                raw += 1.0
                reasons.append(f"今回平均{label}が調教師好走平均より速い(差{d:+.1f})")
            elif d <= 0.3:
                raw += 0.6
                reasons.append(f"今回平均{label}が調教師好走水準(差{d:+.1f})")
            elif d >= 0.8:
                raw -= 0.8
                reasons.append(f"今回平均{label}が調教師好走平均より遅い(差{d:+.1f})")

    score = _clip_score_0_100(50.0 + raw * 5.0)
    base["trainer_pattern_score"] = round(score, 2)
    if score >= 65:
        base["trainer_pattern_judge"] = "調教師パターン合致"
    elif score >= 48:
        base["trainer_pattern_judge"] = "普通"
    else:
        base["trainer_pattern_judge"] = "調教師パターン不一致気味"
    base["trainer_pattern_reason"] = " / ".join(list(dict.fromkeys(reasons))[:8]) if reasons else "明確な一致・不一致はありません"
    return base


def build_trainer_pattern_sheet(raw_df: pd.DataFrame) -> pd.DataFrame:
    """調教師ごとの好走調教パターンを一覧化する。"""
    if raw_df is None or raw_df.empty or "trainer_key" not in raw_df.columns:
        return pd.DataFrame()
    rows: List[Dict[str, Any]] = []
    for trainer_key, g in raw_df.groupby("trainer_key", dropna=True):
        if not str(trainer_key).strip():
            continue
        finish = pd.to_numeric(g.get("race_finish_order"), errors="coerce")
        good = g[finish.between(1, 3, inclusive="both")].copy()
        bad = g[finish >= 6].copy()
        rows.append({
            "trainer_key": trainer_key,
            "調教師": _mode_text(g["調教師"]) if "調教師" in g.columns else "",
            "調教師コード": _mode_text(g["調教師コード"]) if "調教師コード" in g.columns else "",
            "sample_training_rows": int(len(g)),
            "good_training_rows": int(len(good)),
            "bad_training_rows": int(len(bad)),
            "good_course_main": _mode_text(good["course"]) if not good.empty and "course" in good.columns else "",
            "bad_course_main": _mode_text(bad["course"]) if not bad.empty and "course" in bad.columns else "",
            "good_avg_1f": _round_or_nan(_safe_mean(good, "time_1f")),
            "good_avg_4f": _round_or_nan(_safe_mean(good, "time_4f")),
            "good_avg_5f": _round_or_nan(_safe_mean(good, "time_5f")),
            "bad_avg_1f": _round_or_nan(_safe_mean(bad, "time_1f")),
            "bad_avg_4f": _round_or_nan(_safe_mean(bad, "time_4f")),
            "bad_avg_5f": _round_or_nan(_safe_mean(bad, "time_5f")),
        })
    out = pd.DataFrame(rows)
    if not out.empty:
        out = out.sort_values(["good_training_rows", "sample_training_rows"], ascending=False, kind="mergesort")
    return out


# v9: 今走Excelから調教師情報も残すように上書き

def load_entries_from_excel(src_excel_path: str, raceday: str = "") -> pd.DataFrame:
    book = pd.read_excel(src_excel_path, sheet_name=None, engine="openpyxl")
    if NOW_SHEET not in book:
        raise RuntimeError(f"今走シートが見つかりません: {NOW_SHEET} / {src_excel_path}")

    now = book[NOW_SHEET].copy()

    if raceday:
        date_col = _pick_col(now, ["日付", "開催日", "日付(月日)", "年月日", "raceday"])
        if date_col is not None:
            s = now[date_col].astype(str).str.replace("/", "", regex=False).str.replace("-", "", regex=False)
            now = now.loc[s.str.contains(str(raceday), na=False)].copy()

    now = _ensure_rid_str(now, label="training(NOW)")
    if "rid_str" not in now.columns and "レースID" in now.columns:
        now["rid_str"] = now["レースID"]
    if "rid_str" not in now.columns:
        raise RuntimeError("今走レース情報に rid_str / レースID が見つかりません")
    now["rid_str"] = _normalize_rid_series(now["rid_str"])

    if "馬番" not in now.columns:
        c = _pick_col(now, ["馬 番", "umaban", "馬番 "])
        now["馬番"] = now[c] if c else pd.NA
    now["馬番"] = _normalize_umaban_series(now["馬番"])

    if "馬名" not in now.columns:
        c = _pick_col(now, ["horse_name", "name", "馬 名"])
        if c:
            now["馬名"] = now[c]
    if "馬名" not in now.columns:
        raise RuntimeError("今走レース情報に馬名列が見つかりません")

    horse_id_col = _pick_col(now, ["horse_id", "馬ID", "netkeiba_horse_id", "netkeiba_id"])
    if horse_id_col is not None:
        now["horse_id"] = now[horse_id_col].astype(str).str.extract(r"(\d+)", expand=False).fillna("")
    else:
        now["horse_id"] = ""

    trainer_col = _pick_col(now, ["調教師", "厩舎", "trainer", "trainer_name"])
    trainer_code_col = _pick_col(now, ["調教師コード", "厩舎コード", "trainer_code", "trainer_id"])
    now["調教師"] = now[trainer_col] if trainer_col else ""
    now["調教師コード"] = now[trainer_code_col] if trainer_code_col else ""
    now["trainer_key"] = now["調教師コード"].astype(str).map(_norm_text)
    empty_key = now["trainer_key"].astype(str).str.strip().eq("") | now["trainer_key"].astype(str).str.lower().eq("nan")
    now.loc[empty_key, "trainer_key"] = now.loc[empty_key, "調教師"].astype(str).map(_norm_text)

    keep_cols = [
        "rid_str", "馬番", "馬名", "horse_id",
        "レースID", "レース名", "場所", "頭数", "人気", "単勝オッズ", "複勝オッズ",
        "調教師", "調教師コード", "trainer_key",
    ]
    for c in keep_cols:
        if c not in now.columns:
            now[c] = pd.NA
    out = now[keep_cols].copy()
    out["馬名_norm"] = out["馬名"].map(_norm_name)
    out = out[out["rid_str"].astype(str).str.len() >= 10].copy()
    out = out[out["馬名_norm"] != ""].copy()
    return out.reset_index(drop=True)


# v9: 今回調教中心のスコアに上書き

def calc_training_score(training_df: pd.DataFrame, cfg: Optional[TrainingScoreConfig] = None) -> Dict[str, Any]:
    cfg = cfg or TrainingScoreConfig()
    if training_df is None or training_df.empty:
        return {
            "training_score_raw": 0.0,
            "training_score": 50.0,
            "current_training_score": 50.0,
            "training_count": 0,
            "training_recent_count": 0,
            "training_judge": "調教データなし",
            "training_reason": "調教ページからデータを取得できませんでした",
            "self_compare_judge": "比較不可",
            "self_compare_reason": "過去調教データがありません",
        }

    work = training_df.copy()
    if "race_finish_order" not in work.columns and "race_result" in work.columns:
        work["race_finish_order"] = work["race_result"].map(_parse_finish_order_from_result_text)
    if "training_date" in work.columns:
        work = work.sort_values("training_date", ascending=False, na_position="last", kind="mergesort")

    current = _get_current_training_block(work)
    past = _exclude_current_block(work, current)
    past_finish = pd.to_numeric(past.get("race_finish_order"), errors="coerce") if not past.empty else pd.Series(dtype="float64")
    good = past[past_finish.between(1, 3, inclusive="both")].copy() if not past.empty else pd.DataFrame()
    bad = past[past_finish >= 6].copy() if not past.empty else pd.DataFrame()

    raw_score, reasons, cols = _score_current_vs_history(current, good, bad, cfg)

    score_100 = _clip_score_0_100(50.0 + raw_score * 5.0)
    judge = _judge_from_score(score_100)
    unique_reasons = list(dict.fromkeys([r for r in reasons if r]))
    reason_text = " / ".join(unique_reasons[:14]) if unique_reasons else "目立つ加減点なし"

    if good.empty and bad.empty:
        self_judge = "過去比較材料少なめ"
        self_reason = "今回調教は取れていますが、好走時・凡走時の比較材料が少ないです"
    else:
        self_judge = "自己比較あり"
        self_reason = reason_text

    out = {
        "training_score_raw": round(raw_score, 3),
        "training_score": round(score_100, 2),
        "current_training_score": round(score_100, 2),
        "training_count": int(len(work)),
        "training_recent_count": int(len(current)),
        "training_good_run_count": int(len(good)),
        "training_bad_run_count": int(len(bad)),
        "training_win_run_count": int(len(past[past_finish == 1])) if not past.empty else 0,
        "training_best_1f": _round_or_nan(_safe_min(work, "time_1f")),
        "training_best_4f": _round_or_nan(_safe_min(work, "time_4f")),
        "training_best_5f": _round_or_nan(_safe_min(work, "time_5f")),
        "training_avg_1f": _round_or_nan(_safe_mean(work, "time_1f")),
        "training_avg_4f": _round_or_nan(_safe_mean(work, "time_4f")),
        "training_avg_5f": _round_or_nan(_safe_mean(work, "time_5f")),
        "training_last_1f": cols.get("current_avg_1f", np.nan),
        "training_last_4f": cols.get("current_avg_4f", np.nan),
        "training_last_5f": cols.get("current_avg_5f", np.nan),
        "training_judge": judge,
        "training_reason": reason_text,
        "self_compare_judge": self_judge,
        "self_compare_reason": self_reason,
    }
    out.update(cols)
    return out


# v9: 全馬取得後に、調教師パターンも反映してスコア表を完成させる

def build_training_scores_for_excel(
    src_excel_path: str,
    raceday: str = "",
    sleep_sec: float = DEFAULT_SLEEP_SEC,
    output_raw_training_csv: Optional[str] = None,
    max_horses: int = 0,
    stop_after_no_data: int = 20,
    debug_html_dir: Optional[str] = None,
    session: Optional[requests.Session] = None,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    session = session or _make_session()

    entries = load_entries_from_excel(src_excel_path, raceday=raceday)
    print(f"[INFO] 今走登録馬を読み込みました: {len(entries)}頭")

    if max_horses and int(max_horses) > 0:
        entries = entries.head(int(max_horses)).copy()
        print(f"[INFO] テスト用に取得対象を先頭 {len(entries)} 頭に制限します")

    entries = attach_horse_ids(entries, session=session, sleep_sec=sleep_sec)
    missing = entries[entries["horse_id"].astype(str).str.strip().eq("")]
    if not missing.empty:
        print(f"[WARN] horse_id を取得できない馬がいます: {len(missing)}頭")
        for _, r in missing.head(20).iterrows():
            print(f"  - race_id={r.get('rid_str')} 馬番={r.get('馬番')} 馬名={r.get('馬名')}")

    raw_list: List[pd.DataFrame] = []
    cache: Dict[str, pd.DataFrame] = {}
    horse_df_map: Dict[str, pd.DataFrame] = {}
    consecutive_no_data = 0

    for i, r in entries.iterrows():
        horse_id = str(r.get("horse_id") or "").strip()
        horse_name = str(r.get("馬名") or "").strip()
        rid = str(r.get("rid_str") or "").strip()
        umaban = r.get("馬番")
        print(f"[INFO] 調教取得: {i + 1}/{len(entries)} race_id={rid} 馬番={umaban} 馬名={horse_name} horse_id={horse_id}")

        if horse_id:
            if horse_id in cache:
                train_df = cache[horse_id]
            else:
                train_df = scrape_training_by_horse_id(
                    horse_id,
                    session=session,
                    sleep_sec=sleep_sec,
                    debug_html_dir=debug_html_dir,
                )
                cache[horse_id] = train_df
        else:
            train_df = pd.DataFrame()
        horse_df_map[horse_id] = train_df

        if not train_df.empty:
            consecutive_no_data = 0
            tmp = train_df.copy()
            tmp["rid_str"] = rid
            tmp["馬番"] = umaban
            tmp["馬名"] = horse_name
            tmp["調教師"] = r.get("調教師", "")
            tmp["調教師コード"] = r.get("調教師コード", "")
            tmp["trainer_key"] = r.get("trainer_key", "")
            raw_list.append(tmp)
        else:
            consecutive_no_data += 1
            if stop_after_no_data and int(stop_after_no_data) > 0 and consecutive_no_data >= int(stop_after_no_data):
                print(
                    f"[ERROR] 調教データなしが {consecutive_no_data} 頭連続しました。"
                    "Cookie不足・ログインページ取得・ページ構造変更の可能性が高いため停止します。"
                )
                print("[HINT] まず --max-horses 5 --debug-html-dir を付けてHTMLの中身を確認してください。")
                break

    raw_df = pd.concat(raw_list, ignore_index=True) if raw_list else pd.DataFrame()

    score_rows: List[Dict[str, Any]] = []
    for _, r in entries.iterrows():
        horse_id = str(r.get("horse_id") or "").strip()
        train_df = horse_df_map.get(horse_id, pd.DataFrame())
        score_info = calc_training_score(train_df)
        trainer_info = _calc_trainer_pattern_for_entry(raw_df, r, score_info)
        # まずは調教師パターンを「列として見える化」。加点は控えめに反映。
        base_score = float(score_info.get("training_score", 50.0) or 50.0)
        trainer_score = float(trainer_info.get("trainer_pattern_score", 50.0) or 50.0)
        final_score = _clip_score_0_100(base_score + (trainer_score - 50.0) * 0.20)
        final_reason = _norm_text(score_info.get("training_reason", ""))
        if trainer_info.get("trainer_pattern_judge") not in ["材料不足", ""]:
            final_reason = (final_reason + " / " if final_reason else "") + "調教師傾向:" + _norm_text(trainer_info.get("trainer_pattern_reason", ""))

        score_rows.append({
            "rid_str": str(r.get("rid_str") or "").strip(),
            "馬番": r.get("馬番"),
            "馬名": str(r.get("馬名") or "").strip(),
            "horse_id": horse_id,
            "training_url": NETKEIBA_TRAINING_URL.format(horse_id=horse_id) if horse_id else "",
            **score_info,
            **trainer_info,
            "training_score_before_trainer": round(base_score, 2),
            "training_score_final": round(final_score, 2),
            "training_judge_final": _judge_from_score(final_score),
            "training_reason_final": final_reason,
        })

    score_df = pd.DataFrame(score_rows)
    if not score_df.empty:
        score_df["rid_str"] = _normalize_rid_series(score_df["rid_str"])
        score_df["馬番"] = _normalize_umaban_series(score_df["馬番"])
        score_df = score_df.sort_values(["rid_str", "馬番"], kind="mergesort").reset_index(drop=True)

    if output_raw_training_csv and not raw_df.empty:
        Path(output_raw_training_csv).parent.mkdir(parents=True, exist_ok=True)
        raw_df.to_csv(output_raw_training_csv, index=False, encoding="utf-8-sig")
        print(f"[INFO] 調教明細CSVを保存しました: {output_raw_training_csv}")

    return score_df, raw_df


# v9: 調教師傾向シートも出す

def append_training_scores_to_excel(
    src_excel_path: str,
    out_excel_path: str,
    score_df: pd.DataFrame,
    sheet_name: str = "調教スコア",
    raw_df: Optional[pd.DataFrame] = None,
    raw_sheet_name: str = "調教明細",
) -> None:
    if score_df is None:
        score_df = pd.DataFrame()
    if raw_df is None:
        raw_df = pd.DataFrame()

    src = Path(src_excel_path)
    out = Path(out_excel_path)
    out.parent.mkdir(parents=True, exist_ok=True)

    if src.resolve() != out.resolve():
        import shutil
        shutil.copy2(src, out)

    score_out = score_df.copy()
    raw_out = raw_df.copy()
    trainer_out = build_trainer_pattern_sheet(raw_out)

    for df in [score_out, raw_out, trainer_out]:
        for c in df.columns:
            if "date" in str(c).lower() or "日付" in str(c):
                try:
                    df[c] = pd.to_datetime(df[c], errors="coerce").dt.strftime("%Y-%m-%d")
                except Exception:
                    pass

    with pd.ExcelWriter(out, engine="openpyxl", mode="a", if_sheet_exists="replace") as writer:
        score_out.to_excel(writer, sheet_name=sheet_name, index=False)
        if not raw_out.empty:
            raw_out.to_excel(writer, sheet_name=raw_sheet_name, index=False)
        if not trainer_out.empty:
            trainer_out.to_excel(writer, sheet_name="調教師傾向", index=False)

    print(f"[INFO] 調教スコアシートを書き込みました: {out} / sheet={sheet_name}")
    if not raw_out.empty:
        print(f"[INFO] 調教明細シートを書き込みました: {out} / sheet={raw_sheet_name} rows={len(raw_out)}")
    if not trainer_out.empty:
        print(f"[INFO] 調教師傾向シートを書き込みました: {out} / sheet=調教師傾向 rows={len(trainer_out)}")

# ============================================================
# CLI
# ============================================================
def main() -> None:
    parser = argparse.ArgumentParser(description="netkeiba調教データを取得し、対象レース登録馬ごとに調教スコアを付ける")
    parser.add_argument("--src", required=True, help="入力Excelパス。例: data/input/馬の競走成績_20260602.xlsx")
    parser.add_argument("--out", default="", help="出力Excelパス。未指定なら入力Excel名_training.xlsx")
    parser.add_argument("--raceday", default="", help="対象日 YYYYMMDD。今走シートに日付列がある場合だけ絞り込み")
    parser.add_argument("--sleep", type=float, default=DEFAULT_SLEEP_SEC, help="アクセス間隔秒。既定=1.0")
    parser.add_argument("--raw-csv", default="", help="調教明細CSVの保存先。未指定なら保存しない")
    parser.add_argument("--max-horses", type=int, default=0, help="テスト用。先頭N頭だけ取得する。0なら全頭")
    parser.add_argument("--stop-after-no-data", type=int, default=20, help="調教データなしがN頭連続したら停止。0なら停止しない")
    parser.add_argument("--debug-html-dir", default="", help="抽出失敗時のHTML保存先。原因確認用")
    parser.add_argument("--selenium-login", action="store_true", help="Seleniumブラウザでログインし、そのCookieをrequestsへ流用する")
    parser.add_argument("--selenium-login-url", default="https://db.netkeiba.com/horse/training.html?id=2022105396", help="Seleniumログイン確認で最初に開くURL")
    parser.add_argument("--selenium-wait-seconds", type=int, default=180, help="Seleniumログイン操作の待機目安秒")
    parser.add_argument("--auto-login", action="store_true", help="SeleniumでnetkeibaへID/PW自動ログインしてCookieをrequestsへ流用する")
    parser.add_argument("--credentials-ini", default="", help="netkeibaログイン情報を書いたcredentials.ini。未指定なら自動探索")
    parser.add_argument("--netkeiba-login-url", default="https://regist.netkeiba.com/account/?pid=login", help="netkeibaログインページURL")
    parser.add_argument("--keep-browser-open", action="store_true", help="デバッグ用。Seleniumブラウザを閉じずに残す")
    args = parser.parse_args()

    src = Path(args.src)
    if not src.exists():
        raise FileNotFoundError(f"入力Excelが見つかりません: {src}")

    if args.out:
        out = Path(args.out)
    else:
        out = src.with_name(src.stem + "_training.xlsx")

    session = _make_session_with_optional_browser_login(
        use_selenium_login=bool(args.selenium_login or args.auto_login),
        selenium_login_url=args.selenium_login_url,
        selenium_wait_seconds=args.selenium_wait_seconds,
        auto_login=bool(args.auto_login),
        credentials_ini=args.credentials_ini,
        netkeiba_login_url=args.netkeiba_login_url,
        keep_browser_open=bool(args.keep_browser_open),
    )

    score_df, raw_df = build_training_scores_for_excel(
        src_excel_path=str(src),
        raceday=args.raceday,
        sleep_sec=args.sleep,
        output_raw_training_csv=args.raw_csv or None,
        max_horses=args.max_horses,
        stop_after_no_data=args.stop_after_no_data,
        debug_html_dir=args.debug_html_dir or None,
        session=session,
    )

    append_training_scores_to_excel(
        src_excel_path=str(src),
        out_excel_path=str(out),
        score_df=score_df,
        sheet_name="調教スコア",
        raw_df=raw_df,
        raw_sheet_name="調教明細",
    )

    print("[INFO] 完了")
    print(f"[INFO] 調教スコア件数: {len(score_df)}")
    print(f"[INFO] 調教明細件数: {len(raw_df)}")
    print(f"[INFO] 出力Excel: {out}")


if __name__ == "__main__":
    main()

# ============================================================
# v10追加：YouTube調教理論反映
# - 栗東坂路 4F 53.9秒以下
# - 坂路加速ラップ A1〜A3 相当
# - 一杯で出した好時計は過信しない
# - 併せ遅れ・同入は割引
# - 先着・単走は加点
# ============================================================
# ここから下は v9 の関数を上書き・拡張します。
# main() はファイル末尾で呼ばれるため、この再定義版が使われます。


def _v10_to_float(x: Any) -> float:
    try:
        if x is None or pd.isna(x):
            return float("nan")
        s = str(x).strip().replace(",", "")
        if s in ["", "-", "nan", "None", "<NA>"]:
            return float("nan")
        return float(s)
    except Exception:
        return float("nan")


def _v10_norm_course(course: Any) -> str:
    s = _norm_text(course)
    s = s.replace("栗東", "栗").replace("美浦", "美")
    s = s.replace("坂路", "坂")
    s = s.replace("ＣＷ", "CW").replace("Ｗ", "W")
    return s


def _v10_is_ritto_slope(course: Any) -> bool:
    s = _v10_norm_course(course)
    return ("栗" in s) and ("坂" in s)


def _v10_is_slope(course: Any) -> bool:
    s = _v10_norm_course(course)
    return "坂" in s


def _v10_contains_any(text: Any, words: List[str]) -> bool:
    s = _norm_text(text)
    return any(w in s for w in words)


def _v10_sectional_laps(row: pd.Series) -> Dict[str, Any]:
    """
    累計時計から坂路/ウッドの区間ラップを作る。

    time_4f=53.8, time_3f=38.9, time_2f=24.9, time_1f=12.1 の場合：
      lap_4to3 = 14.9
      lap_3to2 = 14.0
      lap_2to1 = 12.8
      lap_1f   = 12.1
    """
    t4 = _v10_to_float(row.get("time_4f"))
    t3 = _v10_to_float(row.get("time_3f"))
    t2 = _v10_to_float(row.get("time_2f"))
    t1 = _v10_to_float(row.get("time_1f"))

    lap_4to3 = t4 - t3 if pd.notna(t4) and pd.notna(t3) else np.nan
    lap_3to2 = t3 - t2 if pd.notna(t3) and pd.notna(t2) else np.nan
    lap_2to1 = t2 - t1 if pd.notna(t2) and pd.notna(t1) else np.nan
    lap_1f = t1 if pd.notna(t1) else np.nan

    return {
        "lap_4to3": _round_or_nan(lap_4to3),
        "lap_3to2": _round_or_nan(lap_3to2),
        "lap_2to1": _round_or_nan(lap_2to1),
        "lap_1f": _round_or_nan(lap_1f),
    }


def _v10_accel_lap_type(row: pd.Series) -> str:
    """
    加速ラップA1〜A3の簡易判定。

    厳密な竹内式の完全再現ではなく、スクレイピング値だけで使える近似です。

    A1：4F区間→3F区間→2F区間→1Fがほぼ連続加速
    A2：3F区間→2F区間→1Fがほぼ連続加速、かつ終い12秒台前半〜中盤
    A3：2F区間→1Fで明確に加速、かつ終い12秒台前半〜中盤
    """
    laps = _v10_sectional_laps(row)
    l43 = _v10_to_float(laps.get("lap_4to3"))
    l32 = _v10_to_float(laps.get("lap_3to2"))
    l21 = _v10_to_float(laps.get("lap_2to1"))
    l1 = _v10_to_float(laps.get("lap_1f"))

    vals = [l43, l32, l21, l1]
    if any(pd.isna(v) for v in vals):
        return ""

    # 時計は小さいほど速い。前区間 >= 後区間 なら加速方向。
    tol = 0.15
    if (l43 + tol >= l32) and (l32 + tol >= l21) and (l21 + tol >= l1):
        return "A1"
    if (l32 + tol >= l21) and (l21 + tol >= l1) and l1 <= 12.6:
        return "A2"
    if (l21 + tol >= l1) and l1 <= 12.4:
        return "A3"
    return ""


def _v10_row_flags(row: pd.Series) -> Dict[str, Any]:
    course = row.get("course", "")
    t4 = _v10_to_float(row.get("time_4f"))
    t1 = _v10_to_float(row.get("time_1f"))
    footwork = _norm_text(row.get("footwork")) or _norm_text(row.get("footwork_raw"))
    heisou = _norm_text(row.get("heisou_text"))
    partner = _norm_text(row.get("partner_result"))

    is_ritto_slope = _v10_is_ritto_slope(course)
    is_slope = _v10_is_slope(course)
    accel_type = _v10_accel_lap_type(row) if is_slope else ""
    slope_4f_539 = int(bool(is_ritto_slope and pd.notna(t4) and t4 <= 53.9))
    slope_accel = int(bool(is_slope and accel_type in {"A1", "A2", "A3"}))

    hard_work = int(_v10_contains_any(footwork, ["一杯", "Ｇ一", "G一", "強一", "叩一"]))
    # 強め・末強め・仕掛けは残す。馬なりはもちろん残す。
    soft_or_ok_work = int(bool(footwork and not hard_work))

    heisou_delay = int("遅れ" in (partner + heisou))
    heisou_same = int(("同入" in (partner + heisou)) or ("併入" in (partner + heisou)))
    heisou_win = int("先着" in (partner + heisou))
    solo = int(not heisou and not partner)
    heisou_good = int(bool(heisou_win or solo))

    video_base = int(bool(slope_4f_539 and slope_accel))
    video_clean = int(bool(video_base and not hard_work and not heisou_delay and not heisou_same))
    video_good = int(bool(video_clean and heisou_good))

    laps = _v10_sectional_laps(row)
    return {
        **laps,
        "accel_lap_type": accel_type,
        "slope_accel_lap_flag": slope_accel,
        "ritto_slope_4f_539_flag": slope_4f_539,
        "hard_work_risk_flag": hard_work,
        "soft_or_ok_work_flag": soft_or_ok_work,
        "heisou_delay_risk_flag": heisou_delay,
        "heisou_same_risk_flag": heisou_same,
        "heisou_win_flag": heisou_win,
        "solo_training_flag": solo,
        "heisou_good_flag": heisou_good,
        "youtube_video_base_flag": video_base,
        "youtube_video_clean_flag": video_clean,
        "youtube_video_good_flag": video_good,
        "last_1f_12_6_or_less_flag": int(bool(pd.notna(t1) and t1 <= 12.6)),
        "last_1f_12_4_or_less_flag": int(bool(pd.notna(t1) and t1 <= 12.4)),
    }


def _v10_enrich_video_training_flags(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    out = df.copy()
    rows = []
    for _, r in out.iterrows():
        rows.append(_v10_row_flags(r))
    flag_df = pd.DataFrame(rows, index=out.index)
    for c in flag_df.columns:
        out[c] = flag_df[c]
    return out


def _v10_score_youtube_conditions(current: pd.DataFrame) -> Tuple[float, List[str], Dict[str, Any]]:
    """今回調教ブロックに、動画で学んだ調教条件を反映する。"""
    raw = 0.0
    reasons: List[str] = []
    cols: Dict[str, Any] = {}

    if current is None or current.empty:
        cols.update({
            "current_ritto_slope_4f_539_count": 0,
            "current_slope_accel_lap_count": 0,
            "current_youtube_video_base_count": 0,
            "current_youtube_video_clean_count": 0,
            "current_youtube_video_good_count": 0,
            "current_hard_work_risk_count": 0,
            "current_heisou_delay_risk_count": 0,
            "current_heisou_same_risk_count": 0,
            "current_heisou_good_count": 0,
            "current_best_accel_lap_type": "",
            "youtube_training_pattern_score": 50.0,
            "youtube_training_pattern_judge": "材料不足",
            "youtube_training_pattern_reason": "今回調教データがありません",
        })
        return raw, reasons, cols

    work = _v10_enrich_video_training_flags(current)

    def _sum_flag(col: str) -> int:
        return int(pd.to_numeric(work.get(col, 0), errors="coerce").fillna(0).sum()) if col in work.columns else 0

    ritto_539_count = _sum_flag("ritto_slope_4f_539_flag")
    accel_count = _sum_flag("slope_accel_lap_flag")
    video_base_count = _sum_flag("youtube_video_base_flag")
    video_clean_count = _sum_flag("youtube_video_clean_flag")
    video_good_count = _sum_flag("youtube_video_good_flag")
    hard_count = _sum_flag("hard_work_risk_flag")
    delay_count = _sum_flag("heisou_delay_risk_flag")
    same_count = _sum_flag("heisou_same_risk_flag")
    heisou_good_count = _sum_flag("heisou_good_flag")

    accel_types = work.get("accel_lap_type", pd.Series(dtype="object")).dropna().astype(str)
    accel_types = accel_types[accel_types != ""]
    best_accel = ""
    for k in ["A1", "A2", "A3"]:
        if (accel_types == k).any():
            best_accel = k
            break

    # 動画理論のコア条件：栗坂4F53.9以下 + 加速ラップ
    if video_base_count > 0:
        raw += 2.2
        reasons.append(f"栗坂4F53.9秒以下＋加速ラップ該当({video_base_count}本)")
        if best_accel:
            reasons.append(f"坂路加速ラップ{best_accel}相当")
    elif ritto_539_count > 0:
        raw += 0.8
        reasons.append(f"栗坂4F53.9秒以下はあるが加速条件は弱め({ritto_539_count}本)")
    elif accel_count > 0:
        raw += 0.5
        reasons.append(f"坂路加速ラップあり({accel_count}本)")

    # 一杯・併せ内容で危険人気馬を削る考え方
    if video_good_count > 0:
        raw += 1.4
        reasons.append(f"動画条件の良形: 一杯ではなく先着/単走({video_good_count}本)")
    elif video_clean_count > 0:
        raw += 0.8
        reasons.append(f"動画条件の準良形: 一杯・遅れ・同入なし({video_clean_count}本)")

    if hard_count > 0 and video_base_count > 0:
        raw -= 1.4
        reasons.append(f"好時計でも一杯追いあり({hard_count}本)で過信注意")
    elif hard_count > 0:
        raw -= 0.7
        reasons.append(f"一杯追いあり({hard_count}本)")

    if delay_count > 0:
        raw -= 1.2
        reasons.append(f"併せ遅れあり({delay_count}本)")
    if same_count > 0 and video_base_count > 0:
        raw -= 0.4
        reasons.append(f"好時計でも併入/同入あり({same_count}本)で評価控えめ")
    if heisou_good_count > 0 and delay_count == 0:
        raw += 0.5
        reasons.append(f"先着または単走が多い({heisou_good_count}本)")

    pattern_score = _clip_score_0_100(50.0 + raw * 8.0)
    if pattern_score >= 78:
        pattern_judge = "動画条件かなり合致"
    elif pattern_score >= 65:
        pattern_judge = "動画条件やや合致"
    elif pattern_score >= 48:
        pattern_judge = "普通"
    elif pattern_score >= 38:
        pattern_judge = "動画条件やや不一致"
    else:
        pattern_judge = "動画条件不安"

    cols.update({
        "current_ritto_slope_4f_539_count": ritto_539_count,
        "current_slope_accel_lap_count": accel_count,
        "current_youtube_video_base_count": video_base_count,
        "current_youtube_video_clean_count": video_clean_count,
        "current_youtube_video_good_count": video_good_count,
        "current_hard_work_risk_count": hard_count,
        "current_heisou_delay_risk_count": delay_count,
        "current_heisou_same_risk_count": same_count,
        "current_heisou_good_count": heisou_good_count,
        "current_best_accel_lap_type": best_accel,
        "youtube_training_pattern_score": round(pattern_score, 2),
        "youtube_training_pattern_judge": pattern_judge,
        "youtube_training_pattern_reason": " / ".join(list(dict.fromkeys(reasons))[:10]) if reasons else "動画条件での明確な加減点なし",
    })
    return raw, reasons, cols


# v9の自己比較スコア関数を退避して、v10で追加加点する
_v9_score_current_vs_history = _score_current_vs_history


def _score_current_vs_history(current: pd.DataFrame, good: pd.DataFrame, bad: pd.DataFrame, cfg: TrainingScoreConfig) -> Tuple[float, List[str], Dict[str, Any]]:
    raw, reasons, cols = _v9_score_current_vs_history(current, good, bad, cfg)
    yt_raw, yt_reasons, yt_cols = _v10_score_youtube_conditions(current)
    raw += yt_raw
    if yt_reasons:
        reasons.extend(yt_reasons)
    cols.update(yt_cols)
    return raw, reasons, cols


# raw_dfにも動画系フラグを出したいので、v9のbuildをラップしてCSVを書き直す
_v9_build_training_scores_for_excel = build_training_scores_for_excel


def build_training_scores_for_excel(
    src_excel_path: str,
    raceday: str = "",
    sleep_sec: float = DEFAULT_SLEEP_SEC,
    output_raw_training_csv: Optional[str] = None,
    max_horses: int = 0,
    stop_after_no_data: int = 20,
    debug_html_dir: Optional[str] = None,
    session: Optional[requests.Session] = None,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    score_df, raw_df = _v9_build_training_scores_for_excel(
        src_excel_path=src_excel_path,
        raceday=raceday,
        sleep_sec=sleep_sec,
        output_raw_training_csv=output_raw_training_csv,
        max_horses=max_horses,
        stop_after_no_data=stop_after_no_data,
        debug_html_dir=debug_html_dir,
        session=session,
    )

    if raw_df is not None and not raw_df.empty:
        raw_df = _v10_enrich_video_training_flags(raw_df)
        if output_raw_training_csv:
            Path(output_raw_training_csv).parent.mkdir(parents=True, exist_ok=True)
            raw_df.to_csv(output_raw_training_csv, index=False, encoding="utf-8-sig")
            print(f"[INFO] v10動画系フラグ付き調教明細CSVを保存しました: {output_raw_training_csv}")

    # 見やすいように、training_reason_finalにも動画条件の理由を明示的に追記
    if score_df is not None and not score_df.empty:
        if "youtube_training_pattern_reason" in score_df.columns:
            def _append_video_reason(row: pd.Series) -> str:
                base = _norm_text(row.get("training_reason_final", ""))
                yt = _norm_text(row.get("youtube_training_pattern_reason", ""))
                if yt and yt != "動画条件での明確な加減点なし" and "動画条件:" not in base:
                    return (base + " / " if base else "") + "動画条件:" + yt
                return base
            score_df["training_reason_final"] = score_df.apply(_append_video_reason, axis=1)

    return score_df, raw_df

