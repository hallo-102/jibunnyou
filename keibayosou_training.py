# -*- coding: utf-8 -*-
"""
keibayosou_training.py

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
import os
import re
import time
import unicodedata
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import urljoin

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


def _fetch_html(session: requests.Session, url: str, timeout: int = DEFAULT_TIMEOUT) -> str:
    res = session.get(url, timeout=timeout)
    res.raise_for_status()

    # netkeiba は EUC-JP / CP932 系になることがあるため apparent_encoding を優先
    if not res.encoding or res.encoding.lower() in {"iso-8859-1", "ascii"}:
        res.encoding = res.apparent_encoding or "utf-8"
    return res.text


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
        print(f"[WARN] 調教データを抽出できませんでした: horse_id={horse_id} url={url}")
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
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    既存の今走Excelから対象馬を読み、全馬に調教スコアを付ける。

    戻り値:
    - score_df: 1行=今走出走馬。pipelineにmergeしやすい調教スコア表。
    - raw_df:   1行=調教1本。確認・デバッグ用。
    """
    session = _make_session()

    entries = load_entries_from_excel(src_excel_path, raceday=raceday)
    print(f"[INFO] 今走登録馬を読み込みました: {len(entries)}頭")

    entries = attach_horse_ids(entries, session=session, sleep_sec=sleep_sec)
    missing = entries[entries["horse_id"].astype(str).str.strip().eq("")]
    if not missing.empty:
        print(f"[WARN] horse_id を取得できない馬がいます: {len(missing)}頭")
        for _, r in missing.head(20).iterrows():
            print(f"  - race_id={r.get('rid_str')} 馬番={r.get('馬番')} 馬名={r.get('馬名')}")

    raw_list: List[pd.DataFrame] = []
    cache: Dict[str, pd.DataFrame] = {}
    score_rows: List[Dict[str, Any]] = []

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
                train_df = scrape_training_by_horse_id(horse_id, session=session, sleep_sec=sleep_sec)
                cache[horse_id] = train_df
        else:
            train_df = pd.DataFrame()

        if not train_df.empty:
            tmp = train_df.copy()
            tmp["rid_str"] = rid
            tmp["馬番"] = umaban
            tmp["馬名"] = horse_name
            raw_list.append(tmp)

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
# CLI
# ============================================================
def main() -> None:
    parser = argparse.ArgumentParser(description="netkeiba調教データを取得し、対象レース登録馬ごとに調教スコアを付ける")
    parser.add_argument("--src", required=True, help="入力Excelパス。例: data/input/馬の競走成績_20260602.xlsx")
    parser.add_argument("--out", default="", help="出力Excelパス。未指定なら入力Excel名_training.xlsx")
    parser.add_argument("--raceday", default="", help="対象日 YYYYMMDD。今走シートに日付列がある場合だけ絞り込み")
    parser.add_argument("--sleep", type=float, default=DEFAULT_SLEEP_SEC, help="アクセス間隔秒。既定=1.0")
    parser.add_argument("--raw-csv", default="", help="調教明細CSVの保存先。未指定なら保存しない")
    args = parser.parse_args()

    src = Path(args.src)
    if not src.exists():
        raise FileNotFoundError(f"入力Excelが見つかりません: {src}")

    if args.out:
        out = Path(args.out)
    else:
        out = src.with_name(src.stem + "_training.xlsx")

    score_df, raw_df = build_training_scores_for_excel(
        src_excel_path=str(src),
        raceday=args.raceday,
        sleep_sec=args.sleep,
        output_raw_training_csv=args.raw_csv or None,
    )

    append_training_scores_to_excel(
        src_excel_path=str(src),
        out_excel_path=str(out),
        score_df=score_df,
        sheet_name="調教スコア",
    )

    print("[INFO] 完了")
    print(f"[INFO] 調教スコア件数: {len(score_df)}")
    print(f"[INFO] 調教明細件数: {len(raw_df)}")
    print(f"[INFO] 出力Excel: {out}")


if __name__ == "__main__":
    main()
