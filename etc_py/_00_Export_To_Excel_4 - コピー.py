# -*- coding: utf-8 -*-
# racedata_results.xlsx → race_levels.xlsx 生成ツール（完全版）
# 対応済み：
#  (1) 分数着差パース修正（"1 1/2" / "1.1/2" / "1/2" / 全角スラッシュ等）
#  (2) YYYYMMDDシートを日付昇順ソートして処理
#  (3) race_levels に統計列（std/IQR/gapなど）追加
#  (4) start_time（発走時刻）を抽出して races / race_levels に追加
#  (5) place（開催場）を正しく抽出（"09:50発走" を place にしない）
#  (6) PermissionError対策：Excelがロックされていても一時コピーで読み込む

from __future__ import annotations

import argparse
import datetime
import re
import shutil
import tempfile
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd

pd.set_option("future.no_silent_downcasting", True)

BASE_DIR = Path("C:/Users/okino/OneDrive/ドキュメント/my_python_cursor/keiba_yosou_2026/data/master")
PLACE_BABA_TIME_PATH = BASE_DIR / "場所_馬場_タイム.xlsx"

# ========= 0) Excelを安全に開く（PermissionError対策） =======================

def open_excelfile_safely(
    xlsx_path: Path,
    retries: int = 3,
    wait_sec: float = 0.8
) -> Tuple[pd.ExcelFile, Optional[Path]]:
    """
    OneDrive/Excel/ウイルス対策などでファイルがロックされていると PermissionError になることがある。
    その場合、テンポラリにコピーして読むことで回避する。

    戻り値：
      (xls, temp_copy_path)
      temp_copy_path が None でなければ、一時コピーを使っているので最後に削除する。
    """
    xlsx_path = Path(xlsx_path)

    last_err = None
    for _ in range(retries):
        try:
            xls = pd.ExcelFile(xlsx_path, engine="openpyxl")
            return xls, None
        except PermissionError as e:
            last_err = e
            time.sleep(wait_sec)

    tmp_dir = Path(tempfile.gettempdir())
    tmp_copy = tmp_dir / f"{xlsx_path.stem}__tmp_read__{int(time.time())}{xlsx_path.suffix}"
    try:
        shutil.copy2(xlsx_path, tmp_copy)
        xls = pd.ExcelFile(tmp_copy, engine="openpyxl")
        print(f"[warn] 入力Excelがロック中のため、一時コピーで読み込みます: {tmp_copy}")
        return xls, tmp_copy
    except Exception as e:
        print(f"[!] Excelを開けません（ロック/権限）。元ファイル: {xlsx_path}")
        print("    対処：Excelを閉じる/OneDrive同期を待つ/別フォルダへコピーして再実行してください。")
        raise e from last_err


def load_place_baba_time_master(
    xlsx_path: Path,
) -> Dict[Tuple[str, str, str, str], float]:
    """
    場所_馬場_タイム.xlsx を読み込み、
    (場所, クラス, 馬場, コース) -> タイム秒 の辞書へ変換する。
    """
    if not xlsx_path.exists():
        print(f"[warn] 条件別タイム基準ファイルが見つかりません: {xlsx_path}")
        return {}

    xls, temp_copy = open_excelfile_safely(xlsx_path)
    try:
        df = pd.read_excel(xls, sheet_name=0)
    finally:
        try:
            xls.close()
        except Exception:
            pass
        if temp_copy is not None:
            try:
                Path(temp_copy).unlink(missing_ok=True)
            except Exception:
                pass

    required = ["場所", "クラス", "馬場", "コース", "タイム"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        print(f"[warn] 条件別タイム基準ファイルの必須列が不足しています: {missing}")
        return {}

    baseline_map: Dict[Tuple[str, str, str, str], float] = {}
    for _, row in df.iterrows():
        place = str(row["場所"]).strip() if pd.notna(row["場所"]) else ""
        race_class = str(row["クラス"]).strip() if pd.notna(row["クラス"]) else ""
        baba = str(row["馬場"]).strip() if pd.notna(row["馬場"]) else ""
        course = str(row["コース"]).strip() if pd.notna(row["コース"]) else ""
        time_val = row["タイム"]
        if not place or not race_class or not baba or not course or pd.isna(time_val):
            continue
        try:
            baseline_map[(place, race_class, baba, course)] = float(time_val)
        except Exception:
            continue

    print(f"[info] 条件別タイム基準を読込: {xlsx_path} / {len(baseline_map)} 件")
    return baseline_map


# ========= 1) 列名の名寄せ（日本語カラム → 統一キー） ========================

COLMAP_HARDCODE = {
    "レースID": "race_id",
    "ﾚｰｽID": "race_id",
    "レースId": "race_id",
    "レースＩＤ": "race_id",
    "レースＩｄ": "race_id",

    "着 順": "rank",
    "順位": "rank",
    "着順": "rank",

    "枠": "frame",
    "馬 番": "number",
    "馬番": "number",

    "馬名": "horse_name",
    "性齢": "sex_age",
    "斤量": "weight",
    "騎手": "jockey",

    "タイム": "time_str",
    "着差": "margin_str",
    "通過": "passing",
    "コーナー 通過順": "passing",

    "人 気": "pop",
    "人気": "pop",
    "単勝 オッズ": "odds",
    "単勝オッズ": "odds",

    "後3F": "last3f",
    "上がり": "last3f",

    "厩舎": "trainer",
    "馬体重 (増減)": "weight_body",

    "レース名": "race_name",
    "レース情報": "race_info",
}

# MultiIndex 風の列名（('騎手', '騎手') など）が文字列として入ることがあるので吸収
_ALIAS_LOOKUP = {
    "('騎手','騎手')": "jockey",
    "('単勝','オッズ')": "odds",
    "('単勝','人気')": "pop",
    "('着順','着 順')": "rank",
    "('着順','順位')": "rank",
    "('レース情報','レース情報')": "race_info",
    "('レース名','レース名')": "race_name",
}


def _normalize_key(s: str) -> str:
    t = str(s)
    t = t.replace(" ", "").replace("　", "")
    t = t.replace("\n", "").replace("\r", "")
    return t


def _merge_duplicate_columns(df: pd.DataFrame, name: str) -> pd.DataFrame:
    """
    同名列が複数ある場合に 1 列へ縮約する（bfillで左優先）
    """
    cols = [c for c in df.columns if c == name]
    if len(cols) >= 2:
        obj_df = df[cols].apply(lambda s: s.astype("object"), axis=0)
        combined = obj_df.bfill(axis=1).infer_objects(copy=False).iloc[:, 0]
        df.drop(columns=list(set(cols)), inplace=True, errors="ignore")
        df[name] = combined
    return df


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    new_cols = {}
    for c in df.columns:
        c_str = str(c)
        if c_str in COLMAP_HARDCODE:
            new_cols[c] = COLMAP_HARDCODE[c_str]
            continue

        key = _normalize_key(c_str)
        if key in _ALIAS_LOOKUP:
            new_cols[c] = _ALIAS_LOOKUP[key]
            continue

        m = re.match(r"^\(\s*'([^']+)'\s*,\s*'([^']+)'\s*\)$", c_str)
        if m:
            inner = f"('{m.group(1)}', '{m.group(2)}')"
            nk = _normalize_key(inner)
            if nk in _ALIAS_LOOKUP:
                new_cols[c] = _ALIAS_LOOKUP[nk]
                continue

        new_cols[c] = c

    df = df.rename(columns=new_cols)

    for key in ["race_id", "rank", "frame", "number", "horse_name", "jockey",
                "odds", "pop", "race_info", "race_name"]:
        df = _merge_duplicate_columns(df, key)

    return df


# ========= 2) タイム・着差文字列のパース =====================================

def parse_time_str(s: str) -> Optional[float]:
    """
    '1:34.5' → 94.5秒
    """
    if not isinstance(s, str):
        return None
    s = s.strip()
    if not s:
        return None
    if ":" in s:
        m, rest = s.split(":", 1)
        try:
            return float(m) * 60.0 + float(rest)
        except ValueError:
            return None
    try:
        return float(s)
    except ValueError:
        return None


MARGIN_BODYLEN_SEC = 0.20  # 1馬身=0.20秒（簡易仮定）
MARGIN_WORD_TO_BODY = {
    "ハナ": 0.1,
    "アタマ": 0.2,
    "短頭": 0.15,
    "クビ": 0.3,
}


def parse_margin_sec(s: Optional[str]) -> Optional[float]:
    """
    着差文字列（例: '1/2', '1 1/2', '1.1/2', '3/4', '1馬身', 'ハナ' など）を秒に変換する。
    解釈できない値は None（0扱いにしない）。
    """
    if not isinstance(s, str):
        return None

    t = s.strip()
    if not t or t in ("0", "0.0", "0.00"):
        return 0.0

    t = t.replace("　", " ")
    t = t.replace("／", "/")
    t = t.replace("・", ".")

    # 大差系（大きめでOK。後段で頭打ちあり）
    if t in ("大", "大差") or "大差" in t:
        return 4.0

    if "同着" in t:
        return 0.0

    for k, v in MARGIN_WORD_TO_BODY.items():
        if k in t:
            return v * MARGIN_BODYLEN_SEC

    # ○馬身（例: 1馬身, 1.5馬身）
    m_body = re.match(r"^([0-9]+(?:\.[0-9]+)?)\s*馬身$", t)
    if m_body:
        body = float(m_body.group(1))
        return body * MARGIN_BODYLEN_SEC

    # ドット区切りの帯分数（例: 1.1/2, 2.3/4）
    m_dot_mix = re.match(r"^(\d+)\.(\d+)\s*/\s*(\d+)\s*(?:馬身)?$", t)
    if m_dot_mix:
        base = int(m_dot_mix.group(1))
        num = int(m_dot_mix.group(2))
        den = int(m_dot_mix.group(3))
        if den != 0:
            body = base + num / den
            return body * MARGIN_BODYLEN_SEC

    # 空白区切りの帯分数（例: 1 1/2, 1 3/4）
    m_mix = re.match(r"^(\d+)\s+(\d+)\s*/\s*(\d+)\s*(?:馬身)?$", t)
    if m_mix:
        base = int(m_mix.group(1))
        num = int(m_mix.group(2))
        den = int(m_mix.group(3))
        if den != 0:
            body = base + num / den
            return body * MARGIN_BODYLEN_SEC

    # 分数のみ（例: 1/2, 3/4）
    m_frac = re.match(r"^(\d+)\s*/\s*(\d+)\s*(?:馬身)?$", t)
    if m_frac:
        num = int(m_frac.group(1))
        den = int(m_frac.group(2))
        if den != 0:
            body = num / den
            return body * MARGIN_BODYLEN_SEC

    # 数字だけ（馬身として扱う）
    m_num = re.match(r"^([0-9]+(?:\.[0-9]+)?)$", t)
    if m_num:
        body = float(m_num.group(1))
        return body * MARGIN_BODYLEN_SEC

    return None


# ========= 3) レース情報パース（start_time/place/ground/distance） ===========

def parse_race_info(s: str) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[int]]:
    """
    race_info 文字列から
      start_time: '09:50発走' など
      place     : '東京','中山' など（競馬場名）
      ground    : 芝/ダ/障
      distance  : 距離(m)
    を抽出する。
    """
    if not isinstance(s, str):
        return None, None, None, None

    t = s.strip()

    # 発走時刻（例: 09:50発走）
    m_time = re.search(r"(\d{1,2}:\d{2})\s*発走", t)
    start_time = f"{m_time.group(1)}発走" if m_time else None

    # 競馬場名（JRA10場）
    venues = ["札幌", "函館", "福島", "新潟", "東京", "中山", "中京", "京都", "阪神", "小倉"]
    place = None
    for v in venues:
        if v in t:
            place = v
            break

    # 芝/ダ/障 + 距離
    ground = None
    distance = None
    m = re.search(r"(芝|ダ|障)\s*?(\d{4})", t)
    if m:
        ground = m.group(1)
        distance = int(m.group(2))

    return start_time, place, ground, distance


def parse_baba(s: Optional[str]) -> Optional[str]:
    """
    レース情報文字列から馬場状態を抽出して正規化
    """
    if not isinstance(s, str):
        return None
    m = re.search(r"馬場[:：]\s*([^\s/]+)", s)
    if not m:
        return None
    raw = m.group(1)
    if "稍" in raw:
        return "稍重"
    if "良" in raw:
        return "良"
    if "不" in raw:
        return "不良"
    if "重" in raw:
        return "重"
    return raw


def parse_race_class(race_info: Optional[str], race_name: Optional[str]) -> Optional[str]:
    """
    レース情報/レース名からクラスを抽出して正規化する。
    """
    s_info = str(race_info) if isinstance(race_info, str) else ""
    s_name = str(race_name) if isinstance(race_name, str) else ""
    s_all = s_info + " " + s_name

    is_jump = ("障害" in s_all)

    if "未勝利" in s_all:
        return "未勝利(障害)" if is_jump else "未勝利"

    if "新馬" in s_all:
        return "新馬"

    m = re.search(r"([0-3１-３])勝", s_all)
    if m:
        ch = m.group(1)
        trans = str.maketrans("１２３", "123")
        num = ch.translate(trans)
        return f"{num}勝クラス"

    if any(k in s_all for k in ("Ｇ１", "G1", "GI", "ＧＩ")):
        return "Ｇ１"
    if any(k in s_all for k in ("Ｇ２", "G2", "GII", "ＧＩＩ")):
        return "Ｇ２"
    if any(k in s_all for k in ("Ｇ３", "G3", "GIII", "ＧＩＩＩ")):
        return "Ｇ３"

    if "リステッド" in s_all or re.search(r"\bL\b", s_all, flags=re.I):
        return "ｵｰﾌﾟﾝ"
    if any(k in s_all for k in ("ｵｰﾌﾟﾝ", "オープン", "OPEN", "OP")):
        return "ｵｰﾌﾟﾝ"

    return None


# ========= 4) レーティング（Elo近似） ========================================

def rank_to_score(rank: Optional[int], field: int) -> float:
    if rank is None or rank <= 0 or field <= 0:
        return 0.0
    if field == 1:
        return 1.0

    base = 1.0 - ((rank - 1) / (field - 1))
    field_scale = clamp(field / 12.0, 0.70, 1.35)

    if rank == 1:
        base += 0.10 * field_scale
        if field <= 6:
            base -= 0.05
    elif rank == 2:
        base += 0.04 * field_scale
    elif rank == 3:
        base += 0.02 * field_scale

    if field >= 8:
        if rank == field:
            base -= 0.08
        elif rank == field - 1:
            base -= 0.04

    return clamp(base, 0.0, 1.0)


def expected_score(r_i: float, opp_avg: float) -> float:
    return 1.0 / (1.0 + 10 ** ((opp_avg - r_i) / 400.0))


CLASS_EXPECTED_OFFSET = {
    "Ｇ１": 160.0,
    "Ｇ２": 130.0,
    "Ｇ３": 100.0,
    "ｵｰﾌﾟﾝ": 75.0,
    "3勝クラス": 55.0,
    "2勝クラス": 35.0,
    "1勝クラス": 10.0,
    "新馬": -5.0,
    "未勝利": -30.0,
    "未勝利(障害)": -30.0,
}

CLASS_K_MULTIPLIER = {
    "Ｇ１": 1.90,
    "Ｇ２": 1.70,
    "Ｇ３": 1.50,
    "ｵｰﾌﾟﾝ": 1.35,
    "3勝クラス": 1.20,
    "2勝クラス": 1.05,
    "1勝クラス": 0.95,
    "新馬": 0.90,
    "未勝利": 0.85,
    "未勝利(障害)": 0.85,
}

INITIAL_CLASS_RATING = {
    "Ｇ１": 1560.0,
    "Ｇ２": 1545.0,
    "Ｇ３": 1530.0,
    "ｵｰﾌﾟﾝ": 1520.0,
    "3勝クラス": 1515.0,
    "2勝クラス": 1505.0,
    "1勝クラス": 1495.0,
    "新馬": 1480.0,
    "未勝利": 1490.0,
    "未勝利(障害)": 1490.0,
}

SURFACE_RATING_WEIGHT = 0.45
OVERALL_UPDATE_SHARE = 0.65
SURFACE_UPDATE_SHARE = 1.15


def normalize_race_class_key(race_class: Optional[str]) -> Optional[str]:
    if not race_class:
        return None
    rc = str(race_class).strip().replace(" ", "")
    rc = (
        rc.replace("ＧⅠ", "Ｇ１").replace("ＧＩ", "Ｇ１").replace("GI", "Ｇ１").replace("G1", "Ｇ１")
        .replace("ＧⅡ", "Ｇ２").replace("ＧＩＩ", "Ｇ２").replace("GII", "Ｇ２").replace("G2", "Ｇ２")
        .replace("ＧⅢ", "Ｇ３").replace("ＧＩＩＩ", "Ｇ３").replace("GIII", "Ｇ３").replace("G3", "Ｇ３")
        .replace("オープン", "ｵｰﾌﾟﾝ").replace("OPEN", "ｵｰﾌﾟﾝ").replace("OP", "ｵｰﾌﾟﾝ")
    )
    return rc


def class_rating_offset(race_class: Optional[str]) -> float:
    rc = normalize_race_class_key(race_class)
    return CLASS_EXPECTED_OFFSET.get(rc, 0.0)


def class_k_multiplier(race_class: Optional[str]) -> float:
    rc = normalize_race_class_key(race_class)
    return CLASS_K_MULTIPLIER.get(rc, 1.0)


def class_initial_rating(race_class: Optional[str]) -> float:
    rc = normalize_race_class_key(race_class)
    return INITIAL_CLASS_RATING.get(rc, 1500.0)


def clamp(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, value))


def mean_or_default(values: List[float], default: float) -> float:
    if not values:
        return float(default)
    return float(sum(values) / len(values))


def median_or_none(values: List[float]) -> Optional[float]:
    if not values:
        return None
    return float(pd.Series(values, dtype="float").median())


def get_surface_key(ground: Optional[str]) -> Optional[str]:
    if ground in ("芝", "ダ", "障"):
        return str(ground)
    return None


def parse_passing_positions(passing: Optional[str]) -> List[int]:
    if not isinstance(passing, str):
        return []
    text = str(passing).strip()
    if not text:
        return []
    text = text.replace("=", "-").replace("→", "-").replace(">", "-").replace(" ", "")
    return [int(x) for x in re.findall(r"\d+", text) if int(x) > 0]


def distance_band(distance: Optional[int]) -> Optional[str]:
    if distance is None:
        return None
    if distance <= 1400:
        return "短距離"
    if distance <= 1800:
        return "マイル"
    if distance <= 2200:
        return "中距離"
    return "長距離"


def normalize_baba_for_master(baba: Optional[str]) -> Optional[str]:
    if not baba:
        return None
    text = str(baba).strip()
    if text == "稍重":
        return "稍"
    if text == "不良":
        return "不"
    if text in ("良", "稍", "重", "不"):
        return text
    if "稍" in text:
        return "稍"
    if "不" in text:
        return "不"
    if "重" in text:
        return "重"
    if "良" in text:
        return "良"
    return text


def build_course_key(ground: Optional[str], distance: Optional[int]) -> Optional[str]:
    if not ground or distance is None:
        return None
    return f"{ground}{int(distance)}"


def get_master_class_candidates(race_class: Optional[str]) -> List[str]:
    rc = normalize_race_class_key(race_class)
    if rc is None:
        return []
    if rc == "1勝クラス":
        return ["1勝"]
    if rc == "2勝クラス":
        return ["2勝"]
    if rc == "3勝クラス":
        return ["3勝"]
    if rc == "ｵｰﾌﾟﾝ":
        return ["ｵｰﾌﾟﾝ", "OP(L)"]
    return [rc]


def calc_style_bonus(
    rank: Optional[int],
    field: int,
    passing_positions: List[int],
) -> float:
    if rank is None or rank <= 0 or field <= 1 or not passing_positions:
        return 0.0

    avg_pos = sum(passing_positions) / len(passing_positions)
    first_pos = passing_positions[0]
    last_pos = passing_positions[-1]
    avg_ratio = avg_pos / field
    finish_ratio = rank / field
    pos_gain = first_pos - last_pos

    bonus = 0.0
    if avg_ratio <= 0.35 and finish_ratio <= 0.40:
        bonus += 0.04
    elif avg_ratio <= 0.35 and finish_ratio <= 0.60:
        bonus += 0.02

    if avg_ratio >= 0.70 and finish_ratio <= 0.20:
        bonus += 0.02

    if pos_gain >= max(2, int(field * 0.15)) and finish_ratio <= 0.35:
        bonus += 0.02

    if avg_ratio >= 0.75 and finish_ratio >= 0.55:
        bonus -= 0.02

    return clamp(bonus, -0.04, 0.06)


def get_condition_key(
    place: Optional[str],
    ground: Optional[str],
    distance: Optional[int],
    baba: Optional[str],
) -> Tuple[str, str, int, str]:
    return (
        place or "",
        ground or "",
        int(distance or 0),
        baba or "",
    )


def get_day_bias_key(
    date_str: str,
    place: Optional[str],
    ground: Optional[str],
    baba: Optional[str],
) -> Tuple[str, str, str, str]:
    return (
        str(date_str),
        place or "",
        ground or "",
        baba or "",
    )


def get_condition_time_baseline(
    store: "MemoryStore",
    condition_key: Tuple[str, str, int, str],
    race_class: Optional[str],
    day_bias_key: Tuple[str, str, str, str],
) -> Tuple[Optional[float], Optional[float], float]:
    place, ground, distance, baba = condition_key
    master_baba = normalize_baba_for_master(baba)
    course_key = build_course_key(ground, distance)

    cond_best = None
    if place and master_baba and course_key:
        for class_candidate in get_master_class_candidates(race_class):
            ref_key = (place, class_candidate, master_baba, course_key)
            if ref_key in store.condition_time_master:
                cond_best = store.condition_time_master[ref_key]
                break

    fallback_best = median_or_none(store.condition_best_times.get(condition_key, []))
    fallback_median = median_or_none(store.condition_median_times.get(condition_key, []))
    if cond_best is None:
        cond_best = fallback_best

    cond_median = cond_best if cond_best is not None else fallback_median
    day_bias = median_or_none(store.condition_day_bias.get(day_bias_key, []))
    day_bias_val = float(day_bias) if day_bias is not None else 0.0

    adj_best = cond_best + day_bias_val if cond_best is not None else None
    adj_median = cond_median + day_bias_val if cond_median is not None else None
    return adj_best, adj_median, day_bias_val


def compute_performance_score(
    rank_score: float,
    rank: Optional[int],
    field: int,
    margin_sec: Optional[float],
    gap_from_winner_sec: Optional[float],
    winner_margin_sec: Optional[float],
    time_sec: Optional[float],
    best_time_sec: Optional[float],
    median_time_sec: Optional[float],
    cond_best_time_sec: Optional[float],
    cond_median_time_sec: Optional[float],
    last3f: Optional[float],
    best_last3f: Optional[float],
    median_last3f: Optional[float],
    passing_positions: List[int],
) -> float:
    score = rank_score

    if rank == 1:
        win_margin = winner_margin_sec if winner_margin_sec is not None else margin_sec
        if win_margin is not None:
            score += clamp(0.18 * win_margin, 0.0, 0.12)
        else:
            score += 0.02
    elif gap_from_winner_sec is not None:
        score += clamp(-0.10 * gap_from_winner_sec, -0.28, 0.0)
        if rank is not None and rank <= 5 and gap_from_winner_sec <= 0.40:
            score += clamp(0.06 - (0.12 * gap_from_winner_sec), 0.0, 0.06)

    ref_best = cond_best_time_sec if cond_best_time_sec is not None else best_time_sec
    ref_median = cond_median_time_sec if cond_median_time_sec is not None else median_time_sec

    if time_sec is not None:
        if ref_best is not None:
            score += clamp(-0.05 * (time_sec - ref_best), -0.18, 0.10)
        if ref_median is not None:
            score += clamp(0.025 * (ref_median - time_sec), -0.05, 0.07)

    if last3f is not None:
        if best_last3f is not None:
            score += clamp(-0.04 * (last3f - best_last3f), -0.06, 0.05)
        if median_last3f is not None:
            score += clamp(0.015 * (median_last3f - last3f), -0.03, 0.03)

    score += calc_style_bonus(rank, field, passing_positions)
    return clamp(score, 0.0, 1.0)


def k_factor(
    race_class: Optional[str],
    n_starts: int,
    field_size: int,
    winner_margin_sec: Optional[float],
) -> float:
    k = 32.0
    if n_starts <= 3:
        k += 16.0
    k *= class_k_multiplier(race_class)

    if field_size <= 5:
        k *= 0.78
    elif field_size <= 7:
        k *= 0.84
    elif field_size <= 10:
        k *= 0.92
    elif field_size >= 16:
        k *= 1.05

    if winner_margin_sec is not None:
        if winner_margin_sec >= 0.80:
            k *= 1.12
        elif winner_margin_sec >= 0.40:
            k *= 1.06
        elif winner_margin_sec <= 0.10:
            k *= 0.96

    return k


# ========= 5) メモリストア ===============================================

@dataclass
class MemoryStore:
    horses: Dict[str, int] = field(default_factory=dict)
    horse_rows: Dict[int, Dict] = field(default_factory=dict)
    races: Dict[str, Dict] = field(default_factory=dict)
    entries: List[Dict] = field(default_factory=list)
    ratings: Dict[int, float] = field(default_factory=dict)
    surface_ratings: Dict[str, Dict[int, float]] = field(
        default_factory=lambda: {"芝": {}, "ダ": {}, "障": {}}
    )
    start_counts: Dict[int, int] = field(default_factory=dict)
    ratings_history: List[Dict] = field(default_factory=list)
    race_levels: List[Dict] = field(default_factory=list)
    condition_time_master: Dict[Tuple[str, str, str, str], float] = field(default_factory=dict)
    condition_best_times: Dict[Tuple[str, str, int, str], List[float]] = field(default_factory=dict)
    condition_median_times: Dict[Tuple[str, str, int, str], List[float]] = field(default_factory=dict)
    condition_day_bias: Dict[Tuple[str, str, str, str], List[float]] = field(default_factory=dict)

    def get_horse_id(
        self,
        name: str,
        trainer: Optional[str],
        seen_date: str,
        race_class: Optional[str],
        ground: Optional[str],
    ) -> int:
        key = str(name)
        if key in self.horses:
            hid = self.horses[key]
            row = self.horse_rows[hid]
            if row["last_seen"] < seen_date:
                row["last_seen"] = seen_date
            return hid

        new_id = len(self.horses) + 1
        self.horses[key] = new_id
        hid = new_id
        initial_rating = class_initial_rating(race_class)
        self.horse_rows[hid] = {
            "id": hid,
            "name": name,
            "trainer": trainer,
            "first_seen": seen_date,
            "last_seen": seen_date,
            "initial_rating": initial_rating,
        }
        self.ratings[hid] = self.ratings.get(hid, initial_rating)
        surface_key = get_surface_key(ground)
        if surface_key:
            self.surface_ratings.setdefault(surface_key, {})
            self.surface_ratings[surface_key][hid] = self.surface_ratings[surface_key].get(hid, initial_rating)
        return hid

    def get_rating(self, horse_id: int) -> float:
        return self.ratings.get(horse_id, 1500.0)

    def get_surface_rating(self, horse_id: int, ground: Optional[str]) -> float:
        surface_key = get_surface_key(ground)
        if not surface_key:
            return self.get_rating(horse_id)
        surface_map = self.surface_ratings.setdefault(surface_key, {})
        return surface_map.get(horse_id, self.get_rating(horse_id))

    def get_combined_rating(self, horse_id: int, ground: Optional[str]) -> float:
        overall = self.get_rating(horse_id)
        surface = self.get_surface_rating(horse_id, ground)
        return ((1.0 - SURFACE_RATING_WEIGHT) * overall) + (SURFACE_RATING_WEIGHT * surface)

    def set_rating(self, horse_id: int, rating: float) -> None:
        self.ratings[horse_id] = float(rating)

    def set_surface_rating(self, horse_id: int, ground: Optional[str], rating: float) -> None:
        surface_key = get_surface_key(ground)
        if not surface_key:
            return
        surface_map = self.surface_ratings.setdefault(surface_key, {})
        surface_map[horse_id] = float(rating)


def safe_get_scalar(row: pd.Series, key: str):
    if key not in row.index:
        return None
    val = row[key]
    if isinstance(val, pd.Series):
        for v in val:
            if pd.notna(v):
                return v
        return None
    return val


# ========= 6) メイン処理：Excel → MemoryStore ================================

def process_excel_to_memory(xlsx_path: Path) -> MemoryStore:
    assert Path(xlsx_path).exists(), f"入力Excelが見つかりません: {xlsx_path}"

    xls, temp_copy = open_excelfile_safely(Path(xlsx_path))
    store = MemoryStore()
    store.condition_time_master = load_place_baba_time_master(PLACE_BABA_TIME_PATH)

    date_sheets = [s for s in xls.sheet_names if re.match(r"^\d{8}$", str(s))]
    date_sheets = sorted(date_sheets, key=lambda x: int(str(x)))
    if date_sheets:
        print(f"[info] 開催日シート数={len(date_sheets)} / 日付順で処理します（最初={date_sheets[0]} 最後={date_sheets[-1]}）")
    else:
        print("[warn] YYYYMMDD形式の開催日シートが見つかりません（処理対象なし）")

    unknown_margins = set()

    try:
        for sheet in date_sheets:
            print(f"[sheet] シート処理中: {sheet}")
            df_raw = pd.read_excel(xls, sheet_name=sheet)
            if df_raw.empty:
                continue

            df = normalize_columns(df_raw)

            if "race_id" not in df.columns or "horse_name" not in df.columns:
                print(f"  ⚠ 必須カラム不足のためスキップ: {sheet}")
                continue

            for rid, g0 in df.groupby("race_id", dropna=False):
                g = g0.copy()
                date_str = str(sheet)

                race_name = str(g["race_name"].iloc[0]) if "race_name" in g.columns else None
                race_info = str(g["race_info"].iloc[0]) if "race_info" in g.columns else None

                start_time, place, ground, distance = parse_race_info(race_info)
                race_class = parse_race_class(race_info, race_name)
                baba = parse_baba(race_info)
                surface_key = get_surface_key(ground)
                condition_key = get_condition_key(place, ground, distance, baba)
                day_bias_key = get_day_bias_key(date_str, place, ground, baba)

                store.races[str(rid)] = {
                    "race_id": str(rid),
                    "date": date_str,
                    "start_time": start_time,
                    "place": place,
                    "class": race_class,
                    "course": None,
                    "distance": distance,
                    "distance_band": distance_band(distance),
                    "ground": ground,
                    "baba": baba,
                    "race_name": race_name,
                }

                starters: List[Dict] = []
                for _, row in g.iterrows():
                    name = safe_get_scalar(row, "horse_name")
                    if name is None or str(name).strip() == "" or str(name).strip().lower() == "nan":
                        continue
                    trainer = safe_get_scalar(row, "trainer")
                    trainer = None if trainer is None or str(trainer).lower() == "nan" else str(trainer)
                    hid = store.get_horse_id(str(name), trainer, date_str, race_class, ground)

                    rank_val = safe_get_scalar(row, "rank")
                    try:
                        rank_val = int(rank_val)
                    except Exception:
                        rank_val = None

                    time_str = safe_get_scalar(row, "time_str")
                    time_sec = parse_time_str(time_str)

                    margin_str = safe_get_scalar(row, "margin_str")
                    margin_sec = parse_margin_sec(margin_str)
                    if margin_sec is None and isinstance(margin_str, str):
                        ms = margin_str.strip()
                        if ms and ms not in ("0", "0.0", "0.00") and ms.lower() != "nan":
                            unknown_margins.add(ms)

                    last3f = safe_get_scalar(row, "last3f")
                    try:
                        last3f_v = float(last3f)
                    except Exception:
                        last3f_v = None

                    frame = safe_get_scalar(row, "frame")
                    number = safe_get_scalar(row, "number")
                    jockey = safe_get_scalar(row, "jockey")
                    weight = safe_get_scalar(row, "weight")
                    odds = safe_get_scalar(row, "odds")
                    pop = safe_get_scalar(row, "pop")
                    passing_raw = safe_get_scalar(row, "passing")
                    passing_positions = parse_passing_positions(passing_raw)

                    pre_overall = store.get_rating(hid)
                    pre_surface = store.get_surface_rating(hid, ground)
                    pre_effective = store.get_combined_rating(hid, ground)

                    starters.append({
                        "horse_id": hid,
                        "name": str(name),
                        "trainer": trainer,
                        "frame": frame,
                        "number": number,
                        "jockey": jockey,
                        "weight": weight,
                        "odds": odds,
                        "pop": pop,
                        "rank": rank_val,
                        "time_str": time_str,
                        "time_sec": time_sec,
                        "margin_str": margin_str,
                        "margin_sec": margin_sec,
                        "last3f": last3f_v,
                        "passing": passing_raw,
                        "passing_positions": passing_positions,
                        "pre_overall": pre_overall,
                        "pre_surface": pre_surface,
                        "pre_effective": pre_effective,
                    })

                if not starters:
                    continue

                pre_vals = [s["pre_effective"] for s in starters]
                ser = pd.Series(pre_vals, dtype="float")
                field_size_lv = int(len(pre_vals))

                pre_mean = float(ser.mean())
                pre_p50 = float(ser.median())

                pre_top1 = float(ser.max())
                pre_bottom1 = float(ser.min())

                pre_std = float(ser.std(ddof=0)) if field_size_lv >= 2 else 0.0
                q25 = float(ser.quantile(0.25))
                q75 = float(ser.quantile(0.75))
                pre_iqr = float(q75 - q25)

                pre_top3 = float(ser.sort_values(ascending=False).head(3).mean())
                pre_top5 = float(ser.sort_values(ascending=False).head(5).mean())
                pre_top7 = float(ser.sort_values(ascending=False).head(7).mean())
                pre_bottom5 = float(ser.sort_values(ascending=True).head(5).mean())

                gap_top1_p50 = float(pre_top1 - pre_p50)
                gap_top3_p50 = float(pre_top3 - pre_p50)
                gap_top5_p50 = float(pre_top5 - pre_p50)

                time_values = [s["time_sec"] for s in starters if s["time_sec"] is not None]
                last3f_values = [s["last3f"] for s in starters if s["last3f"] is not None]
                best_time = min(time_values) if time_values else None
                median_time = median_or_none(time_values)
                best_last3f = min(last3f_values) if last3f_values else None
                median_last3f = median_or_none(last3f_values)

                cond_best_time, cond_median_time, day_bias_sec = get_condition_time_baseline(
                    store,
                    condition_key,
                    race_class,
                    day_bias_key,
                )

                ranked_starters = sorted(
                    [s for s in starters if s["rank"] is not None and s["rank"] > 0],
                    key=lambda x: x["rank"],
                )
                cumulative_gap = 0.0
                winner_margin_sec = None
                gap_map: Dict[int, float] = {}
                for idx, starter in enumerate(ranked_starters):
                    if idx == 0:
                        gap_map[starter["horse_id"]] = 0.0
                        continue
                    step_gap = starter["margin_sec"] if starter["margin_sec"] is not None else 0.0
                    cumulative_gap += max(float(step_gap), 0.0)
                    gap_map[starter["horse_id"]] = cumulative_gap
                    if idx == 1:
                        winner_margin_sec = cumulative_gap

                field_size = len(starters)
                store.race_levels.append({
                    "race_id": str(rid),
                    "date": date_str,
                    "start_time": start_time,
                    "place": place,
                    "class": race_class,
                    "ground": ground,
                    "distance": distance,
                    "distance_band": distance_band(distance),
                    "baba": baba,
                    "race_name": race_name,
                    "field_size": field_size_lv,
                    "pre_mean": pre_mean,
                    "pre_p50": pre_p50,
                    "pre_top1": pre_top1,
                    "pre_top3_mean": pre_top3,
                    "pre_top5_mean": pre_top5,
                    "pre_top7_mean": pre_top7,
                    "pre_bottom1": pre_bottom1,
                    "pre_bottom5_mean": pre_bottom5,
                    "pre_std": pre_std,
                    "pre_iqr": pre_iqr,
                    "gap_top1_p50": gap_top1_p50,
                    "gap_top3_p50": gap_top3_p50,
                    "gap_top5_p50": gap_top5_p50,
                    "cond_best_time_baseline": cond_best_time,
                    "cond_median_time_baseline": cond_median_time,
                    "day_bias_sec": day_bias_sec,
                    "race_best_time": best_time,
                    "race_median_time": median_time,
                    "winner_margin_sec": winner_margin_sec,
                    "surface_rating_mode": surface_key or "overall",
                })

                for starter in starters:
                    hid = starter["horse_id"]
                    others = [s["pre_effective"] for s in starters if s["horse_id"] != hid]
                    opp_avg = mean_or_default(others, starter["pre_effective"])
                    opp_top3 = mean_or_default(sorted(others, reverse=True)[:3], opp_avg)
                    opp_strength = (opp_avg * 0.55) + (opp_top3 * 0.45) + class_rating_offset(race_class)

                    rank_score = rank_to_score(starter["rank"], field_size)
                    actual_score = compute_performance_score(
                        rank_score=rank_score,
                        rank=starter["rank"],
                        field=field_size,
                        margin_sec=starter["margin_sec"],
                        gap_from_winner_sec=gap_map.get(hid),
                        winner_margin_sec=winner_margin_sec,
                        time_sec=starter["time_sec"],
                        best_time_sec=best_time,
                        median_time_sec=median_time,
                        cond_best_time_sec=cond_best_time,
                        cond_median_time_sec=cond_median_time,
                        last3f=starter["last3f"],
                        best_last3f=best_last3f,
                        median_last3f=median_last3f,
                        passing_positions=starter["passing_positions"],
                    )

                    expected = expected_score(starter["pre_effective"], opp_strength)
                    n_starts = store.start_counts.get(hid, 0)
                    k = k_factor(race_class, n_starts, field_size, winner_margin_sec)
                    delta = k * (actual_score - expected)

                    post_overall = starter["pre_overall"] + (delta * OVERALL_UPDATE_SHARE)
                    post_surface = starter["pre_surface"] + (delta * SURFACE_UPDATE_SHARE)
                    if surface_key:
                        post_effective = ((1.0 - SURFACE_RATING_WEIGHT) * post_overall) + (
                            SURFACE_RATING_WEIGHT * post_surface
                        )
                    else:
                        post_effective = post_overall

                    store.set_rating(hid, post_overall)
                    store.set_surface_rating(hid, ground, post_surface)
                    store.start_counts[hid] = n_starts + 1

                    store.ratings_history.append({
                        "horse_id": hid,
                        "race_id": str(rid),
                        "pre_rating": starter["pre_effective"],
                        "post_rating": post_effective,
                        "pre_overall_rating": starter["pre_overall"],
                        "post_overall_rating": post_overall,
                        "pre_surface_rating": starter["pre_surface"],
                        "post_surface_rating": post_surface,
                        "surface": surface_key,
                        "k_factor": k,
                        "rank_score": rank_score,
                        "actual_score": actual_score,
                        "expected_score": expected,
                        "opp_avg_rating": opp_avg,
                        "opp_top3_rating": opp_top3,
                        "opp_strength": opp_strength,
                        "margin_sec": starter["margin_sec"],
                        "gap_from_winner_sec": gap_map.get(hid),
                        "winner_margin_sec": winner_margin_sec,
                        "condition_best_time": cond_best_time,
                        "condition_median_time": cond_median_time,
                        "ts": datetime.datetime.now().isoformat(timespec="seconds"),
                    })

                    store.entries.append({
                        "race_id": str(rid),
                        "horse_id": hid,
                        "frame": starter["frame"],
                        "number": starter["number"],
                        "jockey": starter["jockey"],
                        "weight": starter["weight"],
                        "odds": starter["odds"],
                        "pop": starter["pop"],
                        "rank": starter["rank"],
                        "time_str": starter["time_str"],
                        "margin_sec": starter["margin_sec"],
                        "gap_from_winner_sec": gap_map.get(hid),
                        "last3f": starter["last3f"],
                        "passing": starter["passing"],
                        "pre_rating": starter["pre_effective"],
                        "pre_overall_rating": starter["pre_overall"],
                        "pre_surface_rating": starter["pre_surface"],
                        "surface": surface_key,
                    })

                if best_time is not None:
                    store.condition_best_times.setdefault(condition_key, []).append(best_time)
                    if cond_best_time is not None:
                        store.condition_day_bias.setdefault(day_bias_key, []).append(best_time - cond_best_time)
                if median_time is not None:
                    store.condition_median_times.setdefault(condition_key, []).append(median_time)
    finally:
        try:
            xls.close()
        except Exception:
            pass
        if temp_copy is not None:
            try:
                Path(temp_copy).unlink(missing_ok=True)
            except Exception:
                pass

    if unknown_margins:
        sample = sorted(list(unknown_margins))[:50]
        print(f"[warn] 着差を秒に変換できなかった値（ユニーク {len(unknown_margins)} 件）: {sample}")
        if len(unknown_margins) > 50:
            print("       ...（50件のみ表示）")

    print("[done] 集計完了（メモリ上）")
    return store


# ========= 7) README（説明書）シート作成 =====================================

def build_readme_dataframe(source_xlsx: Path) -> pd.DataFrame:
    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    rows = []

    def add(sheet_name: str, purpose: str, grain: str, main_cols: str, notes: str = ""):
        rows.append({
            "シート名": sheet_name,
            "目的": purpose,
            "主キー/粒度": grain,
            "主な列": main_cols,
            "備考": notes
        })

    add(
        "horses",
        "馬のマスタ情報。名称の一意化と出現範囲の把握。",
        "1行=1頭（idで一意）",
        "id: 連番ID\nname: 馬名（キー）\ntrainer: 厩舎/調教師\nfirst_seen/last_seen: 最初/最後に観測した開催日(シート名)",
        ""
    )

    add(
        "races",
        "レースの基本属性（会場・発走時刻・距離・馬場など）",
        "1行=1レース（race_idで一意）",
        "race_id: 入力のレースID\n"
        "date: 開催日(シート名)\n"
        "start_time: 発走時刻（例: 09:50発走）\n"
        "place: 開催場（東京/中山/京都など）\n"
        "class: クラス(G1/G2/1勝クラス/未勝利など)\n"
        "course: コース形態(現状未使用)\n"
        "distance: 距離(m)\n"
        "distance_band: 距離帯（短距離/マイル/中距離/長距離）\n"
        "ground: 芝/ダ/障\n"
        "baba: 馬場状態(良/稍重/重/不良)\n"
        "race_name: レース名",
        "race_info 文字列から start_time/place/ground/distance/class/baba を抽出しています。"
    )

    add(
        "entries",
        "出走明細（馬×レースの結果・オッズ等）",
        "1行=1出走（race_id × horse_id）",
        "race_id, horse_id\n"
        "frame/number: 枠番/馬番\n"
        "jockey: 騎手\n"
        "weight: 斤量\n"
        "odds/pop: 単勝オッズ/人気\n"
        "rank: 着順\n"
        "time_str: 走破タイム\n"
        "margin_sec: 着差(秒換算)\n"
        "gap_from_winner_sec: 勝ち馬との差(推定秒)\n"
        "last3f: 後3F（数値化できた場合のみ）\n"
        "passing: 通過順の原文\n"
        "pre_rating/pre_overall_rating/pre_surface_rating: レース前評価\n"
        "surface: 適用した芝/ダ/障rating",
        "着差は「ハナ/アタマ/短頭/クビ/○馬身/分数（1 1/2, 1.1/2, 1/2など）」を 1馬身=0.20秒で秒換算します。"
    )

    add(
        "ratings",
        "各馬の“現時点での最終レーティング”。総合と芝ダ別を併記。",
        "1行=1頭（horse_idで一意）",
        "horse_id: 馬ID（horses.id）\n"
        "rating: 総合rating\n"
        "turf_rating/dirt_rating/jump_rating: 芝/ダ/障の条件別rating",
        "全レース処理後の最終値です。実際の事前評価は総合ratingと条件別ratingを合成して使います。"
    )

    add(
        "ratings_history",
        "各出走ごとのレーティング推移ログ。",
        "1行=1出走イベント",
        "horse_id, race_id\n"
        "pre_rating/post_rating: レース前/後の実効rating\n"
        "pre_overall_rating/post_overall_rating: 総合rating\n"
        "pre_surface_rating/post_surface_rating: 芝/ダ/障rating\n"
        "surface: 更新対象の条件rating\n"
        "k_factor: 学習率\n"
        "rank_score: 頭数比例の着順スコア(0~1)\n"
        "actual_score: 着差・タイム・上がり・通過順込みの実績スコア\n"
        "expected_score: 期待スコア\n"
        "opp_avg_rating/opp_top3_rating/opp_strength: 相手平均と上位層の強さ\n"
        "margin_sec/gap_from_winner_sec/winner_margin_sec: 着差情報\n"
        "condition_best_time/condition_median_time: 条件別標準タイム参照値\n"
        "ts: 記録タイムスタンプ",
        "条件別標準タイムは `data/master/場所_馬場_タイム.xlsx` を参照し、同日同場の補正のみ内部で加えます。"
    )

    add(
        "race_levels",
        "レース前の出走馬レート統計（レースレベル指標）。",
        "1行=1レース（race_idで一意）",
        "race_id: レースID\n"
        "date/start_time/place/class/ground/distance/baba/race_name: レース属性（結合しやすいよう同梱）\n"
        "field_size: 頭数\n"
        "pre_mean/pre_p50: 事前ratingの平均/中央値\n"
        "pre_top1/pre_top3_mean/pre_top5_mean/pre_top7_mean: 上位層の強さ\n"
        "pre_bottom1/pre_bottom5_mean: 下位層の弱さ\n"
        "pre_std/pre_iqr: ばらつき（団子/格差）\n"
        "gap_top1_p50/gap_top3_p50/gap_top5_p50: 1強・上位層の厚み指標\n"
        "cond_best_time_baseline/cond_median_time_baseline/day_bias_sec: 条件別標準タイムと当日馬場差\n"
        "race_best_time/race_median_time/winner_margin_sec: 当該レース実績",
        "条件別標準タイムは `data/master/場所_馬場_タイム.xlsx` を参照。予想側で「レースの強さ・格差・荒れやすさ・条件時計差」を特徴量に利用する想定。"
    )

    meta_top = {
        "シート名": "【このファイルについて】",
        "目的": "race_levels.xlsx の各シートの役割・列の意味をまとめた説明書です。",
        "主キー/粒度": "",
        "主な列": "",
        "備考": f"作成日時: {now}\n元データ: {str(source_xlsx)}"
    }
    rows.insert(0, meta_top)

    return pd.DataFrame(rows, columns=["シート名", "目的", "主キー/粒度", "主な列", "備考"])


# ========= 8) Excel へ書き出し =============================================

def write_store_to_excel(store: MemoryStore, out_path: Path, source_xlsx: Path) -> None:
    out_path = Path(out_path)
    mode = "a" if out_path.exists() else "w"
    print(f"[excel] Excel書き出し: {out_path} (mode={mode})")

    df_horses = pd.DataFrame(list(store.horse_rows.values())).sort_values("id")
    df_races = pd.DataFrame(list(store.races.values())).sort_values("race_id")
    df_entries = pd.DataFrame(store.entries)
    df_ratings = pd.DataFrame([
        {
            "horse_id": hid,
            "rating": store.ratings.get(hid),
            "turf_rating": store.surface_ratings.get("芝", {}).get(hid),
            "dirt_rating": store.surface_ratings.get("ダ", {}).get(hid),
            "jump_rating": store.surface_ratings.get("障", {}).get(hid),
        }
        for hid in sorted(store.ratings.keys())
    ])
    df_hist = pd.DataFrame(store.ratings_history)

    df_levels = pd.DataFrame(store.race_levels)
    if not df_levels.empty and "date" in df_levels.columns:
        df_levels = df_levels.sort_values(["date", "race_id"])
    else:
        df_levels = df_levels.sort_values("race_id")

    df_readme = build_readme_dataframe(source_xlsx)

    if mode == "w":
        with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
            df_horses.to_excel(writer, sheet_name="horses", index=False)
            df_races.to_excel(writer, sheet_name="races", index=False)
            df_entries.to_excel(writer, sheet_name="entries", index=False)
            df_ratings.to_excel(writer, sheet_name="ratings", index=False)
            df_hist.to_excel(writer, sheet_name="ratings_history", index=False)
            df_levels.to_excel(writer, sheet_name="race_levels", index=False)
            df_readme.to_excel(writer, sheet_name="README", index=False)
    else:
        with pd.ExcelWriter(out_path, engine="openpyxl", mode="a", if_sheet_exists="replace") as writer:
            df_horses.to_excel(writer, sheet_name="horses", index=False)
            df_races.to_excel(writer, sheet_name="races", index=False)
            df_entries.to_excel(writer, sheet_name="entries", index=False)
            df_ratings.to_excel(writer, sheet_name="ratings", index=False)
            df_hist.to_excel(writer, sheet_name="ratings_history", index=False)
            df_levels.to_excel(writer, sheet_name="race_levels", index=False)
            df_readme.to_excel(writer, sheet_name="README", index=False)

    print("[done] Excel書き出し完了")


# ========= 9) CLI エントリポイント ==========================================

def main():
    parser = argparse.ArgumentParser(description="racedata_results.xlsx → race_levels.xlsx 変換ツール")
    parser.add_argument("--excel", required=False, default=None, help="入力: racedata_results.xlsx のパス")
    parser.add_argument("--out", required=False, default=None, help="出力: race_levels.xlsx のパス")
    args = parser.parse_args()

    base_dir = Path("C:/Users/okino/OneDrive/ドキュメント/my_python_cursor/keiba_yosou_2026/data/master")
    default_src = base_dir / "racedata_results.xlsx"
    default_dst = base_dir / "race_levels.xlsx"

    src = Path(args.excel) if args.excel else default_src
    dst = Path(args.out) if args.out else default_dst

    if not src.exists():
        print(f"[!] 入力Excelが見つかりません: {src}")
        print(f"    想定フォルダ: {base_dir}")
        if args.excel is None:
            print("    --excel を指定するか、上記フォルダに racedata_results.xlsx を置いてください。")
        raise SystemExit(1)

    print(f"? 入力: {src}")
    print(f"? 出力: {dst}")

    store = process_excel_to_memory(src)
    write_store_to_excel(store, dst, src)


if __name__ == "__main__":
    main()
