# -*- coding: utf-8 -*-
"""
keibayosou_best_import_roi_runner.py

やりたいこと
- 1回目: 元の入力Excelから特徴量計算して with_feat を作る
- 2回目の前: with_feat を元に dl_rank / dl_prob を自動作成して with_dl を作る
- 2回目: with_dl を入力にして最終予想Excelを作る

このコードでは、
1) keibayosou_best_import_roi_runner.py を1回目実行
2) make_dl_rank_from_racedata_results.py を実行
3) keibayosou_best_import_roi_runner.py を2回目実行

という3回実行を、

keibayosou_best_import_roi_runner.py の1回実行

だけで完了できるようにしています。

今回の修正ポイント
- dl_rank だけでなく dl_prob も with_dl.xlsx に保存する
- 既存の動きはなるべく崩さない
"""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import torch
from openpyxl import load_workbook
from torch import nn
from torch.utils.data import DataLoader, TensorDataset

from keibayosou_config import BASE_DIR, HORSE_RESULTS_DIR, RACE_LEVEL_XLSX, BASE_TIME_XLSX, ODDS_CSV
from keibayosou_pipeline import run_pipeline
from keibayosou_utils import _normalize_place


# ============================================================
# 基本パス
# ============================================================
OUTPUT_DIR = BASE_DIR / "data" / "output"
TRAIN_XLSX = BASE_DIR / "data" / "master" / "racedata_results.xlsx"
NOW_SHEET = "今走レース情報"


# ============================================================
# DL順位作成用の列候補
# ============================================================
COLS_CANDIDATES: Dict[str, List[str]] = {
    "horse_name": ["馬名"],
    "finish": ["着順", "着 順"],
    "race_id": ["レースID", "race_id"],
    "umaban": ["馬番", "馬 番"],
    "popularity": ["人気", "人 気"],
    "odds": ["単勝オッズ", "単勝 オッズ", "単勝"],
    "frame": ["枠"],
    "weight": ["斤量"],
    "sex_age": ["性齢"],
    "body_weight": ["馬体重(増減)", "馬体重 (増減)", "馬体重"],
}

DL_FEATURE_COLS = [
    "popularity",
    "odds",
    "frame",
    "weight",
    "age",
    "sex",
    "body_weight",
    "body_weight_diff",
]


# ============================================================
# パス解決
# ============================================================
def _resolve_raw_src_out_paths(raceday_str: Optional[str]) -> Tuple[str, str]:
    """
    1回目実行用
    - 入力: 元の 馬の競走成績_YYYYMMDD.xlsx
    - 出力: data/output/馬の競走成績_with_feat_YYYYMMDD.xlsx
    """
    if raceday_str:
        candidate = HORSE_RESULTS_DIR / f"馬の競走成績_{raceday_str}.xlsx"
        if os.path.exists(candidate):
            src_excel = str(candidate)
        else:
            src_excel = str(HORSE_RESULTS_DIR / "馬の競走成績.xlsx")
        out_excel = str(OUTPUT_DIR / f"馬の競走成績_with_feat_{raceday_str}.xlsx")
    else:
        src_excel = str(HORSE_RESULTS_DIR / "馬の競走成績.xlsx")
        out_excel = str(OUTPUT_DIR / "馬の競走成績_with_feat.xlsx")
    return src_excel, out_excel


def _resolve_second_run_paths(raceday_str: Optional[str]) -> Tuple[str, str]:
    """
    2回目実行用
    - 入力: with_dl
    - 出力: 最終の with_feat
    """
    if raceday_str:
        src_excel = str(OUTPUT_DIR / f"馬の競走成績_with_feat_{raceday_str}_with_dl.xlsx")
        out_excel = str(OUTPUT_DIR / f"馬の競走成績_with_feat_{raceday_str}.xlsx")
    else:
        src_excel = str(OUTPUT_DIR / "馬の競走成績_with_feat_with_dl.xlsx")
        out_excel = str(OUTPUT_DIR / "馬の競走成績_with_feat.xlsx")
    return src_excel, out_excel


def _resolve_with_dl_path(raceday_str: Optional[str]) -> Path:
    if raceday_str:
        return OUTPUT_DIR / f"馬の競走成績_with_feat_{raceday_str}_with_dl.xlsx"
    return OUTPUT_DIR / "馬の競走成績_with_feat_with_dl.xlsx"


def _pick_actual_out_excel(expected_out_excel: str) -> str:
    """
    pipeline 側が PermissionError 回避のため、
    末尾に _HHMMSS を付けて保存することがある。
    その場合でも「実際にできた出力Excel」を拾う。
    """
    if os.path.exists(expected_out_excel):
        return expected_out_excel

    out_dir = os.path.dirname(expected_out_excel)
    base = os.path.splitext(os.path.basename(expected_out_excel))[0]
    ext = os.path.splitext(expected_out_excel)[1]

    if not os.path.isdir(out_dir):
        return expected_out_excel

    cands = [
        os.path.join(out_dir, f)
        for f in os.listdir(out_dir)
        if f.startswith(base) and f.endswith(ext)
    ]
    if not cands:
        return expected_out_excel

    return max(cands, key=lambda p: os.path.getmtime(p))


# ============================================================
# OZZU単勝オッズ反映
# ============================================================
def _pick_ozzu_csv(base: str, raceday: Optional[str]) -> Optional[str]:
    """
    ODDS_CSV（フォルダ or ファイル）から、使う OZZU CSV を1つ選ぶ。
    - base がファイルならそれを使う
    - base がフォルダなら、raceday を含むファイルを優先し、なければ最新を使う
    """
    if os.path.isfile(base):
        return base
    if not os.path.isdir(base):
        return None

    csv_files = [os.path.join(base, f) for f in os.listdir(base) if f.lower().endswith(".csv")]
    if not csv_files:
        return None

    if raceday:
        preferred = [p for p in csv_files if str(raceday) in os.path.basename(p)]
        if preferred:
            return max(preferred, key=lambda p: os.path.getmtime(p))

    return max(csv_files, key=lambda p: os.path.getmtime(p))


def _read_csv_any_encoding(path: str) -> pd.DataFrame:
    last_err: Optional[Exception] = None
    for enc in ("utf-8-sig", "utf-8", "cp932", "shift_jis", "utf-16"):
        try:
            return pd.read_csv(path, encoding=enc)
        except Exception as e:
            last_err = e
            continue
    if last_err:
        raise last_err
    raise RuntimeError("CSV 読み込みに失敗しました")


def _build_tansho_map_from_ozzu(ozzu_raw: pd.DataFrame) -> Dict[Tuple[str, str, int], float]:
    """
    OZZU CSV（racecourse/race/bet_type/combination/odds）から
    (place_norm, race_no, umaban) -> odds の辞書を作る
    """
    need_cols = {"racecourse", "race", "bet_type", "combination", "odds"}
    if not need_cols.issubset(set(ozzu_raw.columns)):
        missing = need_cols - set(ozzu_raw.columns)
        raise ValueError(f"OZZU CSV に必要列が不足しています: {missing}")

    ozzu = ozzu_raw.copy()
    ozzu = ozzu[ozzu["bet_type"].astype(str).str.contains("単勝", na=False)].copy()

    def _to_race_no(x: object) -> str:
        m = re.search(r"(\d+)", str(x))
        return m.group(1).zfill(2) if m else ""

    def _to_umaban(x: object) -> Optional[int]:
        m = re.search(r"(\d+)", str(x))
        return int(m.group(1)) if m else None

    def _to_odds(x: object) -> Optional[float]:
        s = str(x).replace(",", "")
        if not re.search(r"\d", s):
            return None
        try:
            return float(s)
        except Exception:
            return None

    ozzu["place_norm"] = ozzu["racecourse"].map(_normalize_place)
    ozzu["race_no"] = ozzu["race"].map(_to_race_no)
    ozzu["umaban"] = ozzu["combination"].map(_to_umaban)
    ozzu["tansho"] = ozzu["odds"].map(_to_odds)

    ozzu = ozzu.dropna(subset=["place_norm", "race_no", "umaban", "tansho"])
    out: Dict[Tuple[str, str, int], float] = {}
    for p, r, u, o in zip(ozzu["place_norm"], ozzu["race_no"], ozzu["umaban"], ozzu["tansho"]):
        out[(str(p), str(r), int(u))] = float(o)
    return out


def _fill_tansho_odds_to_bet_sheet(out_excel_path: str, raceday_str: Optional[str]) -> int:
    """
    出力Excelの「買い目_レース別1行」シートに、単勝オッズ_1位 を直接書き込む。
    """
    if not os.path.exists(out_excel_path):
        print(f"[WARN] 出力Excelが見つからないため、単勝オッズ反映をスキップ: {out_excel_path}")
        return 0

    ozzu_path = _pick_ozzu_csv(str(ODDS_CSV), raceday_str)
    if not ozzu_path or not os.path.exists(ozzu_path):
        print("[WARN] オッズCSVが見つからないため単勝オッズ付与をスキップします")
        return 0

    try:
        ozzu_raw = _read_csv_any_encoding(ozzu_path)
        tansho_map = _build_tansho_map_from_ozzu(ozzu_raw)
    except Exception as e:
        print(f"[WARN] OZZU CSV から単勝オッズマップ作成に失敗しました: {e}")
        return 0

    try:
        wb = load_workbook(out_excel_path)
    except Exception as e:
        print(f"[WARN] 出力Excelを開けませんでした: {e}")
        return 0

    sheet_name = "買い目_レース別1行"
    if sheet_name not in wb.sheetnames:
        print(f"[INFO] '{sheet_name}' シートが無いため単勝オッズ反映をスキップします")
        return 0

    ws = wb[sheet_name]

    header_to_col = {}
    for c in range(1, ws.max_column + 1):
        v = ws.cell(row=1, column=c).value
        if v is None:
            continue
        header_to_col[str(v).strip()] = c

    need = ["場所", "レースID", "1位馬番"]
    for k in need:
        if k not in header_to_col:
            print(f"[WARN] '{sheet_name}' に必要列がありません: {k} -> 単勝オッズ反映をスキップ")
            return 0

    odds_col_name = "単勝オッズ_1位"
    if odds_col_name not in header_to_col:
        new_c = ws.max_column + 1
        ws.cell(row=1, column=new_c).value = odds_col_name
        header_to_col[odds_col_name] = new_c
        print(f"[INFO] '{sheet_name}' に '{odds_col_name}' 列が無かったため新規作成しました（列={new_c}）")

    col_place = header_to_col["場所"]
    col_rid = header_to_col["レースID"]
    col_umaban1 = header_to_col["1位馬番"]
    col_odds = header_to_col[odds_col_name]

    def _race_no_from_rid(rid_val: object) -> str:
        m = re.search(r"(\d{2})$", str(rid_val))
        return m.group(1) if m else ""

    def _to_int_safe(x: object) -> Optional[int]:
        m = re.search(r"(\d+)", str(x))
        return int(m.group(1)) if m else None

    filled = 0
    total = 0

    for r in range(2, ws.max_row + 1):
        rid = ws.cell(row=r, column=col_rid).value
        if rid is None:
            continue

        place = ws.cell(row=r, column=col_place).value
        umaban1 = ws.cell(row=r, column=col_umaban1).value

        total += 1

        place_norm = _normalize_place(place)
        race_no = _race_no_from_rid(rid)
        u1 = _to_int_safe(umaban1)
        if not place_norm or not race_no or u1 is None:
            continue

        odds_val = tansho_map.get((place_norm, race_no, int(u1)))
        if odds_val is None:
            continue

        ws.cell(row=r, column=col_odds).value = float(odds_val)
        filled += 1

    try:
        wb.save(out_excel_path)
    except PermissionError:
        print(f"[WARN] 出力Excelが開かれている可能性があります。Excelを閉じてから再実行してください: {out_excel_path}")
        return 0

    print(f"[INFO] 単勝オッズ反映完了: {filled}/{total} -> {out_excel_path}")
    return filled


# ============================================================
# DL順位作成ユーティリティ
# ============================================================
def _normalize_colname(name: object) -> str:
    s = "" if name is None else str(name)
    s = s.replace("\u3000", " ")
    s = re.sub(r"\s+", "", s)
    return s.lower()


def _pick_col(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    norm_map = {_normalize_colname(c): str(c) for c in df.columns}
    for cand in candidates:
        key = _normalize_colname(cand)
        if key in norm_map:
            return norm_map[key]
    return None


def _normalize_rid_series(s: pd.Series) -> pd.Series:
    s_digits = s.astype(str).str.strip().str.replace(r"\D", "", regex=True)
    num = pd.to_numeric(s, errors="coerce")
    num_str = num.astype("Int64").astype(str).replace("<NA>", pd.NA)
    out = s_digits.where(s_digits.notna() & (s_digits != ""), num_str).fillna("").astype(str)
    out = out.map(lambda x: x[-12:] if isinstance(x, str) and len(x) > 12 else x)
    return out


def _normalize_umaban_series(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce").astype("Int64")


def _parse_sex_age(val: object) -> Tuple[float, float]:
    if val is None or (isinstance(val, float) and np.isnan(val)):
        return np.nan, np.nan
    s = str(val).strip()
    if s == "":
        return np.nan, np.nan

    sex_map = {"牡": 0.0, "牝": 1.0, "セ": 2.0}
    sex_val = np.nan
    for k, v in sex_map.items():
        if s.startswith(k):
            sex_val = v
            break

    m = re.search(r"(\d+)", s)
    age_val = float(m.group(1)) if m else np.nan
    return sex_val, age_val


def _parse_body_weight(val: object) -> Tuple[float, float]:
    if val is None or (isinstance(val, float) and np.isnan(val)):
        return np.nan, np.nan
    s = str(val).strip()
    if s == "" or "計不" in s:
        return np.nan, np.nan

    m_w = re.search(r"(\d+)", s)
    weight_val = float(m_w.group(1)) if m_w else np.nan

    m_d = re.search(r"\(([-+]?\d+)\)", s)
    diff_val = float(m_d.group(1)) if m_d else np.nan
    return weight_val, diff_val


def _to_numeric_series(df: pd.DataFrame, col: Optional[str]) -> pd.Series:
    if col is None or col not in df.columns:
        return pd.Series([np.nan] * len(df))
    return pd.to_numeric(df[col], errors="coerce")


def _build_feature_df(df: pd.DataFrame, col_map: Dict[str, Optional[str]]) -> pd.DataFrame:
    pop = _to_numeric_series(df, col_map.get("popularity"))
    odds = _to_numeric_series(df, col_map.get("odds"))
    frame = _to_numeric_series(df, col_map.get("frame"))
    weight = _to_numeric_series(df, col_map.get("weight"))

    sex_age_col = col_map.get("sex_age")
    sex_age = df[sex_age_col] if sex_age_col in df.columns else pd.Series([np.nan] * len(df))
    sex_age_parsed = sex_age.map(_parse_sex_age)
    sex = sex_age_parsed.map(lambda x: x[0])
    age = sex_age_parsed.map(lambda x: x[1])

    bw_col = col_map.get("body_weight")
    bw = df[bw_col] if bw_col in df.columns else pd.Series([np.nan] * len(df))
    bw_parsed = bw.map(_parse_body_weight)
    body_weight = bw_parsed.map(lambda x: x[0])
    body_weight_diff = bw_parsed.map(lambda x: x[1])

    feat = pd.DataFrame(
        {
            "popularity": pop,
            "odds": odds,
            "frame": frame,
            "weight": weight,
            "age": age,
            "sex": sex,
            "body_weight": body_weight,
            "body_weight_diff": body_weight_diff,
        }
    )
    feat = feat.fillna(0.0)
    return feat


def _build_training_dataframe(path: Path = TRAIN_XLSX) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"学習用Excelが見つかりません: {path}")

    book = pd.read_excel(path, sheet_name=None, engine="openpyxl")
    rows: List[pd.DataFrame] = []

    for _, df in book.items():
        if not isinstance(df, pd.DataFrame) or df.empty:
            continue

        c_name = _pick_col(df, COLS_CANDIDATES["horse_name"])
        c_finish = _pick_col(df, COLS_CANDIDATES["finish"])
        c_race_id = _pick_col(df, COLS_CANDIDATES["race_id"])

        if c_name is None or c_finish is None or c_race_id is None:
            continue

        name = df[c_name]
        finish = pd.to_numeric(df[c_finish], errors="coerce")
        race_id = df[c_race_id].astype(str).str.replace(r"\D", "", regex=True)
        race_id_ok = race_id.str.len() == 12

        mask = name.notna() & finish.notna() & race_id_ok
        if mask.sum() == 0:
            continue

        use_df = df.loc[mask].copy()
        finish = finish.loc[mask].reset_index(drop=True)

        col_map = {
            "popularity": _pick_col(use_df, COLS_CANDIDATES["popularity"]),
            "odds": _pick_col(use_df, COLS_CANDIDATES["odds"]),
            "frame": _pick_col(use_df, COLS_CANDIDATES["frame"]),
            "weight": _pick_col(use_df, COLS_CANDIDATES["weight"]),
            "sex_age": _pick_col(use_df, COLS_CANDIDATES["sex_age"]),
            "body_weight": _pick_col(use_df, COLS_CANDIDATES["body_weight"]),
        }

        feat = _build_feature_df(use_df, col_map)
        y = (finish <= 3).astype(int).reset_index(drop=True)

        rid_str = _normalize_rid_series(use_df[c_race_id]).reset_index(drop=True)
        umaban_col = _pick_col(use_df, COLS_CANDIDATES["umaban"])
        umaban = (
            _normalize_umaban_series(use_df[umaban_col]).reset_index(drop=True)
            if umaban_col
            else pd.Series([pd.NA] * len(use_df))
        )

        base = pd.DataFrame(
            {
                "rid_str": rid_str,
                "馬番": umaban,
                "馬名": use_df[c_name].astype(str).reset_index(drop=True),
                "y": y,
            }
        )

        train_df = pd.concat([base, feat.reset_index(drop=True)], axis=1)
        rows.append(train_df)

    if not rows:
        raise RuntimeError("学習データが作成できませんでした（有効な行がありません）")

    df_train = pd.concat(rows, ignore_index=True)
    for col in DL_FEATURE_COLS:
        if col in df_train.columns:
            df_train[col] = pd.to_numeric(df_train[col], errors="coerce").fillna(0.0)

    df_train["y"] = pd.to_numeric(df_train["y"], errors="coerce").fillna(0).astype(int)
    return df_train


class SimpleMLP(nn.Module):
    def __init__(self, input_dim: int) -> None:
        super().__init__()
        self.net = nn.Sequential(
            nn.Linear(input_dim, 64),
            nn.ReLU(),
            nn.Dropout(0.2),
            nn.Linear(64, 32),
            nn.ReLU(),
            nn.Dropout(0.2),
            nn.Linear(32, 1),
        )

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        return self.net(x).squeeze(1)


def _train_model(X: np.ndarray, y: np.ndarray, seed: int = 42) -> Tuple[SimpleMLP, np.ndarray, np.ndarray]:
    rng = np.random.default_rng(seed)
    idx = rng.permutation(len(X))
    split = int(len(X) * 0.8)
    train_idx = idx[:split]
    val_idx = idx[split:]

    X_train = X[train_idx]
    y_train = y[train_idx]
    X_val = X[val_idx]
    y_val = y[val_idx]

    mean = X_train.mean(axis=0)
    std = X_train.std(axis=0)
    std = np.where(std == 0, 1.0, std)

    X_train = (X_train - mean) / std
    X_val = (X_val - mean) / std

    torch.manual_seed(seed)

    train_ds = TensorDataset(torch.from_numpy(X_train), torch.from_numpy(y_train))
    train_loader = DataLoader(train_ds, batch_size=256, shuffle=True)

    model = SimpleMLP(input_dim=X.shape[1])
    optimizer = torch.optim.Adam(model.parameters(), lr=1e-3)
    loss_fn = nn.BCEWithLogitsLoss()

    model.train()
    for epoch in range(20):
        epoch_loss = 0.0
        for xb, yb in train_loader:
            optimizer.zero_grad()
            logits = model(xb)
            loss = loss_fn(logits, yb)
            loss.backward()
            optimizer.step()
            epoch_loss += float(loss.item())

        if (epoch + 1) % 5 == 0:
            model.eval()
            with torch.no_grad():
                val_logits = model(torch.from_numpy(X_val))
                val_loss = loss_fn(val_logits, torch.from_numpy(y_val)).item()
            model.train()
            print(f"[INFO] epoch={epoch+1} train_loss={epoch_loss:.4f} val_loss={val_loss:.4f}")

    return model, mean, std


def _load_now_data(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"予測用Excelが見つかりません: {path}")
    book = pd.read_excel(path, sheet_name=None, engine="openpyxl")
    if NOW_SHEET not in book:
        raise RuntimeError(f"今走シートが見つかりません: sheet={NOW_SHEET} path={path}")
    return book[NOW_SHEET].copy()


def _ensure_rid_umaban(df: pd.DataFrame) -> pd.DataFrame:
    if "rid_str" in df.columns:
        df["rid_str"] = _normalize_rid_series(df["rid_str"])
    else:
        c_race_id = _pick_col(df, COLS_CANDIDATES["race_id"])
        if c_race_id is None:
            raise RuntimeError("rid_str 列が無く、レースID列も見つかりません")
        df["rid_str"] = _normalize_rid_series(df[c_race_id])

    c_umaban = _pick_col(df, COLS_CANDIDATES["umaban"])
    if c_umaban is None:
        raise RuntimeError("馬番列が見つかりません")
    df["馬番"] = _normalize_umaban_series(df[c_umaban])
    return df


def _predict_dl_rank(model: SimpleMLP, mean: np.ndarray, std: np.ndarray, now_df: pd.DataFrame) -> pd.DataFrame:
    """
    今回の修正ポイント
    - dl_rank だけでなく dl_prob も返す
    """
    col_map = {
        "popularity": _pick_col(now_df, COLS_CANDIDATES["popularity"]),
        "odds": _pick_col(now_df, COLS_CANDIDATES["odds"] + ["tansho"]),
        "frame": _pick_col(now_df, COLS_CANDIDATES["frame"]),
        "weight": _pick_col(now_df, COLS_CANDIDATES["weight"]),
        "sex_age": _pick_col(now_df, COLS_CANDIDATES["sex_age"]),
        "body_weight": _pick_col(now_df, COLS_CANDIDATES["body_weight"]),
    }

    feat = _build_feature_df(now_df, col_map)
    X = feat[DL_FEATURE_COLS].to_numpy(dtype=np.float32)
    X = (X - mean) / std

    model.eval()
    with torch.no_grad():
        logits = model(torch.from_numpy(X))
        prob = torch.sigmoid(logits).cpu().numpy()

    out = now_df.copy()
    out = _ensure_rid_umaban(out)
    out["dl_prob"] = prob.astype(float)
    out["dl_rank"] = out.groupby("rid_str")["dl_prob"].rank(ascending=False, method="first").astype("Int64")

    return out[["rid_str", "馬番", "dl_prob", "dl_rank"]]


def _write_back_dl_rank(src_path: Path, out_path: Path, dl_rank_df: pd.DataFrame) -> None:
    """
    今回の修正ポイント
    - dl_rank だけでなく dl_prob も書き戻す
    - すでに列がある場合は上書き優先
    """
    book = pd.read_excel(src_path, sheet_name=None, engine="openpyxl")
    if NOW_SHEET not in book:
        raise RuntimeError(f"今走シートが見つかりません: sheet={NOW_SHEET} path={src_path}")

    now_df = book[NOW_SHEET].copy()
    now_df = _ensure_rid_umaban(now_df)

    now_df = pd.merge(
        now_df,
        dl_rank_df,
        on=["rid_str", "馬番"],
        how="left",
        suffixes=("", "_dl"),
    )

    # dl_rank の整理
    if "dl_rank_dl" in now_df.columns:
        dl_rank_pred = now_df["dl_rank_dl"]
        if isinstance(dl_rank_pred, pd.DataFrame):
            dl_rank_pred = dl_rank_pred.iloc[:, 0]

        dl_rank_base = now_df.get("dl_rank")
        if isinstance(dl_rank_base, pd.DataFrame):
            dl_rank_base = dl_rank_base.iloc[:, 0]

        if dl_rank_base is None:
            now_df["dl_rank"] = dl_rank_pred
        else:
            now_df["dl_rank"] = dl_rank_pred.combine_first(dl_rank_base)

        now_df = now_df.drop(columns=[c for c in now_df.columns if c == "dl_rank_dl"])

    # dl_prob の整理
    if "dl_prob_dl" in now_df.columns:
        dl_prob_pred = now_df["dl_prob_dl"]
        if isinstance(dl_prob_pred, pd.DataFrame):
            dl_prob_pred = dl_prob_pred.iloc[:, 0]

        dl_prob_base = now_df.get("dl_prob")
        if isinstance(dl_prob_base, pd.DataFrame):
            dl_prob_base = dl_prob_base.iloc[:, 0]

        if dl_prob_base is None:
            now_df["dl_prob"] = dl_prob_pred
        else:
            now_df["dl_prob"] = dl_prob_pred.combine_first(dl_prob_base)

        now_df = now_df.drop(columns=[c for c in now_df.columns if c == "dl_prob_dl"])

    if "dl_rank" in now_df.columns:
        now_df["dl_rank"] = pd.to_numeric(now_df["dl_rank"], errors="coerce").astype("Int64")

    if "dl_prob" in now_df.columns:
        now_df["dl_prob"] = pd.to_numeric(now_df["dl_prob"], errors="coerce")

    book[NOW_SHEET] = now_df

    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        for sheet_name, df in book.items():
            df.to_excel(writer, sheet_name=sheet_name, index=False)


def _create_with_dl_excel(pred_path: Path, out_path: Path) -> None:
    """
    1回目の with_feat Excel を読み、
    dl_rank / dl_prob を自動作成して with_dl Excel を出力する。
    """
    print("[INFO] DL用の学習データ読み込み中...")
    df_train = _build_training_dataframe(TRAIN_XLSX)
    X = df_train[DL_FEATURE_COLS].to_numpy(dtype=np.float32)
    y = df_train["y"].to_numpy(dtype=np.float32)

    if len(X) == 0:
        raise RuntimeError("学習データが空です")

    print("[INFO] DLモデル学習中...")
    model, mean, std = _train_model(X, y)

    print("[INFO] DL順位を予測中...")
    now_df = _load_now_data(pred_path)
    dl_rank_df = _predict_dl_rank(model, mean, std, now_df)

    print("[INFO] with_dl Excel に書き戻し中...")
    _write_back_dl_rank(pred_path, out_path, dl_rank_df)

    print(f"[INFO] with_dl 作成完了: {out_path}")


# ============================================================
# main
# ============================================================
def main() -> None:
    raceday_str = input("対象レース日付を YYYYMMDD 形式で入力してください（空Enterなら全日対象）: ").strip()
    if raceday_str == "":
        raceday_str = None
    elif not re.fullmatch(r"\d{8}", raceday_str):
        raise ValueError("対象レース日付は YYYYMMDD の8桁で入力してください")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # --------------------------------------------------------
    # 1回目: 元データ -> with_feat
    # --------------------------------------------------------
    src_excel_first, out_excel_first = _resolve_raw_src_out_paths(raceday_str)

    if not os.path.exists(src_excel_first):
        raise FileNotFoundError(f"入力ファイルが見つかりません: {src_excel_first}")

    print("[INFO] ===== 1回目の予想処理を開始します =====")
    run_pipeline(
        SRC_EXCEL=src_excel_first,
        OUT_EXCEL=out_excel_first,
        LEVELS_XL=RACE_LEVEL_XLSX,
        BASE_TIME=BASE_TIME_XLSX,
        ODDS_CSV_PATH=ODDS_CSV,
        RACEDAY=raceday_str,
    )

    actual_first_out = _pick_actual_out_excel(out_excel_first)
    if not os.path.exists(actual_first_out):
        raise FileNotFoundError(f"1回目の出力Excelが見つかりません: {actual_first_out}")

    # --------------------------------------------------------
    # DL順位作成: with_feat -> with_dl
    # --------------------------------------------------------
    with_dl_path = _resolve_with_dl_path(raceday_str)
    print("[INFO] ===== DL順位作成を開始します =====")
    _create_with_dl_excel(Path(actual_first_out), with_dl_path)

    if not with_dl_path.exists():
        raise FileNotFoundError(f"with_dl Excel が作成されませんでした: {with_dl_path}")

    # --------------------------------------------------------
    # 2回目: with_dl -> 最終 with_feat
    # --------------------------------------------------------
    src_excel_second, out_excel_second = _resolve_second_run_paths(raceday_str)

    print("[INFO] ===== 2回目の予想処理を開始します =====")
    run_pipeline(
        SRC_EXCEL=src_excel_second,
        OUT_EXCEL=out_excel_second,
        LEVELS_XL=RACE_LEVEL_XLSX,
        BASE_TIME=BASE_TIME_XLSX,
        ODDS_CSV_PATH=ODDS_CSV,
        RACEDAY=raceday_str,
    )

    actual_final_out = _pick_actual_out_excel(out_excel_second)
    if not os.path.exists(actual_final_out):
        raise FileNotFoundError(f"最終出力Excelが見つかりません: {actual_final_out}")

    _fill_tansho_odds_to_bet_sheet(actual_final_out, raceday_str)

    print("[INFO] ===== すべて完了しました =====")
    print(f"[INFO] 最終出力: {actual_final_out}")


if __name__ == "__main__":
    main()
