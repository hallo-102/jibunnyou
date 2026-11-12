"""同じ固定入力とマスタでGitHub mainの予想を独立したShadowとして実行する。"""
from __future__ import annotations

from datetime import datetime
import importlib
import hashlib
import json
import os
from pathlib import Path
import sys
import tempfile
import shutil

import pandas as pd

from .archive import JST, sha256, verify_archive, write_json
from .runtime_metadata import collect


def verify_shadow(folder: Path) -> dict:
    manifest = json.loads((folder / "manifest.json").read_text(encoding="utf-8"))
    if manifest["prediction_sha256"] != sha256(folder / "prediction.xlsx") or manifest["runtime_sha256"] != sha256(folder / "runtime.json"):
        raise ValueError("Shadow固定保存のSHA不一致")
    if "adapter_sha256" in manifest:
        if sha256(folder / "prediction_adapter.py") != manifest["adapter_sha256"]:
            raise ValueError("Shadow実行アダプタのSHA不一致")
        metadata = json.loads((folder / "runtime.json").read_text(encoding="utf-8"))
        for name, expected in metadata["code_hashes"].items():
            path = (folder / "source" / name).resolve()
            if not path.is_relative_to((folder / "source").resolve()) or sha256(path) != expected:
                raise ValueError("Shadow固定ソースのSHA不一致")
    return manifest


def complete_market_columns(pre: Path, prediction: Path, day: str) -> None:
    # 通常runnerと同じオッズ照合・人気補完を通してから、既存買い目を生成する。
    runner = importlib.import_module("1_keibayosou_best_import_roi_runner")
    runner.ODDS_CSV = str(pre / "odds.csv")
    target = pd.read_excel(prediction, sheet_name="TARGET")
    now = pd.read_excel(prediction, sheet_name="今走レース情報")
    enriched = runner._merge_now_and_odds_for_estimation(target, now, day)
    columns = [c for c in ("人気", "単勝オッズ", "複勝オッズ") if c in enriched]
    market = enriched[["rid_str", "馬番", *columns]].copy()
    market["rid_str"] = runner._normalize_rid_series(market["rid_str"])
    market["馬番"] = runner._normalize_umaban_series(market["馬番"])
    if market.duplicated(["rid_str", "馬番"]).any():
        raise ValueError("Shadowのオッズ照合キーが重複しています")
    frames = {}
    for name, frame in (("TARGET", target), ("今走レース情報", now)):
        frame = frame.copy()
        frame["rid_str"] = runner._normalize_rid_series(frame["rid_str"] if "rid_str" in frame else frame["レースID"])
        frame["馬番"] = runner._normalize_umaban_series(frame["馬番"])
        frames[name] = frame.drop(columns=columns, errors="ignore").merge(market, on=["rid_str", "馬番"], how="left", validate="one_to_one")
    if target["rank"].tolist() != frames["TARGET"]["rank"].tolist():
        raise ValueError("市場情報の補完でShadow順位が変化しました")
    with pd.ExcelWriter(prediction, engine="openpyxl", mode="a", if_sheet_exists="replace") as writer:
        for name, frame in frames.items():
            frame.to_excel(writer, sheet_name=name, index=False)
    pipeline = importlib.import_module("1_keibayosou_pipeline")
    pipeline.append_roi_focus_bet_sheet_to_excel(str(prediction))


def predict(pre: Path, *, retrospective: bool = False) -> Path:
    manifest = verify_archive(pre)
    adapter_sha = sha256(Path(__file__))
    destination = pre.parent / ("shadow_pre_" + adapter_sha[:12])
    if destination.exists():
        saved = verify_shadow(destination)
        if saved["production_pre_sha256"] != sha256(pre / "manifest.json"):
            raise ValueError("Shadowが別の固定入力を参照しています")
        return destination
    entries = pd.read_excel(pre / "input.xlsx", sheet_name="今走レース情報")
    departures = pd.to_datetime(manifest["day"] + " " + entries["発走時刻"].astype(str), format="%Y%m%d %H:%M", errors="raise")
    deadline = departures.min().to_pydatetime().replace(tzinfo=JST)
    if not retrospective and (manifest["mode"] != "prospective" or datetime.now(JST) >= deadline):
        raise ValueError("Shadowを発走前に開始できません。MISSEDとして事前実績から除外します")
    root = Path(os.getenv("KEIBA_SHADOW_SOURCE_DIR", str(Path(__file__).resolve().parents[1]))).resolve()
    sys.path.insert(0, str(root))
    config = importlib.import_module("1_keibayosou_config")
    if any(float(getattr(config, name)) != 0 for name in ("DL_PROB_BLEND", "DL_RANK_BLEND", "DL_SCORE_BONUS")):
        raise ValueError("DLが有効なモデルは時点別学習の検証が必要です。Shadowを停止します")
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = Path(tempfile.mkdtemp(prefix=".shadow-", dir=destination.parent))
    pipeline = importlib.import_module("1_keibayosou_pipeline")
    # 累積レポートもShadow専用領域へ向け、NASの正式予想には書き込まない。
    pipeline.SUCCESS_REPORT = temporary / "success_report.xlsx"
    pipeline.run_pipeline(SRC_EXCEL=str(pre / "input.xlsx"), OUT_EXCEL=str(temporary / "prediction.xlsx"),
                          LEVELS_XL=str(pre / "masters/race_levels.xlsx"),
                          BASE_TIME=str(pre / "masters/場所_馬場_タイム.xlsx"),
                          ODDS_CSV_PATH=str(pre / "odds.csv"), RACEDAY=manifest["day"], ENABLE_SHADOW=False)
    complete_market_columns(pre, temporary / "prediction.xlsx", manifest["day"])
    now = datetime.now(JST)
    if not retrospective and (manifest["mode"] != "prospective" or now >= deadline):
        raise ValueError("Shadow計算が発走前に完了しませんでした。事前実績に算入しません")
    metadata = collect(root, root / "yosou_py")
    metadata["runtime_id"] = hashlib.sha256((metadata["runtime_id"] + adapter_sha).encode()).hexdigest()
    metadata["adapter_sha256"] = adapter_sha
    for name, expected in metadata["code_hashes"].items():
        source = (root / name).resolve()
        if not source.is_relative_to(root) or sha256(source) != expected:
            raise ValueError("ShadowソースのSHA不一致")
        copied = temporary / "source" / name
        copied.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, copied)
    shutil.copy2(Path(__file__), temporary / "prediction_adapter.py")
    write_json(temporary / "runtime.json", metadata)
    write_json(temporary / "manifest.json", {"day": manifest["day"], "mode": manifest["mode"],
               "saved_at": now.isoformat(), "runtime_id": metadata["runtime_id"],
               "prediction_sha256": sha256(temporary / "prediction.xlsx"),
               "runtime_sha256": sha256(temporary / "runtime.json"),
               "adapter_sha256": adapter_sha,
               "production_pre_sha256": sha256(pre / "manifest.json"),
               "legacy_forward_generation": None, "auto_promote": False,
               "comparison_scope": "NAS現行とGitHub mainの全体比較。特徴量修正と重み変更を含む"})
    temporary.rename(destination)
    verify_shadow(destination)
    return destination


if __name__ == "__main__":
    print(predict(Path(sys.argv[1]), retrospective="--retrospective" in sys.argv[2:]))
