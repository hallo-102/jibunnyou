"""日別入力・全特徴量・予想・実効重みを追記型で固定保存する。"""
from __future__ import annotations

from datetime import datetime
import hashlib
import json
from pathlib import Path
import shutil
import tempfile
from zoneinfo import ZoneInfo

import pandas as pd

JST = ZoneInfo("Asia/Tokyo")


def sha256(path: Path) -> str:
    # Excelマスタも一定のメモリ量で検証する。
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def write_json(path: Path, payload: dict) -> None:
    # 状態ファイルは置換可能だが、固定済みpreにはこの関数を使わない。
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(payload, ensure_ascii=False, indent=2, allow_nan=False), encoding="utf-8")
    temporary.replace(path)


def verify_archive(folder: Path) -> dict:
    manifest = json.loads((folder / "manifest.json").read_text(encoding="utf-8"))
    for name, expected in manifest["files"].items():
        # マニフェスト中の相対パスが固定フォルダを逸脱する場合も拒否する。
        path = (folder / name).resolve()
        if not path.is_relative_to(folder.resolve()) or sha256(path) != expected:
            raise ValueError(f"固定保存のSHA不一致: {name}")
    return manifest


def freeze(root: Path, day: str, metadata: dict, *, now: datetime | None = None,
           retrospective: bool = False) -> Path:
    datetime.strptime(day, "%Y%m%d")
    now = now or datetime.now(JST)
    if now.tzinfo is None:
        raise ValueError("保存時刻にはタイムゾーンが必要です")
    mode = "retrospective" if retrospective else "prospective"
    destination = root / "data/output/continuous_improvement" / mode / day / "pre"
    if destination.exists():
        # 再起動時も一度固定した内容を置き換えず、改変を検知して停止する。
        verify_archive(destination)
        return destination
    output = root / "data/output" / f"馬の競走成績_with_feat_{day}.xlsx"
    source = root / "data/input" / f"馬の競走成績_{day}.xlsx"
    entries = pd.read_excel(source, sheet_name="今走レース情報")
    if entries.empty or entries["レースID"].isna().any() or entries.duplicated(["レースID", "馬番"]).any():
        raise ValueError("入力のレースID・馬番が不正です")
    departures = pd.to_datetime(day + " " + entries["発走時刻"].astype(str), format="%Y%m%d %H:%M", errors="coerce")
    if departures.isna().any():
        raise ValueError("発走時刻の欠損・不正を検知しました")
    if not retrospective and (now.astimezone(JST).strftime("%Y%m%d") != day or now >= departures.min().to_pydatetime().replace(tzinfo=JST)):
        raise ValueError("発走後・過去日の予想を事前実績として保存できません")
    if not retrospective:
        results_master = root / "data/master/racedata_results.xlsx"
        if results_master.exists():
            with pd.ExcelFile(results_master) as excel:
                dates = [name for name in excel.sheet_names if len(name) == 8 and name.isdigit()]
            if any(value >= day for value in dates):
                raise ValueError("対象日以降の結果を含むマスタは事前保存に使えません")
        ready = json.loads((root / "data/output/status" / f"READY_{day}.json").read_text(encoding="utf-8"))
        if ready.get("status") != "READY" or ready.get("sha256") != sha256(output):
            raise ValueError("READYと予想ExcelのSHAが不一致です")
    files = {"prediction.xlsx": output, "input.xlsx": source,
             "odds.csv": root / "data/ozzu_csv" / f"OZZU_{day}.csv"}
    for name, expected in metadata["code_hashes"].items():
        code_path = (root / name).resolve()
        if not code_path.is_relative_to(root.resolve()) or sha256(code_path) != expected:
            raise ValueError(f"実行コードとメタデータが不一致です: {name}")
        files["source/" + name] = code_path
    for path in (root / "data/master").glob("*"):
        if path.suffix in {".xlsx", ".csv"}:
            files["masters/" + path.name] = path
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = Path(tempfile.mkdtemp(prefix=".pre-", dir=destination.parent))
    try:
        hashes = {}
        for name, source_path in files.items():
            before = sha256(source_path)
            target = temporary / name
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source_path, target)
            if sha256(target) != before or sha256(source_path) != before:
                raise ValueError(f"保存中に原本が変更されました: {source_path.name}")
            hashes[name] = before
        write_json(temporary / "runtime.json", metadata)
        hashes["runtime.json"] = sha256(temporary / "runtime.json")
        write_json(temporary / "manifest.json", {"schema_version": 1, "day": day, "mode": mode,
                   "saved_at": now.isoformat(), "runtime_id": metadata["runtime_id"],
                   "legacy_forward_generation": None, "files": hashes})
        # 同時起動でも既存のpreディレクトリを上書きできないrenameを使う。
        temporary.rename(destination)
    finally:
        if temporary.exists():
            if not temporary.resolve().is_relative_to(destination.parent.resolve()):
                raise ValueError("一時フォルダが保存先を逸脱しています")
            shutil.rmtree(temporary)
    verify_archive(destination)
    return destination
