"""既存NASデータの指定日だけを読み、新規の固定入力を作成する。"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path
import shutil

from continuous_improvement.archive import sha256, verify_archive, write_json
from nas.candidate import separate_paths
from nas.runtime import validate_raceday


def prepare(root: Path, destination: Path, day: str, *, calendar: Path | None = None) -> Path:
    """コピー元とコピー先のSHAを照合し、既存snapshotは上書きしない。"""
    day = validate_raceday(day)
    root, destination = separate_paths(root, destination)
    files = {"input.xlsx": root / "data/input" / f"馬の競走成績_{day}.xlsx",
             "odds.csv": root / "data/ozzu_csv" / f"OZZU_{day}.csv"}
    for name in ("race_levels.xlsx", "場所_馬場_タイム.xlsx"):
        files["masters/" + name] = root / "data/master" / name
    baseline = root / "data/output" / f"馬の競走成績_with_feat_{day}.xlsx"
    if baseline.is_file():
        files["baseline.xlsx"] = baseline
    if calendar is not None:
        files["calendar.txt"] = calendar
    # 先に全ファイルを確認してから新規ディレクトリを確保する。
    hashes = {name: sha256(path) for name, path in files.items()}
    destination.mkdir(parents=True, exist_ok=False)
    for name, source in files.items():
        target = destination / name
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, target)
        if sha256(target) != hashes[name] or sha256(source) != hashes[name]:
            raise ValueError(f"コピー中の原本変更: {name}")
    write_json(destination / "manifest.json", {"schema_version": 1, "day": day, "mode": "offline",
               "saved_at": datetime.now(timezone.utc).isoformat(), "files": hashes,
               "legacy_forward_generation": None, "baseline_provenance": "historical output; same masters/odds unverified"})
    verify_archive(destination)
    return destination


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--destination", type=Path, required=True)
    parser.add_argument("--day", required=True)
    parser.add_argument("--calendar", type=Path)
    args = parser.parse_args()
    print(prepare(args.root, args.destination, args.day, calendar=args.calendar))
