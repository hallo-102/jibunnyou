"""検証済みの結果・レーティングを、バックアップと再検査を伴って更新する。"""
from __future__ import annotations

import argparse
from datetime import datetime
from pathlib import Path
import shutil
import sys

from .archive import JST, sha256, write_json


def publish_pair(root: Path, results: Path, levels: Path, day: str, *, validator=None, allow_same_day: bool = False) -> dict:
    datetime.strptime(day, "%Y%m%d")
    today = datetime.now(JST).strftime("%Y%m%d")
    if day > today or (day == today and not allow_same_day):
        raise ValueError("初期マスタの更新には当日以前の確定データだけを使用します")
    if validator is None:
        sys.path.insert(0, str(root))
        from scripts.update_masters import validate_race_levels
        validator = validate_race_levels
    stats = validator(levels, day)
    master = root / "data/master"
    backup = root / "backups/continuous_improvement" / datetime.now(JST).strftime("bootstrap_%Y%m%dT%H%M%S")
    backup.mkdir(parents=True, exist_ok=False)
    sources = {"racedata_results.xlsx": results, "race_levels.xlsx": levels}
    expected = {name: sha256(path) for name,path in sources.items()}
    for name in sources:
        shutil.copy2(master / name, backup / name)
    journal = {"status": "PREPARED", "source_sha256": expected, "backup": str(backup), "max_date": day}
    write_json(backup / "update.json", journal)
    try:
        for name, source in sources.items():
            temporary = master / (name + ".bootstrap.tmp")
            shutil.copy2(source, temporary)
            if sha256(temporary) != expected[name] or sha256(source) != expected[name]:
                raise ValueError("更新中にマスタ原本が変更されました")
            temporary.replace(master / name)
        if validator(master / "race_levels.xlsx", day) != stats:
            raise ValueError("正式パスの検証結果が一時ファイルと不一致です")
        if any(sha256(master / name) != expected[name] for name in sources):
            raise ValueError("正式マスタのSHAが不一致です")
    except Exception:
        # 片方だけが更新された失敗でも、両方を検証前の世代へ戻す。
        for name in sources:
            shutil.copy2(backup / name, master / name)
        write_json(backup / "update.json", {**journal, "status": "ROLLED_BACK"})
        raise
    journal.update(status="COMPLETED", statistics=stats)
    write_json(backup / "update.json", journal)
    return journal


if __name__ == "__main__":
    import json
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--results", type=Path, required=True)
    parser.add_argument("--levels", type=Path, required=True)
    parser.add_argument("--day", required=True)
    args = parser.parse_args()
    print(json.dumps(publish_pair(args.root, args.results, args.levels, args.day), ensure_ascii=False))
