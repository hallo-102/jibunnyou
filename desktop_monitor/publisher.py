"""NAS上で新しい名前の観測ファイルを発行し、SMBの古い内容の再利用を避ける。"""
from __future__ import annotations

import argparse
from datetime import datetime
import json
import logging
import os
from pathlib import Path
import time

from .state import JST, collect


def publish(root: Path, output: Path) -> Path:
    now = datetime.now(JST)
    # NASのローカルパスから観測する。業務ファイルには書き込まない。
    snapshot = collect(str(root), now.strftime("%Y%m%d"))
    snapshot["snapshot_published_at"] = now.isoformat()
    output.mkdir(parents=True, exist_ok=True)
    name = now.strftime("snapshot_%Y%m%dT%H%M%S_") + str(time.time_ns()) + ".json"
    target = output / name
    temporary = output / (name + ".tmp")
    temporary.write_text(json.dumps(snapshot, ensure_ascii=False), encoding="utf-8")
    temporary.chmod(0o644)
    temporary.replace(target)
    # 同じ名前を上書きしない。ディレクトリ一覧が遅れても読めるよう2分以上保持する。
    files = sorted(output.glob("snapshot_*.json"))
    for expired in files[:-36]:
        expired.unlink(missing_ok=True)
    return target


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, default=Path("/workspace"))
    parser.add_argument("--output", type=Path, default=Path("/observations"))
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)
    while True:
        try:
            publish(args.root, args.output)
        except Exception:
            logging.exception("monitor_observation_failed")
            if args.once:
                raise
        if args.once:
            return
        time.sleep(5)


if __name__ == "__main__":
    main()
