"""composeのrw bind先をホスト実パスで検査してから、一回だけ起動する。"""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import re
import subprocess

from continuous_improvement.archive import verify_archive
from nas.workspace import CODE_ROOT, MARKER, Workspace, overlaps


def checked_environment(workspace: Path, snapshot: Path, output: Path, production_root: Path, commit: str) -> dict:
    """外部のCANDIDATE_*値は使わず、検査済みパスからcompose変数を組み立てる。"""
    boundary = Workspace(workspace)
    production_root = production_root.resolve(strict=True)
    if str(boundary.root) != boundary.policy["host_root"] or str(production_root) != boundary.policy["production_root"]:
        raise ValueError("ホストworkspace/本番rootが初期化時の実パスと一致しません")
    if overlaps(boundary.root, production_root):
        raise ValueError("本番rootをrwマウントできません")
    snapshot = snapshot.resolve(strict=True)
    if overlaps(snapshot, output):
        raise ValueError("固定入力とrw出力が重複しています")
    output = boundary.output(output, "runs", readonly=[snapshot, production_root])
    if not re.fullmatch(r"[0-9a-f]{40}", commit):
        raise ValueError("レビュー済み40桁commitが必要です")
    verify_archive(snapshot)
    return {"CANDIDATE_WORKSPACE_MARKER": str(boundary.root / MARKER), "CANDIDATE_SNAPSHOT": str(snapshot),
            "CANDIDATE_OUTPUT": str(output), "CANDIDATE_COMMIT": commit}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workspace", type=Path, required=True)
    parser.add_argument("--snapshot", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--production-root", type=Path, required=True)
    parser.add_argument("--commit", required=True)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--weight-control", action="store_true")
    args = parser.parse_args()
    checked = checked_environment(args.workspace, args.snapshot, args.output, args.production_root, args.commit)
    if args.dry_run:
        print(json.dumps(checked, indent=2))
    else:
        head = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=CODE_ROOT, text=True).strip()
        dirty = subprocess.check_output(["git", "status", "--porcelain"], cwd=CODE_ROOT, text=True).strip()
        if head != args.commit or dirty:
            raise ValueError("実行ソースはレビューcommitのclean checkoutが必要です")
        Path(checked["CANDIDATE_OUTPUT"]).mkdir(parents=True, exist_ok=True)
        env = {**os.environ, **checked}
        # ホスト境界検査より前にDockerを呼ばない。独立projectだけを対象にする。
        command = ["docker", "compose", "-p", "keiba-issue16-candidate", "-f", str(CODE_ROOT / "nas/compose.yaml"),
                   "run", "--rm", "--build", "candidate"]
        if args.weight_control:
            command.extend(["--workspace", "/candidate", "--snapshot", "/snapshot", "--output", "/candidate/runs", "--weight-control"])
        subprocess.run(command, cwd=CODE_ROOT, env=env, check=True)
