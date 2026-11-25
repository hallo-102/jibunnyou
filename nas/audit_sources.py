"""3者のバイトSHA/LF正規化SHAをソースだけから収集する。"""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import subprocess

COMMON = [f"1_keibayosou_{name}.py" for name in (
    "best_import_roi_runner", "config", "course_style", "features", "loaders", "pace", "penalties", "pipeline", "ranking", "utils")]
COMMON += ["model_registry.py", "config/production_model.json", "config/forward_validation.json", "yosou_py/best_feature_weights_20260817.py"]
NAS_ONLY = ["app/keiba_runtime.py", "scripts/run_daily_pipeline.py", "scripts/run_scheduler.py", "scripts/jra_raceday.py",
            "scripts/validate_excel.py", "scripts/finalize_unsupported_races.py", "scripts/upload_rclone.py", "scripts/update_masters.py",
            "compose.yaml", "Dockerfile", "requirements.txt", "yosou_py/best_feature_weights_20260730.py"]


def digest(data: bytes | None) -> dict | None:
    """存在しないファイルは、推測ハッシュを作らずnullで残す。"""
    if data is None:
        return None
    return {"bytes": hashlib.sha256(data).hexdigest(),
            "lf_normalized": hashlib.sha256(data.replace(b"\r\n", b"\n").replace(b"\r", b"\n")).hexdigest()}


def inventory(root: Path, windows: Path, nas: Path, ref: str) -> list[dict]:
    rows = []
    for name in COMMON:
        candidate = root / name
        old = nas / ("app/" + name if "/" not in name else name)
        win = windows / name
        try:
            main = subprocess.check_output(["git", "show", ref + ":" + name], cwd=root, stderr=subprocess.DEVNULL)
        except subprocess.CalledProcessError:
            main = None
        rows.append({"path": name, "main": digest(main), "windows": digest(win.read_bytes() if win.is_file() else None),
                     "nas_path": old.relative_to(nas).as_posix(), "nas_static_copy": digest(old.read_bytes() if old.is_file() else None),
                     "candidate": digest(candidate.read_bytes() if candidate.is_file() else None),
                     "adopted": "GitHub main", "reason": "共通予想ロジックを二重保守しない"})
    for name in NAS_ONLY:
        path = nas / name
        rows.append({"nas_path": name, "nas_static_copy": digest(path.read_bytes() if path.is_file() else None),
                     "adopted": "NAS helper subset; existing production orchestration unchanged",
                     "reason": "隔離実行に必要な純粋な運用契約のみ再利用。収集/配信/更新/定期実行は起動しない"})
    return rows


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--windows", type=Path, required=True)
    parser.add_argument("--nas", type=Path, required=True)
    parser.add_argument("--ref", default="main")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    args.output.write_text(json.dumps(inventory(root, args.windows, args.nas, args.ref), ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
