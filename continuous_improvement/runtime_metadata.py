"""実行中の予想コード自身から実効重みを取得する。秘密設定は読まない。"""
from __future__ import annotations

import hashlib
import importlib
import json
from pathlib import Path
import sys


def collect(app_dir: Path, weights_dir: Path) -> dict:
    # 別プロセスで実行し、最新mainとNAS旧モジュールを同じPythonへ混在させない。
    sys.path.insert(0, str(app_dir.resolve()))
    config = importlib.import_module("1_keibayosou_config")
    common = {str(k): dict(v) for k, v in config.FEATURE_WEIGHTS.items()}
    surface = {"|".join(k): dict(v) for k, v in config.FEATURE_WEIGHTS_BY_PLACE_SURFACE.items()}
    weights = {"common": common, "surface": surface}
    base = app_dir.parent if app_dir.name == "app" else app_dir
    files = sorted(app_dir.glob("*.py")) + sorted(weights_dir.glob("best_feature_weights_*.py"))
    files += [p for p in (base / "config/production_model.json", base / "config/shadow_model.json") if p.exists()]
    hashes = {p.relative_to(base).as_posix(): hashlib.sha256(p.read_bytes()).hexdigest() for p in files}
    identity = hashlib.sha256(json.dumps({"weights": weights, "files": hashes}, sort_keys=True, ensure_ascii=False).encode()).hexdigest()
    return {"schema_version": 1, "runtime_id": identity, "features": list(config.FEAT_COLS),
            "feature_names": config.JAPANESE_FEATURE_NAMES, "weights": weights,
            "code_hashes": hashes, "scoring_model_version": config.SCORING_MODEL_VERSION,
            "auto_promote": False}


if __name__ == "__main__":
    # 呼出元が標準出力末尾のマーカーを読み、設定import時の診断ログと分離する。
    print("RUNTIME_METADATA=" + json.dumps(collect(Path(sys.argv[1]), Path(sys.argv[2])), ensure_ascii=False))
