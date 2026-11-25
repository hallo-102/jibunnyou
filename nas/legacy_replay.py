"""取得済みNAS旧コードを新規領域へコピーし、同一固定入力でoffline再生する。"""
from __future__ import annotations

import argparse
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile

from continuous_improvement.archive import sha256, verify_archive, write_json
from nas.candidate import ROOT, separate_paths


def replay(source: Path, snapshot: Path, output: Path) -> Path:
    """任意のNASコマンドや本番runnerを起動せず、コピーのpipelineだけを実行する。"""
    source, output = separate_paths(source, output)
    snapshot, output = separate_paths(snapshot, output)
    manifest = verify_archive(snapshot)
    output.mkdir(parents=True, exist_ok=True)
    work = Path(tempfile.mkdtemp(prefix="legacy_offline_", dir=output))
    files = {"app/" + p.name: p for p in (source / "app").glob("*.py")}
    files["yosou_py/best_feature_weights_20260730.py"] = source / "yosou_py/best_feature_weights_20260730.py"
    hashes = {name: sha256(path) for name, path in files.items()}
    for name, path in files.items():
        destination = work / name
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(path, destination)
        if sha256(destination) != hashes[name] or sha256(path) != hashes[name]:
            raise ValueError("旧NASコードのコピー中改変")
    write_json(work / "source_manifest.json", {"files": hashes, "mode": "offline", "input_manifest_sha256": sha256(snapshot / "manifest.json")})
    env = os.environ.copy()
    # 本番envを継承させず、旧NASが参照するパスをコピー内へ向ける。
    env = {k: v for k, v in env.items() if not k.startswith("KEIBA_")}
    env.update(KEIBA_BASE_DIR=str(work), KEIBA_WEIGHTS_DIR=str(work / "yosou_py"),
               KEIBA_SCORING_MODEL_VERSION="legacy", PYTHONDONTWRITEBYTECODE="1", PYTHONUNBUFFERED="1")
    with (work / "run.log").open("w", encoding="utf-8") as log:
        subprocess.run([sys.executable, "-m", "nas.legacy_replay", "--worker", str(work), str(snapshot), manifest["day"]],
                       cwd=ROOT, env=env, stdout=log, stderr=subprocess.STDOUT, check=True)
    verify_archive(snapshot)
    return work


def worker(work: Path, snapshot: Path, day: str) -> None:
    """別プロセスに分離し、旧/新の同名モジュールが混在するのを防ぐ。"""
    import importlib
    sys.path.insert(0, str(work / "app"))
    pipeline = importlib.import_module("1_keibayosou_pipeline")
    pipeline.SUCCESS_REPORT = work / "success_report.xlsx"
    compute = pipeline.compute_scores_with_pipeline_logic

    def capture(*args, **kwargs):
        scored = compute(*args, **kwargs)
        diagnostic = scored.copy()
        # 旧実装と同じnormalize_scoreを丸め前で採取し、元のrank/scoreを変更しない。
        diagnostic["score_raw"] = diagnostic.groupby("rid_str")["total"].transform(pipeline.normalize_score)
        diagnostic.to_json(work / "raw_scores.json", orient="records", force_ascii=False, double_precision=15)
        return scored

    pipeline.compute_scores_with_pipeline_logic = capture
    # PR #10と同じ市場補完のみ再利用する。旧NASのdense順位には触れない。
    from continuous_improvement.shadow import complete_market_columns
    pipeline.run_pipeline(SRC_EXCEL=str(snapshot / "input.xlsx"), OUT_EXCEL=str(work / "prediction.xlsx"),
                          LEVELS_XL=str(snapshot / "masters/race_levels.xlsx"),
                          BASE_TIME=str(snapshot / "masters/場所_馬場_タイム.xlsx"),
                          ODDS_CSV_PATH=str(snapshot / "odds.csv"), RACEDAY=day)
    complete_market_columns(snapshot, work / "prediction.xlsx", day)
    write_json(work / "trace_manifest.json", {"raw_scores_sha256": sha256(work / "raw_scores.json"),
               "prediction_sha256": sha256(work / "prediction.xlsx"), "scope": "offline"})


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--worker":
        worker(Path(sys.argv[2]), Path(sys.argv[3]), sys.argv[4])
    else:
        parser = argparse.ArgumentParser(description=__doc__)
        parser.add_argument("--source", type=Path, required=True)
        parser.add_argument("--snapshot", type=Path, required=True)
        parser.add_argument("--output", type=Path, required=True)
        args = parser.parse_args()
        print(replay(args.source, args.snapshot, args.output))
