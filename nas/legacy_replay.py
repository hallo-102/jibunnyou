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
from nas.workspace import Workspace


def replay(source: Path, snapshot: Path, output: Path, *, workspace: Path) -> Path:
    """任意のNASコマンドや本番runnerを起動せず、コピーのpipelineだけを実行する。"""
    source, output = separate_paths(source, output)
    snapshot, output = separate_paths(snapshot, output)
    boundary = Workspace(workspace)
    output = boundary.output(output, "legacy", readonly=[source, snapshot])
    manifest = verify_archive(snapshot)
    output.mkdir(parents=True, exist_ok=True)
    work = Path(tempfile.mkdtemp(prefix="legacy_offline_", dir=output))
    # 収集/配信/マスタ更新のスクリプトはコピー対象にも実行対象にも含めない。
    names = [f"1_keibayosou_{name}.py" for name in ("best_import_roi_runner", "config", "course_style", "features", "loaders", "pace", "penalties", "pipeline", "utils")]
    names.append("keiba_runtime.py")
    files = {"app/" + name: source / "app" / name for name in names}
    files["yosou_py/best_feature_weights_20260730.py"] = source / "yosou_py/best_feature_weights_20260730.py"
    hashes = {name: sha256(path) for name, path in files.items()}
    for name, path in files.items():
        destination = work / name
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(path, destination)
        if sha256(destination) != hashes[name] or sha256(path) != hashes[name]:
            raise ValueError("旧NASコードのコピー中改変")
    write_json(work / "source_manifest.json", {"files": hashes, "mode": "offline", "input_manifest_sha256": sha256(snapshot / "manifest.json"),
               "model": {"weight_file": "yosou_py/best_feature_weights_20260730.py",
                         "file_sha256": hashes["yosou_py/best_feature_weights_20260730.py"]}})
    env = os.environ.copy()
    # 本番envを継承させず、旧NASが参照するパスをコピー内へ向ける。
    env = {k: v for k, v in env.items() if not k.startswith("KEIBA_")}
    env.update(KEIBA_BASE_DIR=str(work), KEIBA_WEIGHTS_DIR=str(work / "yosou_py"),
               KEIBA_SCORING_MODEL_VERSION="legacy", PYTHONDONTWRITEBYTECODE="1", PYTHONUNBUFFERED="1")
    env["PYTHONPATH"] = str(ROOT)
    with (work / "run.log").open("w", encoding="utf-8") as log:
        subprocess.run([sys.executable, "-m", "nas.legacy_replay", "--worker", str(work), str(snapshot), manifest["day"], str(boundary.root)],
                       cwd=work, env=env, stdout=log, stderr=subprocess.STDOUT, check=True)
    verify_archive(snapshot)
    return work


def worker(work: Path, snapshot: Path, day: str, workspace: Path) -> None:
    """別プロセスに分離し、旧/新の同名モジュールが混在するのを防ぐ。"""
    import importlib
    work = Workspace(workspace).output(work, "legacy", readonly=[snapshot])
    verify_archive(snapshot)
    os.chdir(work)
    for name in list(os.environ):
        if name.startswith("KEIBA_"):
            del os.environ[name]
    os.environ.update(KEIBA_BASE_DIR=str(work), KEIBA_WEIGHTS_DIR=str(work / "yosou_py"),
                      KEIBA_INPUT_DIR=str(snapshot), KEIBA_MASTER_DIR=str(snapshot / "masters"),
                      KEIBA_ODDS_DIR=str(snapshot), KEIBA_OUTPUT_DIR=str(work), KEIBA_SCORING_MODEL_VERSION="legacy")
    code = __import__("json").loads((work / "source_manifest.json").read_text(encoding="utf-8"))
    for name, expected in code["files"].items():
        path = (work / name).resolve()
        if not path.is_relative_to(work) or sha256(path) != expected:
            raise ValueError("コピーした旧NASソースのSHA不一致")
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
    from continuous_improvement.runtime_metadata import collect
    from model_registry import normalized_weights_sha256
    metadata = collect(work / "app", work / "yosou_py")
    weights = {**metadata["weights"]["common"], **{tuple(k.split("|")): v for k, v in metadata["weights"]["surface"].items()}}
    metadata.update(input_manifest_sha256=code["input_manifest_sha256"], prediction_sha256=sha256(work / "prediction.xlsx"),
                    model_identity={**code["model"], "effective_sha256": normalized_weights_sha256(weights)})
    write_json(work / "runtime.json", metadata)
    write_json(work / "trace_manifest.json", {"raw_scores_sha256": sha256(work / "raw_scores.json"),
               "prediction_sha256": sha256(work / "prediction.xlsx"), "runtime_sha256": sha256(work / "runtime.json"),
               "source_manifest_sha256": sha256(work / "source_manifest.json"), "scope": "offline"})


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--worker":
        worker(Path(sys.argv[2]), Path(sys.argv[3]), sys.argv[4], Path(sys.argv[5]))
    else:
        parser = argparse.ArgumentParser(description=__doc__)
        parser.add_argument("--source", type=Path, required=True)
        parser.add_argument("--snapshot", type=Path, required=True)
        parser.add_argument("--output", type=Path, required=True)
        parser.add_argument("--workspace", type=Path, required=True)
        args = parser.parse_args()
        print(replay(args.source, args.snapshot, args.output, workspace=args.workspace))
