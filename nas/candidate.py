"""固定入力から共通予想を実行するoffline専用アダプター。"""
from __future__ import annotations

import argparse
import hashlib
from contextlib import redirect_stderr, redirect_stdout
from dataclasses import asdict
from datetime import datetime, timezone
import importlib
import json
from pathlib import Path
import platform
import shutil
import subprocess
import tempfile

import numpy as np
import pandas as pd

from continuous_improvement.archive import sha256, verify_archive, write_json
from continuous_improvement.runtime_metadata import collect
from continuous_improvement.shadow import complete_market_columns
from model_registry import load_model_spec
from nas.runtime import validate_raceday
from nas.scripts.finalize_unsupported_races import complete_unsupported_races
from nas.scripts.jra_raceday import check_jra_raceday
from nas.scripts.validate_excel import validate_excel
from nas.workspace import Workspace

ROOT = Path(__file__).resolve().parents[1]


def separate_paths(snapshot: Path, output: Path) -> tuple[Path, Path]:
    """入出力の包含・シンボリックリンクによる原本書込を拒否する。"""
    snapshot, output = snapshot.resolve(), output.resolve()
    if output.is_relative_to(snapshot) or snapshot.is_relative_to(output):
        raise ValueError("snapshotとoutputは互いに独立したディレクトリが必要です")
    if output == ROOT or ROOT.is_relative_to(output):
        raise ValueError("コードディレクトリを出力先にできません")
    return snapshot, output


def verify_candidate() -> dict:
    """明示候補設定と既存registryの実体・実効SHAを両方検証する。"""
    policy = json.loads((ROOT / "nas/candidate.json").read_text(encoding="utf-8"))
    if policy["scope"] != "offline" or policy["auto_promote"] is not False or policy["upload_mode"] != "dry-run":
        raise ValueError("候補はoffline/dry-run/自動採用禁止に限定します")
    spec = load_model_spec(ROOT, policy["model_config"], expected_role="production")
    for field in ("model_name", "file_sha256", "effective_sha256"):
        if getattr(spec, field) != policy[field]:
            raise ValueError(f"候補設定と共通モデルの不一致: {field}")
    config = importlib.import_module("1_keibayosou_config")
    if config.PRODUCTION_MODEL_SPEC != spec:
        raise ValueError("共通configと候補指定のモデルが異なります")
    # import時の既存実効SHA検証を再利用し、DL有効時は別途時点検証を要求する。
    if any(float(getattr(config, key)) != 0 for key in ("DL_PROB_BLEND", "DL_RANK_BLEND", "DL_SCORE_BONUS")):
        raise ValueError("DL有効モデルの時点別学習は未承認です")
    return policy


def validate_population(source: Path, prediction: Path) -> dict:
    """予想対象レースは入力全馬と一致し、レース内の一部欠落を許容しない。"""
    from continuous_improvement.review import key
    ranking = importlib.import_module("1_keibayosou_ranking")
    entries = pd.read_excel(source, sheet_name="今走レース情報")
    target = pd.read_excel(prediction, sheet_name="TARGET")
    bets = pd.read_excel(prediction, sheet_name="買い目_レース別1行")
    required_bet = {"レースID", "予想状態", "予想可否"}
    if not required_bet.issubset(bets):
        raise ValueError("買い目の分類列が不足しています")
    bets["rid_str"] = bets["レースID"].map(key)
    if bets["rid_str"].duplicated().any():
        raise ValueError("買い目の分類レースが重複しています")
    entries["rid_str"] = entries["レースID"].map(key)
    target["rid_str"] = target["rid_str"].map(key)
    for frame in (entries, target):
        frame["馬番"] = pd.to_numeric(frame["馬番"], errors="raise")
        if frame[["rid_str", "馬番"]].isna().any().any() or frame.duplicated(["rid_str", "馬番"]).any():
            raise ValueError("入力/予想の馬キー欠損・重複")
    if not set(target.rid_str).issubset(set(entries.rid_str)):
        raise ValueError("入力にないレースを予想しました")
    predicted = bets["予想状態"].eq("PREDICTED") & bets["予想可否"].eq("数値予想済み")
    excluded = bets["予想状態"].eq("NO_PREDICTION") & bets["予想可否"].eq("予想対象外")
    if not (predicted | excluded).all() or set(bets.rid_str) != set(entries.rid_str):
        raise ValueError("買い目の分類と入力全レースが一致しません")
    if set(target.rid_str) != set(bets.loc[predicted, "rid_str"]):
        raise ValueError("TARGET予想レースと数値予想済み分類が一致しません")
    excluded_ids = set(bets.loc[excluded, "rid_str"])
    # 正式検査に加え、ROIシートにも対象外の固定券を残さない。
    from nas.compare import fixed_tickets
    with pd.ExcelFile(prediction) as book:
        roi = pd.read_excel(book, sheet_name="回収率重視_買い目候補") if "回収率重視_買い目候補" in book.sheet_names else pd.DataFrame()
    if "レースID" in roi:
        roi["レースID"] = roi["レースID"].map(key)
        for rid in excluded_ids:
            if any(item["tickets"] or item["purchase_yen"] for item in fixed_tickets(roi, rid).values()):
                raise ValueError("対象外レースに購入券が残っています")
    for column in ("rank", "score", "1位馬番", "2位馬番", "3位馬番", "4位馬番", "5位馬番"):
        if column in bets and bets.loc[excluded, column].notna().any():
            raise ValueError("対象外レースに予想順位/馬番が残っています")
    for column in [c for c in bets if c == "購入金額" or c.endswith("_金額")]:
        if pd.to_numeric(bets.loc[excluded, column], errors="coerce").fillna(0).ne(0).any():
            raise ValueError("対象外レースに購入金額が残っています")
    for rid, race in target.groupby("rid_str"):
        if set(race["馬番"]) != set(entries.loc[entries.rid_str.eq(rid), "馬番"]):
            raise ValueError(f"予想レースの対象頭数不一致（取消も暗黙除去しない）: {rid}")
    if not target.empty:
        if not np.isfinite(pd.to_numeric(target["score_raw"], errors="coerce")).all():
            raise ValueError("丸め前scoreが欠損または非有限です")
        ranking.validate_prediction_ranks(target, raw_score_col="score_raw")
        config = importlib.import_module("1_keibayosou_config")
        for column in ("risk_score", "extra_penalty", "data_confidence"):
            label = config.JAPANESE_FEATURE_NAMES.get(column, column)
            if column not in target and label in target:
                target[column] = target[label]
            if column not in target:
                raise ValueError(f"順位再計算の同点キーがありません: {column}")
        expected = ranking.create_unique_rank_series(target, "rid_str", "score_raw", "risk_score", "extra_penalty", "data_confidence")
        if not expected.eq(pd.to_numeric(target["rank"], errors="coerce")).all():
            raise ValueError("共通順位の再計算結果と保存順位が一致しません")
    return {"input_horses": len(entries), "predicted_horses": len(target),
            "excluded_races": sorted(set(entries.rid_str) - set(target.rid_str))}


def forward_audit() -> dict:
    """既存の凍結規則を変更せず、失敗も候補メタデータへ記録する。"""
    from shadow_validation import load_forward_validation_policy
    payload = json.loads((ROOT / "config/forward_validation.json").read_text(encoding="utf-8"))
    active = payload["active_generation"]
    generation = payload["generations"][active]
    try:
        load_forward_validation_policy()
        status, error = "PASS", None
    except Exception as exc:
        status, error = "BLOCKED", str(exc)
    return {"active_generation": active, "start_date": generation["start_date"],
            "holdout": generation["optimizer_holdout_start_date"], "status": status, "error": error,
            "candidate_scope": "offline", "legacy_forward_generation": None}


def verify_completed(folder: Path) -> dict:
    """成功はCOMPLETED・READY・公開manifest/Excel SHAの全一致で判断する。"""
    status = json.loads((folder / "STATUS.json").read_text(encoding="utf-8"))
    ready = json.loads((folder / "READY.json").read_text(encoding="utf-8"))
    manifest = verify_archive(folder)
    if status.get("status") != "COMPLETED" or ready.get("status") != "READY":
        raise ValueError("候補の成功状態が揃っていません")
    if ready.get("sha256") != manifest["files"]["prediction.xlsx"] or ready.get("manifest_sha256") != sha256(folder / "manifest.json"):
        raise ValueError("READYと公開ファイルのSHAが一致しません")
    return manifest


def run(snapshot: Path, output: Path, *, workspace: Path, calendar: Path | None = None, weight_control: bool = False) -> Path:
    """新規runにだけ保存し、成功検査後に候補READYを発行する。"""
    snapshot, output = separate_paths(snapshot, output)
    boundary = Workspace(workspace)
    output = boundary.output(output, "runs", readonly=[snapshot])
    manifest = verify_archive(snapshot)
    manifest_sha = sha256(snapshot / "manifest.json")
    day = validate_raceday(manifest["day"])
    if manifest["mode"] not in {"offline", "retrospective", "prospective"}:
        raise ValueError("未知の入力区分")
    required = {"input.xlsx", "odds.csv", "masters/race_levels.xlsx", "masters/場所_馬場_タイム.xlsx"}
    if not required.issubset(manifest["files"]):
        raise ValueError("固定SHAがない入力・マスタは使用できません")
    if calendar is None and (snapshot / "calendar.txt").exists():
        calendar = snapshot / "calendar.txt"
    if calendar is not None and calendar.resolve() != (snapshot / "calendar.txt").resolve():
        raise ValueError("開催日カレンダーもsnapshot内に固定してください")
    if calendar is not None and "calendar.txt" not in manifest["files"]:
        raise ValueError("カレンダーの固定SHAがありません")
    output.mkdir(parents=True, exist_ok=True)
    run_dir = Path(tempfile.mkdtemp(prefix=f"offline_{day}_", dir=output))
    status = {"day": day, "scope": "offline", "legacy_forward_generation": None, "auto_promote": False}
    write_json(run_dir / "STATUS.json", {**status, "status": "RUNNING"})
    try:
        with (run_dir / "run.log").open("w", encoding="utf-8") as log, redirect_stdout(log), redirect_stderr(log):
            policy = verify_candidate()
            if calendar is not None and not check_jra_raceday(day, calendar).is_raceday:
                write_json(run_dir / "STATUS.json", {**status, "status": "NO_RACE"})
                return run_dir
            metadata = collect(ROOT, ROOT / "yosou_py")
            adapter_hashes = {p.relative_to(ROOT).as_posix(): sha256(p) for p in sorted((ROOT / "nas").rglob("*"))
                              if p.is_file() and p.suffix in {".py", ".json", ".yaml", ".txt"}}
            pipeline = importlib.import_module("1_keibayosou_pipeline")
            from contextlib import nullcontext
            from nas.weight_control import capture_control
            # PR #10と同じpipeline呼出し。累積レポートもrun内に隔離する。
            pipeline.SUCCESS_REPORT = run_dir / "success_report.xlsx"
            prediction = run_dir / f"馬の競走成績_with_feat_{day}.xlsx"
            with capture_control(pipeline, run_dir) if weight_control else nullcontext():
                pipeline.run_pipeline(SRC_EXCEL=str(snapshot / "input.xlsx"), OUT_EXCEL=str(prediction),
                                  LEVELS_XL=str(snapshot / "masters/race_levels.xlsx"),
                                  BASE_TIME=str(snapshot / "masters/場所_馬場_タイム.xlsx"),
                                  ODDS_CSV_PATH=str(snapshot / "odds.csv"), RACEDAY=day, ENABLE_SHADOW=False)
            complete_market_columns(snapshot, prediction, day)
            completion = complete_unsupported_races(prediction, snapshot / "input.xlsx", day)
            population = validate_population(snapshot / "input.xlsx", prediction)
            validation = validate_excel(prediction, day, source_input=snapshot / "input.xlsx")
            # 実行途中のソース差し替えを、実行後のSHAだけで正当化しない。
            if any(sha256(ROOT / name) != expected for name, expected in {**metadata["code_hashes"], **adapter_hashes}.items()):
                raise ValueError("実行中に共通コード/アダプターが変更されました")
            metadata.update(candidate_policy=policy, scope="offline", legacy_forward_generation=None,
                            population=population, completion=asdict(completion), excel_validation=asdict(validation),
                            forward_validation=forward_audit(), python=platform.python_version(), platform=platform.platform(),
                            input_manifest_sha256=manifest_sha,
                            ranking_order=["score_raw desc", "risk_score desc", "extra_penalty asc", "data_confidence desc", "馬番 asc"],
                            betting_code_sha256=sha256(ROOT / "1_keibayosou_pipeline.py"),
                            adapter_hashes=adapter_hashes)
            try:
                revision = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, stderr=subprocess.DEVNULL).decode().strip()
            except (OSError, subprocess.CalledProcessError):
                import os
                revision = os.getenv("KEIBA_CANDIDATE_COMMIT", "unknown")
            metadata["source_commit"] = revision
            metadata["runtime_id"] = hashlib.sha256(json.dumps({"core": metadata["runtime_id"], "adapters": metadata["adapter_hashes"]}, sort_keys=True).encode()).hexdigest()
            write_json(run_dir / "runtime.json", metadata)
            shutil.copy2(prediction, run_dir / "prediction.xlsx")
            shutil.copy2(snapshot / "input.xlsx", run_dir / "input.xlsx")
            published = run_dir / "prediction.xlsx"
            if sha256(published) != sha256(prediction):
                raise ValueError("公開対象の予想コピーSHAが元出力と一致しません")
            # PR #11の回顧APIが読めるmanifestを作る。常に過去再計算区分とする。
            published_files = {"prediction.xlsx": sha256(published), "runtime.json": sha256(run_dir / "runtime.json"),
                               "input.xlsx": sha256(run_dir / "input.xlsx")}
            if (run_dir / "weight_control.json").exists():
                published_files["weight_control.json"] = sha256(run_dir / "weight_control.json")
            write_json(run_dir / "manifest.json", {"schema_version": 1, "day": day, "mode": "retrospective",
                       "runtime_id": metadata["runtime_id"], "legacy_forward_generation": None,
                       "files": published_files})
            verify_archive(run_dir)
            verify_archive(snapshot)
            if sha256(snapshot / "manifest.json") != manifest_sha:
                raise ValueError("実行中に入力manifestが変更されました")
            write_json(run_dir / "sync_plan.json", {"mode": "dry-run", "destination": None,
                       "files": ["prediction.xlsx", "runtime.json", "manifest.json"], "executed": False})
            write_json(run_dir / "STATUS.json", {**status, "status": "COMPLETED"})
            # 公開対象そのものを検証した後、成功マーカーを最後に書く。
            write_json(run_dir / "READY.json", {**status, "status": "READY", "sha256": sha256(published),
                       "manifest_sha256": sha256(run_dir / "manifest.json"),
                       "completed_at": datetime.now(timezone.utc).isoformat()})
            verify_completed(run_dir)
    except Exception as exc:
        (run_dir / "READY.json").unlink(missing_ok=True)
        try:
            write_json(run_dir / "STATUS.json", {**status, "status": "FAILED", "error": str(exc)})
        except OSError:
            pass  # 状態保存自体が故障していても元の例外を保持し、READYは残さない。
        raise
    return run_dir


def main() -> None:
    """スケジューラーや外部配信を起動しない明示的な一回実行。"""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--calendar", type=Path)
    parser.add_argument("--workspace", type=Path, required=True)
    parser.add_argument("--weight-control", action="store_true")
    args = parser.parse_args()
    print(run(args.snapshot, args.output, workspace=args.workspace, calendar=args.calendar, weight_control=args.weight_control))


if __name__ == "__main__":
    main()
