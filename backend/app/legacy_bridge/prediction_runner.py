from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import time
from datetime import date, datetime
from pathlib import Path
from typing import Any

import pandas as pd
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.core.config import get_settings
from app.core.logging import redact_text
from app.db.models import JobRun, PredictionResult, PredictionRun, RaceEntry
from app.legacy_bridge.excel_importer import find_race_workbook
from app.legacy_bridge.normalization import (
    file_sha256,
    is_blank,
    normalize_horse_name,
    normalize_race_id,
    pick_value,
    row_to_jsonable,
    safe_float,
    safe_int,
    safe_str,
)
from app.schemas.api import PredictionImportSummary
from app.services.history import record_job_log, register_artifact
from app.services.prediction_workspace import (
    PredictionWorkspace,
    prepare_prediction_workspace,
    register_prediction_versions,
)


TARGET_SHEET = "TARGET"
NOW_RACE_SHEET = "今走レース情報"
PREDICTION_SCRIPT = Path("1_keibayosou_best_import_roi_runner.py")


class PredictionRunnerError(RuntimeError):
    """Raised when the legacy prediction runner cannot produce importable output."""


def execute_queued_prediction_job(job_id: str) -> dict[str, str]:
    """Load and execute one queued prediction job in a worker-owned DB session."""

    from app.db.session import SessionLocal
    from app.services.data_quality import has_blocking_quality_status

    with SessionLocal() as db:
        job = db.get(JobRun, job_id)
        if job is None:
            raise LookupError(f"queued prediction job was not found: {job_id}")
        if job.job_type not in {"prediction.run", "prediction.python"}:
            raise PredictionRunnerError(f"job is not a prediction job: {job.job_type}")
        if job.status not in {"queued", "running"}:
            return {"job_id": job.id, "status": job.status}

        job.status = "running"
        job.started_at = datetime.utcnow()
        record_job_log(
            db,
            job_run_id=job.id,
            level="INFO",
            event_code="JOB_STARTED",
            message="prediction worker started job execution",
            context={"job_type": job.job_type, "queue": "prediction"},
        )
        db.commit()

        try:
            if has_blocking_quality_status(
                db,
                race_date=job.race_date,
                race_id=job.race_id,
            ):
                raise PredictionRunnerError(
                    "REDのデータ品質状態があるため、このジョブは実行できません"
                )
            summary = run_prediction_job(
                db,
                prediction_run_id=job.id,
                race_date=job.race_date,
                race_id=job.race_id,
                params=job.params,
                force=job.force,
            )
            job.message = json.dumps(summary, ensure_ascii=False, default=str)
            job.status = "completed"
        except Exception as exc:
            db.rollback()
            job = db.get(JobRun, job_id)
            if job is None:
                raise
            job.status = "failed"
            job.message = redact_text(f"{exc.__class__.__name__}: {exc}")
        finally:
            job.finished_at = datetime.utcnow()
            record_job_log(
                db,
                job_run_id=job.id,
                level="INFO" if job.status == "completed" else "ERROR",
                event_code="JOB_COMPLETED" if job.status == "completed" else "JOB_FAILED",
                message=(
                    "prediction job execution completed"
                    if job.status == "completed"
                    else "prediction job execution failed"
                ),
                context={"status": job.status, "queue": "prediction"},
            )
            db.add(job)
            db.commit()
        return {"job_id": job.id, "status": job.status}


def run_prediction_job(
    db: Session,
    prediction_run_id: str,
    race_date: date | None = None,
    race_id: str | None = None,
    params: dict[str, Any] | None = None,
    force: bool = False,
) -> dict[str, Any]:
    """Run or dry-run the Phase 5 prediction pipeline and import the results."""

    settings = get_settings()
    mode = settings.prediction_runner_mode
    existing_result_count = db.scalar(
        select(func.count())
        .select_from(PredictionResult)
        .where(PredictionResult.prediction_run_id == prediction_run_id)
    )
    if int(existing_result_count or 0) > 0:
        raise PredictionRunnerError("prediction run is immutable; create a new run_id")

    run_dir = settings.exports_dir / "runs" / prediction_run_id
    log_dir = settings.logs_dir / "runs" / prediction_run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    log_dir.mkdir(parents=True, exist_ok=True)

    workspace: PredictionWorkspace | None = None
    if mode == "execute":
        if race_date is None:
            raise PredictionRunnerError("race_date is required for execute mode")
        source_workbook = find_race_workbook(race_date=race_date, prefer_feature_file=False)
        if source_workbook is None:
            raise FileNotFoundError(f"prediction input workbook not found: {race_date}")
        workspace = prepare_prediction_workspace(
            settings,
            run_id=prediction_run_id,
            race_date=race_date,
            source_workbook=source_workbook,
        )
        script_path = workspace.script_path
        execution_workbook = workspace.expected_output_path
        input_checksum = file_sha256(workspace.source_input_path)
    else:
        script_path = (settings.legacy_root / PREDICTION_SCRIPT).resolve()
        source_workbook = find_race_workbook(race_date=race_date, prefer_feature_file=True)
        execution_workbook = source_workbook
        input_checksum = file_sha256(source_workbook) if source_workbook else None

    input_manifest = {
        "run_id": prediction_run_id,
        "job_type": "prediction.run",
        "race_date": race_date.isoformat() if race_date else None,
        "race_id": race_id,
        "force": force,
        "params": _jsonable(params or {}),
        "mode": mode,
        "script": str(script_path),
        "script_exists": script_path.exists(),
        "source_workbook": str(source_workbook) if source_workbook else None,
        "source_workbook_sha256": input_checksum,
        "workspace_manifest": str(workspace.manifest_path) if workspace else None,
        "workspace_manifest_sha256": (
            file_sha256(workspace.manifest_path) if workspace else None
        ),
        "input_snapshot_sha256": (
            workspace.manifest["input_snapshot_sha256"] if workspace else input_checksum
        ),
        "data_contract": {
            "raw": str(settings.excel_input_dir),
            "staging": str(settings.staging_dir),
            "exports": str(settings.exports_dir),
            "logs": str(settings.logs_dir),
        },
        "created_at": _utc_now_text(),
    }
    _write_json(run_dir / "input_manifest_v1.json", input_manifest)

    stdout_path = log_dir / "stdout.log"
    stderr_path = log_dir / "stderr.log"
    prediction_started_at = datetime.utcnow()
    started = time.perf_counter()
    if mode == "execute":
        assert workspace is not None
        return_code = execute_prediction_script(
            script_path=script_path,
            workspace_dir=workspace.root,
            stdout_path=stdout_path,
            stderr_path=stderr_path,
            race_date=race_date,
            params=params or {},
        )
    else:
        return_code = _write_dry_run_logs(stdout_path, stderr_path, script_path, source_workbook)

    if return_code != 0:
        raise PredictionRunnerError(
            f"legacy prediction failed: return_code={return_code}, stderr={stderr_path}"
        )
    if execution_workbook is None or not execution_workbook.exists():
        raise FileNotFoundError("prediction output workbook not found")

    prediction_output = run_dir / "prediction_output.xlsx"
    copy_prediction_output(execution_workbook, prediction_output)

    import_summary = import_prediction_workbook(
        db,
        prediction_run_id=prediction_run_id,
        workbook_path=prediction_output,
        race_date=race_date,
        race_id=race_id,
        started_at=prediction_started_at,
        input_checksum=input_checksum,
        version_metadata=workspace.manifest if workspace else None,
    )

    output_manifest = {
        "run_id": prediction_run_id,
        "job_type": "prediction.run",
        "mode": mode,
        "return_code": return_code,
        "duration_seconds": round(time.perf_counter() - started, 3),
        "stdout_log": str(stdout_path),
        "stderr_log": str(stderr_path),
        "workspace_manifest": str(workspace.manifest_path) if workspace else None,
        "input_snapshot_sha256": (
            workspace.manifest["input_snapshot_sha256"] if workspace else input_checksum
        ),
        "outputs": [
            {"kind": "prediction_output", "path": str(prediction_output), "exists": True},
            {"kind": "engine_prediction_json", "path": import_summary.result_json_file, "exists": True},
        ],
        "finished_at": _utc_now_text(),
    }
    _write_json(run_dir / "output_manifest_v1.json", output_manifest)

    prediction_run = db.get(PredictionRun, prediction_run_id)
    if prediction_run is not None:
        prediction_run.manifest_file = str(run_dir / "output_manifest_v1.json")
        if workspace is not None:
            config_version, weight_version = register_prediction_versions(
                db,
                manifest=workspace.manifest,
                environment=settings.environment,
            )
            prediction_run.config_version_id = config_version.id
            prediction_run.feature_weight_version_id = weight_version.id
            prediction_run.code_version = workspace.manifest["code_bundle_sha256"]
            prediction_run.prediction_version = (
                f"legacy-two-stage-{workspace.manifest['code_bundle_sha256'][:12]}"
            )
            prediction_run.feature_version = "prediction_workspace_v1"
            prediction_run.weight_version = weight_version.version_name
            torch_version = workspace.manifest["dependencies"].get("torch", "unknown")
            prediction_run.model_version = f"legacy-simple-mlp-torch-{torch_version}"
        else:
            prediction_run.code_version = settings.git_commit
        prediction_run.parameters = params or {}
        prediction_run.input_manifest_sha256 = file_sha256(run_dir / "input_manifest_v1.json")
        db.add(prediction_run)
        job_run_id = prediction_run_id if db.get(JobRun, prediction_run_id) is not None else None
        artifact_specs: list[tuple[str, str, Path, str]] = [
            (
                "input_manifest",
                "input_manifest_v1.json",
                run_dir / "input_manifest_v1.json",
                "application/json",
            ),
            (
                "output_manifest",
                "output_manifest_v1.json",
                run_dir / "output_manifest_v1.json",
                "application/json",
            ),
            (
                "prediction_output",
                "prediction_output.xlsx",
                prediction_output,
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            ),
            (
                "engine_prediction_json",
                "engine_prediction_v1.json",
                Path(import_summary.result_json_file),
                "application/json",
            ),
            ("stdout_log", "stdout.log", stdout_path, "text/plain"),
            ("stderr_log", "stderr.log", stderr_path, "text/plain"),
        ]
        if workspace is not None:
            artifact_specs.extend(
                [
                    (
                        "prediction_workspace_manifest",
                        "prediction_workspace_manifest_v1.json",
                        workspace.manifest_path,
                        "application/json",
                    ),
                    (
                        "prediction_input",
                        "prediction_input.xlsx",
                        workspace.source_input_path,
                        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    ),
                    (
                        "prediction_odds",
                        "prediction_odds.csv",
                        workspace.odds_path,
                        "text/csv",
                    ),
                ]
            )
            for item in workspace.manifest["masters"]:
                artifact_specs.append(
                    (
                        "prediction_master",
                        f"master:{item['name']}",
                        workspace.root / item["relative_path"],
                        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    )
                )
            weight = workspace.manifest.get("weight")
            if weight:
                artifact_specs.append(
                    (
                        "prediction_weight",
                        f"weight:{weight['name']}",
                        workspace.root / weight["relative_path"],
                        "text/x-python",
                    )
                )
        for artifact_kind, logical_name, artifact_path, content_type in artifact_specs:
            if artifact_path.is_file():
                register_artifact(
                    db,
                    path=artifact_path,
                    artifact_kind=artifact_kind,
                    logical_name=logical_name,
                    prediction_run_id=prediction_run_id,
                    job_run_id=job_run_id,
                    content_type=content_type,
                )
        db.commit()

    if import_summary.mismatches:
        raise PredictionRunnerError(
            f"prediction output identity mismatch: mismatches={import_summary.mismatches}"
        )

    return {
        "legacy": output_manifest,
        "prediction": import_summary.model_dump(mode="json"),
    }


def import_prediction_workbook(
    db: Session,
    prediction_run_id: str,
    workbook_path: Path,
    race_date: date | None = None,
    race_id: str | None = None,
    started_at: datetime | None = None,
    input_checksum: str | None = None,
    version_metadata: dict[str, Any] | None = None,
) -> PredictionImportSummary:
    """Import one prediction workbook into versioned prediction tables."""

    existing_result_count = db.scalar(
        select(func.count())
        .select_from(PredictionResult)
        .where(PredictionResult.prediction_run_id == prediction_run_id)
    )
    if int(existing_result_count or 0) > 0:
        raise PredictionRunnerError("prediction run is immutable; create a new run_id")

    rows = _extract_prediction_rows(workbook_path, race_id=race_id)
    _validate_prediction_rows(rows)
    run_dir = workbook_path.parent
    result_json_path = run_dir / "engine_prediction_v1.json"
    result_payload = {
        "prediction_run_id": prediction_run_id,
        "race_date": race_date.isoformat() if race_date else None,
        "race_id": race_id,
        "source_file": workbook_path.name,
        "created_at": _utc_now_text(),
        "results": rows,
    }
    _write_json(result_json_path, result_payload)

    prediction_run = db.get(PredictionRun, prediction_run_id)
    if prediction_run is None:
        prediction_run = PredictionRun(id=prediction_run_id)
        db.add(prediction_run)

    db.flush()

    race_ids = sorted({row["race_id"] for row in rows})
    entries = list(db.scalars(select(RaceEntry).where(RaceEntry.race_id.in_(race_ids))))
    entries_by_key = {(entry.race_id, entry.horse_no): entry for entry in entries}
    output_keys = {(row["race_id"], row["horse_no"]) for row in rows}

    matched = 0
    # 予想出力に欠けている出走馬も不一致として数え、安全側に失敗させる。
    mismatches = len(set(entries_by_key) - output_keys)
    for row in rows:
        result = PredictionResult(
            prediction_run_id=prediction_run_id,
            race_id=row["race_id"],
            horse_no=row["horse_no"],
            horse_name=row["horse_name"],
            popularity=row.get("popularity"),
            win_odds=row.get("win_odds"),
            place_odds=row.get("place_odds"),
            prediction_rank=row.get("prediction_rank"),
            prediction_score=row.get("prediction_score"),
            estimated_in3_rate=row.get("estimated_in3_rate"),
            expected_value=row.get("expected_value"),
            risk_flag=bool(row.get("risk_flag")),
            risk_score=row.get("risk_score"),
            risk_reason=row.get("risk_reason"),
            evaluation_reason=row.get("evaluation_reason"),
            feature_summary=row.get("feature_summary"),
            raw=row.get("raw"),
        )
        db.add(result)

        entry = entries_by_key.get((result.race_id, result.horse_no))
        if entry is not None and _horse_identity_matches(entry, result):
            _set_if_present(entry, "prediction_rank", result.prediction_rank)
            _set_if_present(entry, "prediction_score", result.prediction_score)
            _set_if_present(entry, "estimated_in3_rate", result.estimated_in3_rate)
            _set_if_present(entry, "expected_value", result.expected_value)
            matched += 1
        else:
            mismatches += 1

    finished_at = datetime.utcnow()
    prediction_run.status = "failed" if mismatches else "completed"
    prediction_run.race_date = race_date
    prediction_run.race_id = race_id
    prediction_run.prediction_version = _prediction_version(version_metadata)
    prediction_run.feature_version = (
        version_metadata.get("schema_version", "legacy-feature-v1")
        if version_metadata
        else "legacy-feature-v1"
    )
    prediction_run.weight_version = _weight_version(version_metadata)
    prediction_run.model_version = _model_version(version_metadata)
    prediction_run.code_version = (
        version_metadata.get("code_bundle_sha256", "unknown")
        if version_metadata
        else "unknown"
    )
    prediction_run.source_file = workbook_path.name
    prediction_run.output_file = str(workbook_path)
    prediction_run.result_json_file = str(result_json_path)
    prediction_run.input_checksum = input_checksum or file_sha256(workbook_path)
    prediction_run.result_count = len(rows)
    prediction_run.matched_count = matched
    prediction_run.mismatch_count = mismatches
    prediction_run.message = (
        f"prediction results imported: {len(rows)} rows, mismatches: {mismatches}"
    )
    prediction_run.started_at = started_at or finished_at
    prediction_run.finished_at = finished_at
    db.add(prediction_run)
    db.commit()

    return PredictionImportSummary(
        prediction_run_id=prediction_run_id,
        race_date=race_date,
        race_id=race_id,
        source_file=workbook_path.name,
        output_file=str(workbook_path),
        result_json_file=str(result_json_path),
        manifest_file=str(run_dir / "output_manifest_v1.json"),
        results=len(rows),
        matched=matched,
        mismatches=mismatches,
    )


def _extract_prediction_rows(workbook_path: Path, race_id: str | None = None) -> list[dict[str, Any]]:
    excel = pd.ExcelFile(workbook_path)
    sheet_name = TARGET_SHEET if TARGET_SHEET in excel.sheet_names else NOW_RACE_SHEET
    if sheet_name not in excel.sheet_names:
        raise PredictionRunnerError("prediction workbook has no TARGET or 今走レース情報 sheet")

    df = pd.read_excel(workbook_path, sheet_name=sheet_name)
    rows: list[dict[str, Any]] = []
    for row_number, record in enumerate(df.to_dict(orient="records"), start=2):
        raw = row_to_jsonable(record)
        if all(is_blank(value) for value in raw.values()):
            continue
        row_race_id = normalize_race_id(pick_value(raw, ["レースID", "rid_str", "race_id"]))
        if race_id is not None and row_race_id is not None and row_race_id != race_id:
            continue
        horse_no = safe_int(pick_value(raw, ["馬番", "馬 番", "umaban"]))
        horse_name = safe_str(pick_value(raw, ["馬名", "horse_name", "name"]))
        if row_race_id is None or horse_no is None or horse_name is None:
            raise PredictionRunnerError(
                f"prediction row has an invalid identity: sheet={sheet_name}, row={row_number}"
            )

        score = safe_float(pick_value(raw, ["score", "total"]))
        prediction_rank = safe_int(pick_value(raw, ["予想順位", "rank"]))
        if prediction_rank is None or score is None:
            raise PredictionRunnerError(
                f"prediction row is missing rank or score: sheet={sheet_name}, row={row_number}"
            )
        estimated_in3_rate = safe_float(
            pick_value(raw, ["推定馬券内率_オッズ補正後", "推定馬券内率"])
        )
        expected_value = safe_float(pick_value(raw, ["期待値"]))
        risk_score = _risk_score(raw)
        risk_flag = risk_score > 0
        row = {
            "race_id": row_race_id,
            "horse_no": horse_no,
            "horse_name": horse_name,
            "popularity": safe_int(pick_value(raw, ["人気", "人 気"])),
            "win_odds": safe_float(pick_value(raw, ["単勝オッズ", "オッズ", "オ ッ ズ", "tansho"])),
            "place_odds": safe_float(pick_value(raw, ["複勝オッズ", "fukusho"])),
            "prediction_rank": prediction_rank,
            "prediction_score": score,
            "estimated_in3_rate": estimated_in3_rate,
            "expected_value": expected_value,
            "risk_flag": risk_flag,
            "risk_score": risk_score,
            "risk_reason": _risk_reason(raw, risk_score),
            "evaluation_reason": _evaluation_reason(
                score=score,
                prediction_rank=prediction_rank,
                estimated_in3_rate=estimated_in3_rate,
                expected_value=expected_value,
            ),
            "feature_summary": _feature_summary(raw),
            "raw": raw,
        }
        rows.append(row)

    rows.sort(key=lambda item: (item["race_id"], item.get("prediction_rank") or 999, item["horse_no"]))
    return rows


def _validate_prediction_rows(rows: list[dict[str, Any]]) -> None:
    if not rows:
        raise PredictionRunnerError("prediction workbook contains no importable result rows")

    keys = [(row["race_id"], row["horse_no"]) for row in rows]
    if len(keys) != len(set(keys)):
        raise PredictionRunnerError("prediction workbook contains duplicate race/horse rows")

    race_ids = sorted({row["race_id"] for row in rows})
    for current_race_id in race_ids:
        race_rows = [row for row in rows if row["race_id"] == current_race_id]
        ranks = sorted(int(row["prediction_rank"]) for row in race_rows)
        expected = list(range(1, len(race_rows) + 1))
        if ranks != expected:
            raise PredictionRunnerError(
                f"prediction ranks must be unique and contiguous: race_id={current_race_id}"
            )


def execute_prediction_script(
    script_path: Path,
    workspace_dir: Path,
    stdout_path: Path,
    stderr_path: Path,
    race_date: date | None,
    params: dict[str, Any],
) -> int:
    if not script_path.exists():
        raise FileNotFoundError(f"legacy prediction script not found: {script_path}")

    settings = get_settings()
    env = os.environ.copy()
    env.update(
        {
            "KEIBA_DATA_RAW_DIR": str(workspace_dir / "data" / "input"),
            "KEIBA_DATA_STAGING_DIR": str(settings.staging_dir),
            "KEIBA_DATA_EXPORTS_DIR": str(settings.exports_dir),
            "KEIBA_DATA_LOGS_DIR": str(settings.logs_dir),
            # CPU演算とハッシュ順序を固定し、同一スナップショットの再現性を高める。
            "PYTHONHASHSEED": "0",
            "OMP_NUM_THREADS": "1",
            "MKL_NUM_THREADS": "1",
        }
    )
    extra_args = params.get("args", [])
    if not isinstance(extra_args, list):
        raise PredictionRunnerError("params.args must be a list when provided")
    stdin_text = race_date.strftime("%Y%m%d") if race_date else ""

    with stdout_path.open("w", encoding="utf-8") as stdout_file, stderr_path.open(
        "w",
        encoding="utf-8",
    ) as stderr_file:
        completed = subprocess.run(
            [sys.executable, str(script_path), *[str(item) for item in extra_args]],
            cwd=workspace_dir,
            env=env,
            input=stdin_text + "\n",
            text=True,
            stdout=stdout_file,
            stderr=stderr_file,
            timeout=settings.prediction_timeout_seconds,
            check=False,
        )
    return completed.returncode


def _write_dry_run_logs(
    stdout_path: Path,
    stderr_path: Path,
    script_path: Path,
    source_workbook: Path | None,
) -> int:
    stdout_path.write_text(
        "\n".join(
            [
                "prediction runner dry-run",
                f"script={script_path}",
                f"script_exists={script_path.exists()}",
                f"source_workbook={source_workbook or '-'}",
                "external prediction script was not executed",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    stderr_path.write_text("", encoding="utf-8")
    return 0


def _risk_score(raw: dict[str, Any]) -> float:
    values = [
        safe_float(pick_value(raw, ["favorite_risk"])),
        safe_float(pick_value(raw, ["extra_penalty"])),
        safe_float(pick_value(raw, ["rest_dist_risk"])),
    ]
    return float(sum(value for value in values if value is not None and value > 0))


def _risk_reason(raw: dict[str, Any], risk_score: float) -> str:
    reasons: list[str] = []
    if (safe_float(pick_value(raw, ["favorite_risk"])) or 0) > 0:
        reasons.append("人気先行リスク")
    if (safe_float(pick_value(raw, ["extra_penalty"])) or 0) > 0:
        reasons.append("追加ペナルティ")
    if (safe_float(pick_value(raw, ["rest_dist_risk"])) or 0) > 0:
        reasons.append("休養・距離変化リスク")
    if not reasons:
        return "危険馬判定なし"
    return f"{'、'.join(reasons)}（risk_score={risk_score:.2f}）"


def _evaluation_reason(
    score: float | None,
    prediction_rank: int | None,
    estimated_in3_rate: float | None,
    expected_value: float | None,
) -> str:
    parts: list[str] = []
    if prediction_rank is not None:
        parts.append(f"Python順位{prediction_rank}位")
    if score is not None:
        parts.append(f"score {score:.2f}")
    if estimated_in3_rate is not None:
        parts.append(f"推定馬券内率 {estimated_in3_rate:.2f}")
    if expected_value is not None:
        parts.append(f"期待値 {expected_value:.2f}")
    return " / ".join(parts) if parts else "評価理由未生成"


def _feature_summary(raw: dict[str, Any]) -> dict[str, Any]:
    keys = [
        "total",
        "dl_rank",
        "dl_prob",
        "favorite_risk",
        "extra_penalty",
        "rest_dist_risk",
        "推定馬券内率_オッズ補正後",
        "期待値",
    ]
    return {key: raw.get(key) for key in keys if key in raw}


def _horse_identity_matches(entry: RaceEntry, result: PredictionResult) -> bool:
    return normalize_horse_name(entry.horse_name) == normalize_horse_name(result.horse_name)


def _prediction_version(metadata: dict[str, Any] | None) -> str:
    if not metadata:
        return "legacy-v1"
    return f"legacy-two-stage-{metadata.get('code_bundle_sha256', 'unknown')[:12]}"


def _weight_version(metadata: dict[str, Any] | None) -> str:
    if not metadata:
        return "legacy-weight-v1"
    weight = metadata.get("weight")
    return weight["name"] if weight else "built-in-config"


def _model_version(metadata: dict[str, Any] | None) -> str:
    if not metadata:
        return "legacy-python"
    torch_version = metadata.get("dependencies", {}).get("torch", "unknown")
    return f"legacy-simple-mlp-torch-{torch_version}"


def copy_prediction_output(source: Path, destination: Path) -> None:
    source = source.resolve(strict=True)
    if not source.is_file():
        raise PredictionRunnerError("prediction output must be a regular file")
    destination.parent.mkdir(parents=True, exist_ok=True)
    source_hash = file_sha256(source)
    if destination.exists():
        if file_sha256(destination) != source_hash:
            raise PredictionRunnerError(
                "immutable prediction output already exists with different bytes"
            )
        return
    temporary = destination.with_name(f".{destination.name}.tmp")
    shutil.copy2(source, temporary)
    if file_sha256(temporary) != source_hash:
        temporary.unlink(missing_ok=True)
        raise PredictionRunnerError("prediction output copy hash mismatch")
    os.replace(temporary, destination)


def _set_if_present(model: Any, field: str, value: Any) -> None:
    if value is not None:
        setattr(model, field, value)


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")


def _jsonable(payload: dict[str, Any]) -> dict[str, Any]:
    return json.loads(json.dumps(payload, default=str, ensure_ascii=False))


def _utc_now_text() -> str:
    return datetime.utcnow().isoformat(timespec="seconds") + "Z"
