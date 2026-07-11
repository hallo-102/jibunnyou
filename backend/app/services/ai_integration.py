from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.core.config import Settings, get_settings
from app.core.logging import redact_text
from app.db.models import (
    AiAnalysis,
    AiAnalysisOutput,
    JobRun,
    PredictionResult,
    PredictionRun,
)
from app.legacy_bridge.normalization import normalize_horse_name
from app.schemas.ai_independent import IndependentAnalysisResponse
from app.schemas.ai_integration import (
    ComparisonInput,
    ComparisonResponse,
    IntegrationInput,
    IntegrationResponse,
    PythonPredictionForComparison,
)
from app.services.ai_independent import canonical_json_bytes, payload_sha256
from app.services.ai_provider import (
    COMPARISON_PROMPT_SHA256,
    COMPARISON_PROMPT_VERSION,
    INTEGRATION_PROMPT_SHA256,
    INTEGRATION_PROMPT_VERSION,
    AiComparisonProviderResult,
    AiIntegrationProviderResult,
    AiPipelineProvider,
    create_independent_ai_provider,
)
from app.services.history import record_job_log, register_artifact


PIPELINE_PROMPT_VERSION = f"{COMPARISON_PROMPT_VERSION}+{INTEGRATION_PROMPT_VERSION}"


class AiIntegrationError(RuntimeError):
    """Raised when locked independent analysis cannot be compared or integrated safely."""


def build_comparison_input(
    db: Session,
    *,
    race_id: str,
    independent_analysis_id: str | None = None,
    prediction_run_id: str | None = None,
) -> ComparisonInput:
    """Reveal Python results only after loading a locked independent output."""

    independent_analysis, independent_output = _locked_independent_output(
        db,
        race_id=race_id,
        analysis_id=independent_analysis_id,
    )
    independent_result = IndependentAnalysisResponse.model_validate(
        independent_output.output_json
    )
    if payload_sha256(independent_result.model_dump(mode="json", exclude_none=False)) != independent_output.output_hash:
        raise AiIntegrationError("固定済み独立AI結果のhashが一致しません")

    prediction_run = _prediction_run_for_race(
        db,
        race_id=race_id,
        prediction_run_id=prediction_run_id,
    )
    results = list(
        db.scalars(
            select(PredictionResult)
            .where(
                PredictionResult.prediction_run_id == prediction_run.id,
                PredictionResult.race_id == race_id,
            )
            .order_by(PredictionResult.prediction_rank, PredictionResult.horse_no)
        )
    )
    if len(results) < 2:
        raise AiIntegrationError("比較可能なPython予想が2頭以上ありません")

    independent_by_horse = {
        runner.horse_no: runner.horse_name for runner in independent_result.runners
    }
    if {result.horse_no for result in results} != set(independent_by_horse):
        raise AiIntegrationError("独立AI結果とPython予想の出走馬集合が一致しません")

    python_results: list[PythonPredictionForComparison] = []
    for result in results:
        if result.prediction_rank is None or result.prediction_score is None:
            raise AiIntegrationError(
                f"Python順位またはscoreがありません: horse_no={result.horse_no}"
            )
        if normalize_horse_name(result.horse_name) != normalize_horse_name(
            independent_by_horse[result.horse_no]
        ):
            raise AiIntegrationError(
                f"独立AI結果とPython予想の馬番・馬名が一致しません: {result.horse_no}"
            )
        python_results.append(
            PythonPredictionForComparison(
                horse_no=result.horse_no,
                horse_name=result.horse_name,
                python_rank=result.prediction_rank,
                python_score=result.prediction_score,
                estimated_in3_rate=result.estimated_in3_rate,
                expected_value=result.expected_value,
                risk_flag=result.risk_flag,
                risk_score=result.risk_score,
                risk_reason=result.risk_reason,
                evaluation_reason=result.evaluation_reason,
            )
        )

    ranks = sorted(item.python_rank for item in python_results)
    if ranks != list(range(1, len(python_results) + 1)):
        raise AiIntegrationError("Python順位は1位から全頭分連続している必要があります")

    return ComparisonInput(
        independent_analysis_id=independent_analysis.id,
        independent_output_hash=independent_output.output_hash,
        independent_result=independent_result,
        python_prediction_run_id=prediction_run.id,
        python_prediction_version=prediction_run.prediction_version,
        python_model_version=prediction_run.model_version,
        python_results=python_results,
    )


def run_comparison_integration(
    db: Session,
    *,
    race_id: str,
    job_run_id: str | None = None,
    independent_analysis_id: str | None = None,
    prediction_run_id: str | None = None,
    rerun_reason: str | None = None,
    provider: AiPipelineProvider | None = None,
    settings: Settings | None = None,
    sleeper: Callable[[float], None] = time.sleep,
) -> dict[str, Any]:
    """Persist comparison and guarded integration as separate immutable stages."""

    resolved_settings = settings or get_settings()
    comparison_input = build_comparison_input(
        db,
        race_id=race_id,
        independent_analysis_id=independent_analysis_id,
        prediction_run_id=prediction_run_id,
    )
    active_provider = provider or create_independent_ai_provider(resolved_settings)
    analysis = AiAnalysis(
        race_id=race_id,
        race_date=_prediction_race_date(db, comparison_input.python_prediction_run_id),
        prediction_run_id=comparison_input.python_prediction_run_id,
        parent_analysis_id=comparison_input.independent_analysis_id,
        analysis_sequence=_next_sequence(db, race_id),
        status="running",
        model_name=active_provider.model_name,
        prompt_version=PIPELINE_PROMPT_VERSION,
        input_data_version=comparison_input.schema_version,
        rerun_reason=_safe_text(rerun_reason, 1000),
        started_at=_utc_now(),
    )
    db.add(analysis)
    db.commit()
    db.refresh(analysis)

    started = time.perf_counter()
    run_dir = resolved_settings.exports_dir / "ai" / analysis.id
    comparison_result: AiComparisonProviderResult | None = None
    comparison_retries = 0
    integration_retries = 0
    try:
        comparison_input_payload = comparison_input.model_dump(mode="json", exclude_none=False)
        comparison_input_hash = payload_sha256(comparison_input_payload)
        comparison_input_path = run_dir / "ai_comparison_input_v1.json"
        _atomic_write_json(comparison_input_path, comparison_input_payload)
        input_artifact = register_artifact(
            db,
            path=comparison_input_path,
            artifact_kind="ai_comparison_input",
            logical_name="ai_comparison_input_v1.json",
            job_run_id=job_run_id,
            content_type="application/json",
            metadata={
                "analysis_id": analysis.id,
                "schema_version": comparison_input.schema_version,
                "python_result_visible": True,
                "independent_analysis_id": comparison_input.independent_analysis_id,
                "independent_output_hash": comparison_input.independent_output_hash,
                "prompt_version": COMPARISON_PROMPT_VERSION,
                "prompt_sha256": COMPARISON_PROMPT_SHA256,
            },
        )
        analysis.input_snapshot_hash = comparison_input_hash
        analysis.input_artifact_id = input_artifact.id
        db.commit()

        comparison_result, comparison_attempts, comparison_retries = _execute_with_retry(
            lambda: active_provider.compare(comparison_input),
            settings=resolved_settings,
            sleeper=sleeper,
            on_retry=lambda number, delay, exc: _record_retry(
                db,
                job_run_id,
                "comparison",
                number,
                delay,
                exc,
            ),
        )
        _validate_comparison(comparison_input, comparison_result.output)
        comparison_output_payload = comparison_result.output.model_dump(
            mode="json",
            exclude_none=False,
        )
        comparison_output_hash = payload_sha256(comparison_output_payload)
        comparison_output_path = run_dir / "ai_comparison_result_v1.json"
        _atomic_write_json(comparison_output_path, comparison_output_payload)
        register_artifact(
            db,
            path=comparison_output_path,
            artifact_kind="ai_comparison_result",
            logical_name="ai_comparison_result_v1.json",
            job_run_id=job_run_id,
            content_type="application/json",
            metadata={
                "analysis_id": analysis.id,
                "schema_version": comparison_result.output.schema_version,
                "provider_response_id": comparison_result.provider_response_id,
                "prompt_version": COMPARISON_PROMPT_VERSION,
                "prompt_sha256": COMPARISON_PROMPT_SHA256,
            },
        )
        db.add(
            AiAnalysisOutput(
                analysis_id=analysis.id,
                stage="comparison",
                output_schema_version=comparison_result.output.schema_version,
                output_json=comparison_output_payload,
                output_hash=comparison_output_hash,
                confidence=_confidence_value(comparison_result.output.data_confidence),
                python_result_visible=True,
                is_locked=True,
                locked_at=_utc_now(),
            )
        )
        db.commit()

        integration_input = IntegrationInput(
            comparison_input=comparison_input,
            comparison_output_hash=comparison_output_hash,
            comparison_result=comparison_result.output,
        )
        integration_input_payload = integration_input.model_dump(mode="json", exclude_none=False)
        integration_input_hash = payload_sha256(integration_input_payload)
        integration_input_path = run_dir / "ai_integration_input_v1.json"
        _atomic_write_json(integration_input_path, integration_input_payload)
        register_artifact(
            db,
            path=integration_input_path,
            artifact_kind="ai_integration_input",
            logical_name="ai_integration_input_v1.json",
            job_run_id=job_run_id,
            content_type="application/json",
            metadata={
                "analysis_id": analysis.id,
                "schema_version": integration_input.schema_version,
                "comparison_output_hash": comparison_output_hash,
                "prompt_version": INTEGRATION_PROMPT_VERSION,
                "prompt_sha256": INTEGRATION_PROMPT_SHA256,
            },
        )
        db.commit()

        integration_result, integration_attempts, integration_retries = _execute_with_retry(
            lambda: active_provider.integrate(integration_input),
            settings=resolved_settings,
            sleeper=sleeper,
            on_retry=lambda number, delay, exc: _record_retry(
                db,
                job_run_id,
                "integration",
                number,
                delay,
                exc,
            ),
        )
        _validate_integration(integration_input, integration_result.output)
        integration_output_payload = integration_result.output.model_dump(
            mode="json",
            exclude_none=False,
        )
        integration_output_hash = payload_sha256(integration_output_payload)
        integration_output_path = run_dir / "ai_integration_result_v1.json"
        _atomic_write_json(integration_output_path, integration_output_payload)
        register_artifact(
            db,
            path=integration_output_path,
            artifact_kind="ai_integration_result",
            logical_name="ai_integration_result_v1.json",
            job_run_id=job_run_id,
            content_type="application/json",
            metadata={
                "analysis_id": analysis.id,
                "schema_version": integration_result.output.schema_version,
                "provider_response_id": integration_result.provider_response_id,
                "prompt_version": INTEGRATION_PROMPT_VERSION,
                "prompt_sha256": INTEGRATION_PROMPT_SHA256,
            },
        )
        db.add(
            AiAnalysisOutput(
                analysis_id=analysis.id,
                stage="integration",
                output_schema_version=integration_result.output.schema_version,
                output_json=integration_output_payload,
                output_hash=integration_output_hash,
                confidence=_confidence_value(integration_result.output.data_confidence),
                python_result_visible=True,
                is_locked=True,
                locked_at=_utc_now(),
            )
        )
        # 追試に必要な入力・出力・prompt hashを一つの固定manifestへ集約する。
        manifest_payload = {
            "schema_version": "ai_pipeline_manifest_v1",
            "analysis_id": analysis.id,
            "race_id": race_id,
            "independent_analysis_id": comparison_input.independent_analysis_id,
            "independent_output_hash": comparison_input.independent_output_hash,
            "prediction_run_id": comparison_input.python_prediction_run_id,
            "comparison_input_hash": comparison_input_hash,
            "comparison_output_hash": comparison_output_hash,
            "integration_input_hash": integration_input_hash,
            "integration_output_hash": integration_output_hash,
            "comparison_prompt_version": COMPARISON_PROMPT_VERSION,
            "comparison_prompt_sha256": COMPARISON_PROMPT_SHA256,
            "integration_prompt_version": INTEGRATION_PROMPT_VERSION,
            "integration_prompt_sha256": INTEGRATION_PROMPT_SHA256,
        }
        manifest_path = run_dir / "ai_pipeline_manifest_v1.json"
        _atomic_write_json(manifest_path, manifest_payload)
        register_artifact(
            db,
            path=manifest_path,
            artifact_kind="ai_pipeline_manifest",
            logical_name="ai_pipeline_manifest_v1.json",
            job_run_id=job_run_id,
            content_type="application/json",
            metadata={
                "analysis_id": analysis.id,
                "schema_version": manifest_payload["schema_version"],
            },
        )
        analysis.status = "succeeded"
        analysis.prompt_tokens = _sum_optional(
            comparison_result.prompt_tokens,
            integration_result.prompt_tokens,
        )
        analysis.completion_tokens = _sum_optional(
            comparison_result.completion_tokens,
            integration_result.completion_tokens,
        )
        analysis.duration_ms = round((time.perf_counter() - started) * 1000)
        analysis.finished_at = _utc_now()
        db.commit()
        return {
            "analysis_id": analysis.id,
            "independent_analysis_id": comparison_input.independent_analysis_id,
            "prediction_run_id": comparison_input.python_prediction_run_id,
            "race_id": race_id,
            "status": analysis.status,
            "comparison_output_hash": comparison_output_hash,
            "integration_output_hash": integration_output_hash,
            "comparison_attempts": comparison_attempts,
            "comparison_retries": comparison_retries,
            "integration_attempts": integration_attempts,
            "integration_retries": integration_retries,
            "overall_alignment": comparison_result.output.overall_alignment,
            "integration_strategy": integration_result.output.integration_strategy,
            "runner_count": len(integration_result.output.horses),
        }
    except Exception as exc:
        db.rollback()
        failed = db.get(AiAnalysis, analysis.id)
        if failed is not None:
            has_comparison = bool(
                db.scalar(
                    select(func.count(AiAnalysisOutput.id)).where(
                        AiAnalysisOutput.analysis_id == analysis.id,
                        AiAnalysisOutput.stage == "comparison",
                    )
                )
            )
            failed.status = "degraded" if has_comparison else "failed"
            failed.error_message = redact_text(f"{exc.__class__.__name__}: {exc}")
            failed.duration_ms = round((time.perf_counter() - started) * 1000)
            failed.finished_at = _utc_now()
            db.commit()
        raise AiIntegrationError(redact_text(str(exc))) from exc


def execute_queued_comparison_job(db: Session, job: JobRun) -> dict[str, Any]:
    """Execute the comparison/integration branch for an already-running AI job."""

    if not job.race_id:
        raise AiIntegrationError("ai.compare_integrateにはrace_idが必要です")
    params = job.params or {}
    return run_comparison_integration(
        db,
        race_id=job.race_id,
        job_run_id=job.id,
        independent_analysis_id=params.get("independent_analysis_id"),
        prediction_run_id=params.get("prediction_run_id"),
        rerun_reason=params.get("rerun_reason"),
    )


def _locked_independent_output(
    db: Session,
    *,
    race_id: str,
    analysis_id: str | None,
) -> tuple[AiAnalysis, AiAnalysisOutput]:
    stmt = (
        select(AiAnalysis, AiAnalysisOutput)
        .join(AiAnalysisOutput, AiAnalysisOutput.analysis_id == AiAnalysis.id)
        .where(
            AiAnalysis.race_id == race_id,
            AiAnalysis.status == "succeeded",
            AiAnalysisOutput.stage == "independent",
            AiAnalysisOutput.is_locked.is_(True),
            AiAnalysisOutput.python_result_visible.is_(False),
        )
    )
    if analysis_id is not None:
        stmt = stmt.where(AiAnalysis.id == analysis_id)
    row = db.execute(
        stmt.order_by(AiAnalysis.analysis_sequence.desc()).limit(1)
    ).first()
    if row is None:
        raise AiIntegrationError("固定済み独立AI結果がありません。先に独立分析を実行してください")
    return row[0], row[1]


def _prediction_run_for_race(
    db: Session,
    *,
    race_id: str,
    prediction_run_id: str | None,
) -> PredictionRun:
    if prediction_run_id is not None:
        run = db.get(PredictionRun, prediction_run_id)
        if run is None or run.status != "completed":
            raise AiIntegrationError("指定したPython予想runが完了状態ではありません")
        exists = db.scalar(
            select(func.count(PredictionResult.id)).where(
                PredictionResult.prediction_run_id == run.id,
                PredictionResult.race_id == race_id,
            )
        )
        if not exists:
            raise AiIntegrationError("指定したPython予想runに対象レースがありません")
        return run

    run = db.scalar(
        select(PredictionRun)
        .join(PredictionResult, PredictionResult.prediction_run_id == PredictionRun.id)
        .where(
            PredictionRun.status == "completed",
            PredictionResult.race_id == race_id,
        )
        .order_by(PredictionRun.finished_at.desc(), PredictionRun.created_at.desc())
        .limit(1)
    )
    if run is None:
        raise AiIntegrationError("完了済みPython予想がありません")
    return run


def _validate_comparison(input_data: ComparisonInput, output: ComparisonResponse) -> None:
    if output.race_id != input_data.independent_result.race_id:
        raise AiIntegrationError("比較出力のrace_idが一致しません")
    if output.independent_analysis_id != input_data.independent_analysis_id:
        raise AiIntegrationError("比較出力のindependent_analysis_idが一致しません")
    if output.python_prediction_run_id != input_data.python_prediction_run_id:
        raise AiIntegrationError("比較出力のprediction_run_idが一致しません")

    python_by_horse = {item.horse_no: item for item in input_data.python_results}
    ai_by_horse = {
        item.horse_no: item for item in input_data.independent_result.runners
    }
    seen: set[int] = set()
    material_horses: set[int] = set()
    for horse in output.horses:
        if horse.horse_no in seen:
            raise AiIntegrationError(f"比較出力に馬番重複があります: {horse.horse_no}")
        seen.add(horse.horse_no)
        python = python_by_horse.get(horse.horse_no)
        independent = ai_by_horse.get(horse.horse_no)
        if python is None or independent is None:
            raise AiIntegrationError(f"比較出力に存在しない馬番があります: {horse.horse_no}")
        if normalize_horse_name(horse.horse_name) != normalize_horse_name(python.horse_name):
            raise AiIntegrationError(f"比較出力の馬番・馬名が一致しません: {horse.horse_no}")
        expected_gap = (
            python.python_rank - independent.ai_rank
            if independent.ai_rank is not None
            else None
        )
        expected_level = (
            "unknown"
            if expected_gap is None
            else "exact"
            if expected_gap == 0
            else "small_difference"
            if abs(expected_gap) <= 2
            else "material_difference"
        )
        if (
            horse.python_rank != python.python_rank
            or horse.ai_rank != independent.ai_rank
            or horse.rank_gap != expected_gap
            or horse.agreement_level != expected_level
        ):
            raise AiIntegrationError(f"比較出力の順位事実が入力と一致しません: {horse.horse_no}")
        expected_material = expected_gap is not None and abs(expected_gap) >= 3
        if horse.material_opposition != expected_material:
            raise AiIntegrationError(f"比較出力の反対材料判定が順位差と一致しません: {horse.horse_no}")
        if expected_material:
            material_horses.add(horse.horse_no)

    if seen != set(python_by_horse):
        raise AiIntegrationError("比較出力に不足している出走馬があります")
    if set(output.opposition.horse_nos) != material_horses:
        raise AiIntegrationError("反対材料の馬番一覧が馬別判定と一致しません")
    if output.opposition.has_material_opposition != bool(material_horses):
        raise AiIntegrationError("反対材料の有無が馬別判定と一致しません")
    if material_horses and not output.manual_review_required:
        raise AiIntegrationError("重大な不一致がある比較はmanual reviewが必要です")


def _validate_integration(input_data: IntegrationInput, output: IntegrationResponse) -> None:
    comparison = input_data.comparison_result
    if output.race_id != comparison.race_id:
        raise AiIntegrationError("統合出力のrace_idが一致しません")
    if output.independent_analysis_id != comparison.independent_analysis_id:
        raise AiIntegrationError("統合出力のindependent_analysis_idが一致しません")
    if output.python_prediction_run_id != comparison.python_prediction_run_id:
        raise AiIntegrationError("統合出力のprediction_run_idが一致しません")

    python_by_horse = {
        item.horse_no: item for item in input_data.comparison_input.python_results
    }
    ai_by_horse = {
        item.horse_no: item
        for item in input_data.comparison_input.independent_result.runners
    }
    seen: set[int] = set()
    ranks: list[int] = []
    scores_by_rank: list[tuple[int, float]] = []
    exceptional_shift = False
    for horse in output.horses:
        if horse.horse_no in seen:
            raise AiIntegrationError(f"統合出力に馬番重複があります: {horse.horse_no}")
        seen.add(horse.horse_no)
        python = python_by_horse.get(horse.horse_no)
        independent = ai_by_horse.get(horse.horse_no)
        if python is None or independent is None:
            raise AiIntegrationError(f"統合出力に存在しない馬番があります: {horse.horse_no}")
        if normalize_horse_name(horse.horse_name) != normalize_horse_name(python.horse_name):
            raise AiIntegrationError(f"統合出力の馬番・馬名が一致しません: {horse.horse_no}")
        if horse.python_rank != python.python_rank or horse.ai_rank != independent.ai_rank:
            raise AiIntegrationError(f"統合出力の元順位が入力と一致しません: {horse.horse_no}")
        if horse.integrated_score is None:
            raise AiIntegrationError(f"統合出力に統合scoreがありません: {horse.horse_no}")
        shift = abs(horse.integrated_rank - python.python_rank)
        if shift > input_data.max_exceptional_rank_shift:
            raise AiIntegrationError(f"統合順位の変更上限を超えています: {horse.horse_no}")
        exceptional_shift = exceptional_shift or shift > input_data.max_normal_rank_shift
        if independent.ai_rank is None and horse.decision_basis == "ai_priority":
            raise AiIntegrationError(f"AI順位不明の馬をAI優先にできません: {horse.horse_no}")
        ranks.append(horse.integrated_rank)
        scores_by_rank.append((horse.integrated_rank, horse.integrated_score))

    if seen != set(python_by_horse):
        raise AiIntegrationError("統合出力に不足している出走馬があります")
    if sorted(ranks) != list(range(1, len(ranks) + 1)):
        raise AiIntegrationError("統合順位は重複なしで1位から全頭分連続する必要があります")
    ordered_scores = [score for _rank, score in sorted(scores_by_rank)]
    if any(left <= right for left, right in zip(ordered_scores, ordered_scores[1:])):
        raise AiIntegrationError("統合scoreは統合順位に沿って同点なしの降順である必要があります")
    if (exceptional_shift or comparison.manual_review_required) and not output.manual_review_required:
        raise AiIntegrationError("大幅順位変更または重大不一致にはmanual reviewが必要です")


def _execute_with_retry(
    operation: Callable[[], Any],
    *,
    settings: Settings,
    sleeper: Callable[[float], None],
    on_retry: Callable[[int, int, BaseException], None],
) -> tuple[Any, int, int]:
    attempts = 0
    retries = 0
    delays = settings.ai_retry_delays_seconds or [2, 10]
    while True:
        attempts += 1
        try:
            return operation(), attempts, retries
        except Exception as exc:
            if retries >= settings.ai_max_retries or not _is_transient_ai_error(exc):
                raise
            delay = delays[min(retries, len(delays) - 1)]
            retries += 1
            on_retry(retries, delay, exc)
            sleeper(delay)


def _is_transient_ai_error(exc: BaseException) -> bool:
    if isinstance(exc, (ConnectionError, TimeoutError)):
        return True
    if exc.__class__.__name__ in {
        "APIConnectionError",
        "APITimeoutError",
        "RateLimitError",
        "InternalServerError",
    }:
        return True
    status_code = getattr(exc, "status_code", None)
    return isinstance(status_code, int) and (status_code == 429 or status_code >= 500)


def _record_retry(
    db: Session,
    job_run_id: str | None,
    stage: str,
    retry_number: int,
    delay: int,
    exc: BaseException,
) -> None:
    if job_run_id is None:
        return
    record_job_log(
        db,
        job_run_id=job_run_id,
        level="WARNING",
        event_code="AI_RETRY_SCHEDULED",
        message="temporary AI provider failure; bounded retry scheduled",
        context={
            "stage": stage,
            "retry_number": retry_number,
            "delay_seconds": delay,
            "error_type": exc.__class__.__name__,
        },
    )
    db.commit()


def _atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_bytes(canonical_json_bytes(payload))
    os.replace(temporary, path)


def _next_sequence(db: Session, race_id: str) -> int:
    latest = db.scalar(
        select(func.max(AiAnalysis.analysis_sequence)).where(AiAnalysis.race_id == race_id)
    )
    return int(latest or 0) + 1


def _prediction_race_date(db: Session, prediction_run_id: str):
    run = db.get(PredictionRun, prediction_run_id)
    return run.race_date if run is not None else None


def _confidence_value(value: str) -> float:
    return {"low": 0.33, "medium": 0.66, "high": 0.9}[value]


def _sum_optional(left: int | None, right: int | None) -> int | None:
    if left is None and right is None:
        return None
    return int(left or 0) + int(right or 0)


def _safe_text(value: Any, max_length: int) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text[:max_length] if text else None


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)
