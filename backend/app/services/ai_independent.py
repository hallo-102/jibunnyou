from __future__ import annotations

import json
import os
import time
from datetime import date, datetime, timezone
from hashlib import sha256
from pathlib import Path
from typing import Any, Callable

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.core.config import Settings, get_settings
from app.core.logging import redact_text
from app.db.models import (
    AiAnalysis,
    AiAnalysisOutput,
    HorsePastPerformance,
    JobRun,
    Race,
    RaceEntry,
    RaceQualityStatus,
)
from app.legacy_bridge.normalization import normalize_horse_name
from app.schemas.ai_independent import (
    AiDataQualityInput,
    AiPastPerformanceInput,
    AiRaceContextInput,
    AiRunnerInput,
    AiRunnerMarketInput,
    AiRunnerQualitativeInput,
    IndependentAnalysisInput,
    IndependentAnalysisResponse,
)
from app.services.ai_provider import (
    INDEPENDENT_PROMPT_SHA256,
    INDEPENDENT_PROMPT_VERSION,
    AiProviderResult,
    IndependentAiProvider,
    create_independent_ai_provider,
)
from app.services.history import record_job_log, register_artifact


FORBIDDEN_INDEPENDENT_INPUT_KEYS = {
    "python",
    "prediction",
    "predictionrank",
    "predictionscore",
    "enginerank",
    "enginescore",
    "estimatedin3",
    "expectedvalue",
    "riskflag",
    "riskscore",
    "riskreason",
    "dangerjudgement",
    "valuejudgement",
    "betdecision",
    "betrole",
    "finalrank",
    "finalscore",
    "予想順位",
    "予想スコア",
    "推定馬券内率",
    "期待値",
    "危険馬",
    "買い目",
    "見送り判定",
    "本命印",
}


class AiIndependentError(RuntimeError):
    """Raised when an independent analysis cannot be completed safely."""


def canonical_json_bytes(payload: dict[str, Any]) -> bytes:
    """Serialize a versioned AI artifact deterministically for hashing."""

    return json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8")


def payload_sha256(payload: dict[str, Any]) -> str:
    return sha256(canonical_json_bytes(payload)).hexdigest()


def assert_python_hidden(payload: Any, path: str = "$") -> None:
    """Fail closed if a Python prediction field reaches the independent input."""

    if isinstance(payload, dict):
        for key, value in payload.items():
            normalized = "".join(character for character in str(key).lower() if character.isalnum())
            if any(blocked in normalized for blocked in FORBIDDEN_INDEPENDENT_INPUT_KEYS):
                raise AiIndependentError(f"独立AI入力へ禁止項目が混入しました: {path}.{key}")
            assert_python_hidden(value, f"{path}.{key}")
    elif isinstance(payload, list):
        for index, value in enumerate(payload):
            assert_python_hidden(value, f"{path}[{index}]")


def build_independent_input(db: Session, race_id: str) -> IndependentAnalysisInput:
    """Build an allowlist-only snapshot without reading prediction result columns."""

    race = db.get(Race, race_id)
    if race is None:
        raise AiIndependentError(f"対象レースが見つかりません: race_id={race_id}")
    if race.race_date is None:
        raise AiIndependentError("独立AI分析にはrace_dateが必要です")

    entries = list(
        db.scalars(
            select(RaceEntry)
            .where(RaceEntry.race_id == race_id)
            .order_by(RaceEntry.horse_no)
        )
    )
    if len(entries) < 2:
        raise AiIndependentError("独立AI分析には2頭以上の出走馬が必要です")

    race_raw = race.raw if isinstance(race.raw, dict) else {}
    runners = [
        _runner_input(db, entry=entry, target_date=race.race_date)
        for entry in entries
    ]
    quality = db.scalar(
        select(RaceQualityStatus).where(RaceQualityStatus.race_id == race_id)
    )
    missing_sections: list[str] = []
    if not any(runner.past_performances for runner in runners):
        missing_sections.append("past_performances")
    if not any(runner.qualitative.training_summary for runner in runners):
        missing_sections.append("training")
    if all(runner.market.win_odds is None for runner in runners):
        missing_sections.append("odds")

    snapshot = IndependentAnalysisInput(
        race=AiRaceContextInput(
            race_id=race.race_id,
            race_date=race.race_date,
            race_number=race.race_number,
            venue=race.venue,
            race_name=race.name,
            start_time=race.start_time,
            course=race.course,
            distance=_safe_text(_pick(race_raw, "距離", "distance"), 64),
            surface=_safe_text(_pick(race_raw, "芝ダ", "surface", "コース種別"), 32),
            track_condition=race.track_condition,
            weather=_safe_text(_pick(race_raw, "天候", "weather"), 64),
            race_type=race.race_type,
            race_class=race.race_class,
            headcount=race.headcount or len(entries),
        ),
        runners=runners,
        data_quality=AiDataQualityInput(
            status=quality.status if quality is not None else "GRAY",
            issue_count=quality.issue_count if quality is not None else 0,
            red_count=quality.red_count if quality is not None else 0,
            yellow_count=quality.yellow_count if quality is not None else 0,
            summary=quality.summary if quality is not None else "品質検査結果がありません",
            missing_sections=missing_sections,
        ),
    )
    payload = snapshot.model_dump(mode="json", exclude_none=False)
    assert_python_hidden(payload)
    return snapshot


def run_independent_analysis(
    db: Session,
    *,
    race_id: str,
    job_run_id: str | None = None,
    rerun_reason: str | None = None,
    provider: IndependentAiProvider | None = None,
    settings: Settings | None = None,
    sleeper: Callable[[float], None] = time.sleep,
) -> dict[str, Any]:
    """Create, validate, lock, and persist one independent AI result."""

    resolved_settings = settings or get_settings()
    input_snapshot = build_independent_input(db, race_id)
    analysis = AiAnalysis(
        race_id=race_id,
        race_date=input_snapshot.race.race_date,
        analysis_sequence=_next_sequence(db, race_id),
        status="running",
        model_name=(provider.model_name if provider is not None else resolved_settings.ai_model),
        prompt_version=INDEPENDENT_PROMPT_VERSION,
        input_data_version=input_snapshot.schema_version,
        rerun_reason=_safe_text(rerun_reason, 1000),
        started_at=_utc_now(),
    )
    db.add(analysis)
    db.commit()
    db.refresh(analysis)

    started = time.perf_counter()
    run_dir = resolved_settings.exports_dir / "ai" / analysis.id
    try:
        input_payload = input_snapshot.model_dump(mode="json", exclude_none=False)
        input_hash = payload_sha256(input_payload)
        input_path = run_dir / "ai_independent_input_v1.json"
        _atomic_write_json(input_path, input_payload)
        input_artifact = register_artifact(
            db,
            path=input_path,
            artifact_kind="ai_independent_input",
            logical_name="ai_independent_input_v1.json",
            job_run_id=job_run_id,
            content_type="application/json",
            metadata={
                "analysis_id": analysis.id,
                "schema_version": input_snapshot.schema_version,
                "python_result_visible": False,
                "prompt_version": INDEPENDENT_PROMPT_VERSION,
                "prompt_sha256": INDEPENDENT_PROMPT_SHA256,
            },
        )
        analysis.input_snapshot_hash = input_hash
        analysis.input_artifact_id = input_artifact.id
        db.commit()

        active_provider = provider or create_independent_ai_provider(resolved_settings)
        result, attempts, retries = _analyze_with_retry(
            active_provider,
            input_snapshot,
            max_retries=resolved_settings.ai_max_retries,
            retry_delays=resolved_settings.ai_retry_delays_seconds,
            sleeper=sleeper,
            on_retry=lambda retry_number, delay, exc: _record_retry(
                db,
                job_run_id,
                retry_number,
                delay,
                exc,
            ),
        )
        _validate_result(input_snapshot, result.output)

        output_payload = result.output.model_dump(mode="json", exclude_none=False)
        output_hash = payload_sha256(output_payload)
        output_path = run_dir / "ai_independent_result_v1.json"
        _atomic_write_json(output_path, output_payload)
        register_artifact(
            db,
            path=output_path,
            artifact_kind="ai_independent_result",
            logical_name="ai_independent_result_v1.json",
            job_run_id=job_run_id,
            content_type="application/json",
            metadata={
                "analysis_id": analysis.id,
                "schema_version": result.output.schema_version,
                "python_result_visible": False,
                "provider_response_id": result.provider_response_id,
            },
        )
        db.add(
            AiAnalysisOutput(
                analysis_id=analysis.id,
                stage="independent",
                output_schema_version=result.output.schema_version,
                output_json=output_payload,
                output_hash=output_hash,
                confidence=_confidence_value(result.output.data_confidence),
                python_result_visible=False,
                is_locked=True,
                locked_at=_utc_now(),
            )
        )
        analysis.status = "succeeded"
        analysis.model_name = active_provider.model_name
        analysis.prompt_tokens = result.prompt_tokens
        analysis.completion_tokens = result.completion_tokens
        analysis.duration_ms = round((time.perf_counter() - started) * 1000)
        analysis.finished_at = _utc_now()
        db.commit()
        return {
            "analysis_id": analysis.id,
            "race_id": race_id,
            "status": analysis.status,
            "model_name": analysis.model_name,
            "prompt_version": analysis.prompt_version,
            "prompt_sha256": INDEPENDENT_PROMPT_SHA256,
            "input_snapshot_hash": input_hash,
            "output_hash": output_hash,
            "attempts": attempts,
            "retries": retries,
            "decision_status": result.output.decision_status,
            "data_confidence": result.output.data_confidence,
            "runner_count": len(result.output.runners),
        }
    except Exception as exc:
        db.rollback()
        failed = db.get(AiAnalysis, analysis.id)
        if failed is not None:
            failed.status = "failed"
            failed.error_message = redact_text(f"{exc.__class__.__name__}: {exc}")
            failed.duration_ms = round((time.perf_counter() - started) * 1000)
            failed.finished_at = _utc_now()
            db.commit()
        raise AiIndependentError(_user_facing_ai_error(exc)) from exc


def execute_queued_ai_job(job_id: str) -> dict[str, str]:
    """Execute one persisted AI pipeline job in a worker-owned session."""

    from app.db.session import SessionLocal
    from app.services.data_quality import has_blocking_quality_status

    with SessionLocal() as db:
        job = db.get(JobRun, job_id)
        if job is None:
            raise LookupError(f"queued AI job was not found: {job_id}")
        if job.job_type not in {"ai.independent", "ai.compare_integrate"}:
            raise AiIndependentError(f"job is not a supported AI job: {job.job_type}")
        if job.status not in {"queued", "running"}:
            return {"job_id": job.id, "status": job.status}

        job.status = "running"
        job.started_at = datetime.utcnow()
        record_job_log(
            db,
            job_run_id=job.id,
            level="INFO",
            event_code="JOB_STARTED",
            message="AI worker started job execution",
            context={"job_type": job.job_type, "queue": "ai"},
        )
        db.commit()
        try:
            if not job.race_id:
                raise AiIndependentError(f"{job.job_type}にはrace_idが必要です")
            if has_blocking_quality_status(db, race_date=job.race_date, race_id=job.race_id):
                raise AiIndependentError("REDのデータ品質状態があるため、AI分析を実行できません")
            if job.job_type == "ai.independent":
                summary = run_independent_analysis(
                    db,
                    race_id=job.race_id,
                    job_run_id=job.id,
                    rerun_reason=(job.params or {}).get("rerun_reason"),
                )
            else:
                # 比較・統合サービスは独立結果の固定hashを再検証してからPython結果を開示する。
                from app.services.ai_integration import execute_queued_comparison_job

                summary = execute_queued_comparison_job(db, job)
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
                    "AI job execution completed"
                    if job.status == "completed"
                    else "AI job execution failed"
                ),
                context={"status": job.status, "queue": "ai"},
            )
            db.add(job)
            db.commit()
        return {"job_id": job.id, "status": job.status}


def _runner_input(db: Session, *, entry: RaceEntry, target_date: date) -> AiRunnerInput:
    raw = entry.raw if isinstance(entry.raw, dict) else {}
    past_rows = list(
        db.scalars(
            select(HorsePastPerformance)
            .where(
                HorsePastPerformance.target_race_id == entry.race_id,
                HorsePastPerformance.horse_name == entry.horse_name,
                HorsePastPerformance.race_date.is_not(None),
                HorsePastPerformance.race_date < target_date,
            )
            .order_by(HorsePastPerformance.race_date.desc(), HorsePastPerformance.id.desc())
            .limit(5)
        )
    )
    return AiRunnerInput(
        horse_no=entry.horse_no,
        frame_no=entry.frame_no,
        horse_name=entry.horse_name,
        age=entry.age,
        carried_weight=entry.carried_weight,
        jockey=entry.jockey,
        trainer=entry.trainer,
        market=AiRunnerMarketInput(
            popularity=entry.popularity,
            win_odds=entry.win_odds,
            place_odds=entry.place_odds,
        ),
        qualitative=AiRunnerQualitativeInput(
            sex_age=_safe_text(_pick(raw, "性齢", "sex_age", "性別"), 32),
            body_weight=_safe_text(_pick(raw, "馬体重", "body_weight"), 64),
            running_style=_safe_text(_pick(raw, "脚質", "running_style"), 64),
            training_summary=_safe_text(
                _pick(raw, "調教要約", "調教評価", "training_summary"),
                600,
            ),
            condition_summary=_safe_text(_pick(raw, "近況", "状態", "condition_summary"), 600),
            trainer_comment=_safe_text(
                _pick(raw, "厩舎 コメント", "関係者コメント", "trainer_comment"),
                600,
            ),
            bloodline_summary=_safe_text(
                _pick(raw, "血統", "血統要約", "bloodline_summary", "父", "母父"),
                600,
            ),
        ),
        past_performances=[_past_input(row) for row in past_rows],
    )


def _past_input(row: HorsePastPerformance) -> AiPastPerformanceInput:
    if row.race_date is None:
        raise AiIndependentError("過去走日付がありません")
    raw = row.raw if isinstance(row.raw, dict) else {}
    return AiPastPerformanceInput(
        race_date=row.race_date,
        race_name=row.race_name,
        finish_position=row.finish_position,
        popularity=row.popularity,
        odds=row.odds,
        distance=row.distance,
        jockey=row.jockey,
        course=_safe_text(_pick(raw, "コース", "course"), 64),
        track_condition=_safe_text(_pick(raw, "馬場", "track_condition"), 64),
        running_position=_safe_text(_pick(raw, "通過", "通過順位", "corner_position"), 128),
        margin=_safe_text(_pick(raw, "着差", "margin"), 64),
    )


def _validate_result(
    input_snapshot: IndependentAnalysisInput,
    output: IndependentAnalysisResponse,
) -> None:
    if output.race_id != input_snapshot.race.race_id:
        raise AiIndependentError("AI出力のrace_idが入力と一致しません")

    expected = {runner.horse_no: runner.horse_name for runner in input_snapshot.runners}
    actual: dict[int, str] = {}
    for runner in output.runners:
        if runner.horse_no in actual:
            raise AiIndependentError(f"AI出力に馬番の重複があります: {runner.horse_no}")
        actual[runner.horse_no] = runner.horse_name
        expected_name = expected.get(runner.horse_no)
        if expected_name is None:
            raise AiIndependentError(f"AI出力に存在しない馬番があります: {runner.horse_no}")
        if normalize_horse_name(expected_name) != normalize_horse_name(runner.horse_name):
            raise AiIndependentError(f"AI出力の馬番・馬名が一致しません: {runner.horse_no}")
        if (
            runner.rank_range_low is not None
            and runner.rank_range_high is not None
            and runner.rank_range_low > runner.rank_range_high
        ):
            raise AiIndependentError(f"AI出力の順位範囲が逆転しています: {runner.horse_no}")

    if set(actual) != set(expected):
        missing = sorted(set(expected) - set(actual))
        raise AiIndependentError(f"AI出力に不足している馬番があります: {missing}")

    ranks = [runner.ai_rank for runner in output.runners if runner.ai_rank is not None]
    if len(ranks) != len(set(ranks)):
        raise AiIndependentError("AI順位に重複があります")
    if sorted(ranks) != list(range(1, len(ranks) + 1)):
        raise AiIndependentError("AI順位は1位から連続している必要があります")
    if output.decision_status == "completed" and len(ranks) != len(output.runners):
        raise AiIndependentError("completedのAI出力には全頭の順位が必要です")
    if output.decision_status == "insufficient_data" and not output.unknowns:
        raise AiIndependentError("情報不足時はunknownsが必要です")


def _analyze_with_retry(
    provider: IndependentAiProvider,
    input_snapshot: IndependentAnalysisInput,
    *,
    max_retries: int,
    retry_delays: list[int],
    sleeper: Callable[[float], None],
    on_retry: Callable[[int, int, BaseException], None],
) -> tuple[AiProviderResult, int, int]:
    attempts = 0
    retries = 0
    delays = retry_delays or [2, 10]
    while True:
        attempts += 1
        try:
            return provider.analyze(input_snapshot), attempts, retries
        except Exception as exc:
            if retries >= max_retries or not _is_transient_ai_error(exc):
                raise
            delay = delays[min(retries, len(delays) - 1)]
            retries += 1
            on_retry(retries, delay, exc)
            sleeper(delay)


def _is_transient_ai_error(exc: BaseException) -> bool:
    if _is_insufficient_quota_error(exc):
        # 残高・利用枠不足は時間待ちの再試行では解消しないため、API消費を増やさず即時停止する。
        return False
    if isinstance(exc, (ConnectionError, TimeoutError)):
        return True
    class_name = exc.__class__.__name__
    if class_name in {"APIConnectionError", "APITimeoutError", "RateLimitError", "InternalServerError"}:
        return True
    status_code = getattr(exc, "status_code", None)
    return isinstance(status_code, int) and (status_code == 429 or status_code >= 500)


def _is_insufficient_quota_error(exc: BaseException) -> bool:
    message = str(exc).lower()
    return "insufficient_quota" in message or "exceeded your current quota" in message


def _user_facing_ai_error(exc: BaseException) -> str:
    message = redact_text(str(exc))
    lowered = message.lower()
    if _is_insufficient_quota_error(exc):
        return (
            "OpenAI APIの利用枠がありません。API Platform側のBillingで支払い方法・残高・"
            "利用上限を確認してください。ChatGPTの契約とは別管理です。"
        )
    if "invalid_api_key" in lowered or "incorrect api key" in lowered:
        return "OpenAI APIキーが無効です。.envのOPENAI_API_KEYを確認してサービスを再起動してください。"
    if "rate_limit" in lowered or getattr(exc, "status_code", None) == 429:
        return "OpenAI APIの一時的なレート上限に達しました。少し待ってから再実行してください。"
    if isinstance(exc, TimeoutError) or "timeout" in lowered:
        return "OpenAI APIが時間内に応答しませんでした。通信状態を確認して再実行してください。"
    return message


def _record_retry(
    db: Session,
    job_run_id: str | None,
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
            "retry_number": retry_number,
            "delay_seconds": delay,
            "error_type": exc.__class__.__name__,
        },
    )
    db.commit()


def _next_sequence(db: Session, race_id: str) -> int:
    latest = db.scalar(
        select(func.max(AiAnalysis.analysis_sequence)).where(AiAnalysis.race_id == race_id)
    )
    return int(latest or 0) + 1


def _atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_bytes(canonical_json_bytes(payload))
    os.replace(temporary, path)


def _pick(payload: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        value = payload.get(key)
        if value is not None and str(value).strip() not in {"", "nan", "None"}:
            return value
    return None


def _safe_text(value: Any, max_length: int) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text or text.lower() in {"nan", "none"}:
        return None
    return text[:max_length]


def _confidence_value(value: str) -> float:
    return {"low": 0.33, "medium": 0.66, "high": 0.9}[value]


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)
