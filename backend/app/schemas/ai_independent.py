from __future__ import annotations

from datetime import date
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


class _StrictModel(BaseModel):
    """Reject fields that are not part of the versioned AI contract."""

    model_config = ConfigDict(extra="forbid")


class AiPastPerformanceInput(_StrictModel):
    race_date: date
    race_name: str | None = Field(default=None, max_length=255)
    finish_position: int | None = Field(default=None, ge=1, le=99)
    popularity: int | None = Field(default=None, ge=1, le=99)
    odds: float | None = Field(default=None, ge=0)
    distance: str | None = Field(default=None, max_length=64)
    jockey: str | None = Field(default=None, max_length=128)
    course: str | None = Field(default=None, max_length=64)
    track_condition: str | None = Field(default=None, max_length=64)
    running_position: str | None = Field(default=None, max_length=128)
    margin: str | None = Field(default=None, max_length=64)


class AiRunnerQualitativeInput(_StrictModel):
    sex_age: str | None = Field(default=None, max_length=32)
    body_weight: str | None = Field(default=None, max_length=64)
    running_style: str | None = Field(default=None, max_length=64)
    training_summary: str | None = Field(default=None, max_length=600)
    condition_summary: str | None = Field(default=None, max_length=600)
    trainer_comment: str | None = Field(default=None, max_length=600)
    bloodline_summary: str | None = Field(default=None, max_length=600)


class AiRunnerMarketInput(_StrictModel):
    popularity: int | None = Field(default=None, ge=1, le=99)
    win_odds: float | None = Field(default=None, ge=0)
    place_odds: float | None = Field(default=None, ge=0)


class AiRunnerInput(_StrictModel):
    horse_no: int = Field(ge=1, le=40)
    frame_no: int | None = Field(default=None, ge=1, le=8)
    horse_name: str = Field(min_length=1, max_length=255)
    age: int | None = Field(default=None, ge=2, le=30)
    carried_weight: float | None = Field(default=None, ge=40, le=80)
    jockey: str | None = Field(default=None, max_length=128)
    trainer: str | None = Field(default=None, max_length=128)
    market: AiRunnerMarketInput
    qualitative: AiRunnerQualitativeInput
    past_performances: list[AiPastPerformanceInput] = Field(default_factory=list, max_length=5)


class AiRaceContextInput(_StrictModel):
    race_id: str = Field(min_length=1, max_length=32)
    race_date: date
    race_number: int | None = Field(default=None, ge=1, le=12)
    venue: str | None = Field(default=None, max_length=64)
    race_name: str | None = Field(default=None, max_length=255)
    start_time: str | None = Field(default=None, max_length=32)
    course: str | None = Field(default=None, max_length=64)
    distance: str | None = Field(default=None, max_length=64)
    surface: str | None = Field(default=None, max_length=32)
    track_condition: str | None = Field(default=None, max_length=64)
    weather: str | None = Field(default=None, max_length=64)
    race_type: str | None = Field(default=None, max_length=64)
    race_class: str | None = Field(default=None, max_length=64)
    headcount: int | None = Field(default=None, ge=1, le=40)


class AiDataQualityInput(_StrictModel):
    status: Literal["GREEN", "YELLOW", "RED", "GRAY"]
    issue_count: int = Field(default=0, ge=0)
    red_count: int = Field(default=0, ge=0)
    yellow_count: int = Field(default=0, ge=0)
    summary: str | None = Field(default=None, max_length=1000)
    missing_sections: list[str] = Field(default_factory=list, max_length=20)


class IndependentAnalysisInput(_StrictModel):
    schema_version: Literal["ai_independent_input_v1"] = "ai_independent_input_v1"
    input_visibility: Literal["python_result_hidden"] = "python_result_hidden"
    race: AiRaceContextInput
    runners: list[AiRunnerInput] = Field(min_length=2, max_length=40)
    data_quality: AiDataQualityInput


class AiRaceAssessment(_StrictModel):
    pace_assessment: Literal["slow", "middle", "fast", "uncertain"]
    track_bias_assessment: str = Field(min_length=1, max_length=500)
    main_risks: list[str] = Field(default_factory=list, max_length=8)
    summary: str = Field(min_length=5, max_length=1200)


class IndependentHorseEvaluation(_StrictModel):
    horse_no: int = Field(ge=1, le=40)
    horse_name: str = Field(min_length=1, max_length=255)
    ai_rank: int | None = Field(default=None, ge=1, le=40)
    rank_range_low: int | None = Field(default=None, ge=1, le=40)
    rank_range_high: int | None = Field(default=None, ge=1, le=40)
    confidence: float = Field(ge=0, le=1)
    risk_level: Literal["low", "medium", "high", "unknown"]
    positive_factors: list[str] = Field(default_factory=list, max_length=8)
    negative_factors: list[str] = Field(default_factory=list, max_length=8)
    uncertainties: list[str] = Field(default_factory=list, max_length=8)
    rationale: str = Field(min_length=5, max_length=1200)


class IndependentAnalysisResponse(_StrictModel):
    schema_version: Literal["ai_independent_result_v1"] = "ai_independent_result_v1"
    race_id: str = Field(min_length=1, max_length=32)
    decision_status: Literal["completed", "insufficient_data"]
    data_confidence: Literal["low", "medium", "high"]
    manual_review_required: bool
    race_assessment: AiRaceAssessment
    runners: list[IndependentHorseEvaluation] = Field(min_length=2, max_length=40)
    unknowns: list[str] = Field(default_factory=list, max_length=20)
    final_comment: str = Field(min_length=5, max_length=1600)
