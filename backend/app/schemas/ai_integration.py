from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from app.schemas.ai_independent import IndependentAnalysisResponse


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class PythonPredictionForComparison(_StrictModel):
    horse_no: int = Field(ge=1, le=40)
    horse_name: str = Field(min_length=1, max_length=255)
    python_rank: int = Field(ge=1, le=40)
    python_score: float
    estimated_in3_rate: float | None = Field(default=None, ge=0)
    expected_value: float | None = Field(default=None, ge=0)
    risk_flag: bool
    risk_score: float | None = Field(default=None, ge=0)
    risk_reason: str | None = Field(default=None, max_length=1000)
    evaluation_reason: str | None = Field(default=None, max_length=1200)


class ComparisonInput(_StrictModel):
    schema_version: Literal["ai_comparison_input_v1"] = "ai_comparison_input_v1"
    input_visibility: Literal["python_result_visible_after_independent_lock"] = (
        "python_result_visible_after_independent_lock"
    )
    independent_analysis_id: str = Field(min_length=1, max_length=36)
    independent_output_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    independent_result: IndependentAnalysisResponse
    python_prediction_run_id: str = Field(min_length=1, max_length=36)
    python_prediction_version: str = Field(min_length=1, max_length=128)
    python_model_version: str = Field(min_length=1, max_length=128)
    python_results: list[PythonPredictionForComparison] = Field(min_length=2, max_length=40)


class HorseComparison(_StrictModel):
    horse_no: int = Field(ge=1, le=40)
    horse_name: str = Field(min_length=1, max_length=255)
    python_rank: int = Field(ge=1, le=40)
    ai_rank: int | None = Field(default=None, ge=1, le=40)
    rank_gap: int | None = Field(default=None, ge=-39, le=39)
    agreement_level: Literal["exact", "small_difference", "material_difference", "unknown"]
    python_view: str = Field(min_length=5, max_length=1000)
    ai_view: str = Field(min_length=5, max_length=1000)
    counterpoints: list[str] = Field(default_factory=list, max_length=8)
    material_opposition: bool
    uncertainty: str = Field(min_length=1, max_length=1000)


class OppositionSummary(_StrictModel):
    has_material_opposition: bool
    horse_nos: list[int] = Field(default_factory=list, max_length=40)
    summary: str = Field(min_length=5, max_length=1600)


class ComparisonResponse(_StrictModel):
    schema_version: Literal["ai_comparison_result_v1"] = "ai_comparison_result_v1"
    race_id: str = Field(min_length=1, max_length=32)
    independent_analysis_id: str = Field(min_length=1, max_length=36)
    python_prediction_run_id: str = Field(min_length=1, max_length=36)
    overall_alignment: Literal["high", "medium", "low", "unknown"]
    data_confidence: Literal["low", "medium", "high"]
    manual_review_required: bool
    horses: list[HorseComparison] = Field(min_length=2, max_length=40)
    opposition: OppositionSummary
    summary: str = Field(min_length=5, max_length=2000)


class IntegrationInput(_StrictModel):
    schema_version: Literal["ai_integration_input_v1"] = "ai_integration_input_v1"
    comparison_input: ComparisonInput
    comparison_output_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    comparison_result: ComparisonResponse
    max_normal_rank_shift: Literal[2] = 2
    max_exceptional_rank_shift: Literal[4] = 4


class IntegratedHorsePrediction(_StrictModel):
    horse_no: int = Field(ge=1, le=40)
    horse_name: str = Field(min_length=1, max_length=255)
    python_rank: int = Field(ge=1, le=40)
    ai_rank: int | None = Field(default=None, ge=1, le=40)
    integrated_rank: int = Field(ge=1, le=40)
    # v1.0.0成果物の読取互換性は保つが、新規runではservice検証により必須とする。
    integrated_score: float | None = Field(default=None, ge=0, le=100)
    decision_basis: Literal["python_priority", "ai_priority", "balanced", "insufficient_data"]
    confidence: float = Field(ge=0, le=1)
    uncertainty_level: Literal["low", "medium", "high"]
    reasons: list[str] = Field(min_length=1, max_length=8)
    risk_summary: str = Field(min_length=1, max_length=1000)


class IntegrationResponse(_StrictModel):
    schema_version: Literal["ai_integration_result_v1"] = "ai_integration_result_v1"
    race_id: str = Field(min_length=1, max_length=32)
    independent_analysis_id: str = Field(min_length=1, max_length=36)
    python_prediction_run_id: str = Field(min_length=1, max_length=36)
    integration_strategy: Literal[
        "python_priority",
        "ai_priority",
        "balanced",
        "no_decision",
    ]
    data_confidence: Literal["low", "medium", "high"]
    manual_review_required: bool
    horses: list[IntegratedHorsePrediction] = Field(min_length=2, max_length=40)
    key_disagreements: list[str] = Field(default_factory=list, max_length=12)
    uncertainties: list[str] = Field(default_factory=list, max_length=20)
    final_comment: str = Field(min_length=5, max_length=2000)
