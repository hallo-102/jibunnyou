from datetime import date, datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from app.schemas.ai_independent import IndependentAnalysisResponse
from app.schemas.ai_integration import ComparisonResponse, IntegrationResponse


class HealthResponse(BaseModel):
    status: str
    database: str
    redis: str
    app: str


class ReadinessResponse(BaseModel):
    status: str
    checks: dict[str, str]


class VersionResponse(BaseModel):
    app: str
    version: str
    environment: str
    git_commit: str
    database_revision: str | None = None


class RaceDayRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    race_date: date
    status: str
    source: str | None = None
    created_at: datetime
    updated_at: datetime


class RaceWorkbookFileRead(BaseModel):
    file_name: str
    race_date: date
    size_bytes: int
    modified_at: datetime
    is_imported: bool = False


class RaceWorkbookSelectRequest(BaseModel):
    file_name: str = Field(min_length=1, max_length=255)


class RaceEntryBase(BaseModel):
    horse_no: int = Field(ge=1, le=40)
    horse_name: str
    frame_no: int | None = None
    age: int | None = None
    carried_weight: float | None = None
    jockey: str | None = None
    trainer: str | None = None
    popularity: int | None = None
    win_odds: float | None = None
    place_odds: float | None = None


class RaceEntryCreate(RaceEntryBase):
    pass


class RaceEntryRead(RaceEntryBase):
    model_config = ConfigDict(from_attributes=True)

    id: int
    race_id: str
    horse_id: int | None = None
    prediction_rank: int | None = None
    prediction_score: float | None = None
    estimated_in3_rate: float | None = None
    expected_value: float | None = None


class RaceCreate(BaseModel):
    race_id: str
    race_date: date | None = None
    race_number: int | None = None
    venue: str | None = None
    name: str | None = None
    start_time: str | None = None
    course: str | None = None
    track_condition: str | None = None
    race_type: str | None = None
    race_class: str | None = None
    headcount: int | None = None


class RaceRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    race_id: str
    race_date: date | None = None
    race_number: int | None = None
    venue: str | None = None
    name: str | None = None
    start_time: str | None = None
    course: str | None = None
    track_condition: str | None = None
    race_type: str | None = None
    race_class: str | None = None
    headcount: int | None = None
    created_at: datetime
    updated_at: datetime


class RaceDetail(RaceRead):
    entries: list[RaceEntryRead] = []


class JobCreate(BaseModel):
    job_type: str
    race_date: date | None = None
    race_id: str | None = None
    force: bool = False
    params: dict[str, Any] | None = None


class JobRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    job_type: str
    status: str
    race_date: date | None = None
    race_id: str | None = None
    force: bool
    params: dict[str, Any] | None = None
    message: str | None = None
    created_at: datetime
    started_at: datetime | None = None
    finished_at: datetime | None = None


class PredictionRunRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    status: str
    race_date: date | None = None
    race_id: str | None = None
    prediction_version: str
    feature_version: str
    weight_version: str
    model_version: str
    source_file: str | None = None
    output_file: str | None = None
    result_json_file: str | None = None
    manifest_file: str | None = None
    result_count: int
    matched_count: int
    mismatch_count: int
    message: str | None = None
    started_at: datetime | None = None
    finished_at: datetime | None = None
    created_at: datetime


class PredictionResultRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    prediction_run_id: str
    race_id: str
    horse_no: int
    horse_name: str
    popularity: int | None = None
    win_odds: float | None = None
    place_odds: float | None = None
    prediction_rank: int | None = None
    prediction_score: float | None = None
    estimated_in3_rate: float | None = None
    expected_value: float | None = None
    risk_flag: bool
    risk_score: float | None = None
    risk_reason: str | None = None
    evaluation_reason: str | None = None
    feature_summary: dict[str, Any] | None = None
    created_at: datetime


class PredictionRaceStatusRead(BaseModel):
    race_id: str
    latest_run_id: str
    status: str
    result_count: int
    predicted_at: datetime | None = None
    top_horse_no: int | None = None
    top_horse_name: str | None = None
    top_score: float | None = None


class PredictionImportSummary(BaseModel):
    prediction_run_id: str
    race_date: date | None = None
    race_id: str | None = None
    source_file: str
    output_file: str
    result_json_file: str
    manifest_file: str
    results: int = 0
    matched: int = 0
    mismatches: int = 0


class AiRunRequest(BaseModel):
    race_date: date | None = None
    race_id: str | None = None
    prediction_run_id: str | None = None
    ai_run_id: str | None = None
    model_name: str | None = None
    prompt_version: str | None = None


class AiRunSummary(BaseModel):
    race_date: date | None = None
    race_id: str | None = None
    ai_mode: str
    runs: int = 0
    evaluations: int = 0
    final_predictions: int = 0
    strategies: int = 0
    warnings: list[str] = []


class AiIndependentRunRequest(BaseModel):
    race_id: str = Field(min_length=1, max_length=32)
    race_date: date | None = None
    force: bool = False
    rerun_reason: str | None = Field(default=None, max_length=1000)


class AiIndependentAnalysisRead(BaseModel):
    id: str
    race_id: str
    race_date: date | None = None
    analysis_sequence: int
    status: str
    model_name: str
    prompt_version: str
    input_data_version: str | None = None
    input_snapshot_hash: str | None = None
    prompt_tokens: int | None = None
    completion_tokens: int | None = None
    duration_ms: int | None = None
    error_message: str | None = None
    rerun_reason: str | None = None
    started_at: datetime | None = None
    finished_at: datetime | None = None
    created_at: datetime
    output: IndependentAnalysisResponse | None = None
    output_hash: str | None = None
    output_locked: bool = False


class AiComparisonIntegrationRunRequest(BaseModel):
    race_id: str = Field(min_length=1, max_length=32)
    race_date: date | None = None
    independent_analysis_id: str | None = Field(default=None, min_length=1, max_length=36)
    prediction_run_id: str | None = Field(default=None, min_length=1, max_length=36)
    force: bool = False
    rerun_reason: str | None = Field(default=None, max_length=1000)


class AiIntegrationAnalysisRead(BaseModel):
    id: str
    race_id: str
    race_date: date | None = None
    prediction_run_id: str | None = None
    independent_analysis_id: str | None = None
    analysis_sequence: int
    status: str
    model_name: str
    prompt_version: str
    input_data_version: str | None = None
    input_snapshot_hash: str | None = None
    prompt_tokens: int | None = None
    completion_tokens: int | None = None
    duration_ms: int | None = None
    error_message: str | None = None
    rerun_reason: str | None = None
    started_at: datetime | None = None
    finished_at: datetime | None = None
    created_at: datetime
    comparison: ComparisonResponse | None = None
    comparison_output_hash: str | None = None
    comparison_locked: bool = False
    integration: IntegrationResponse | None = None
    integration_output_hash: str | None = None
    integration_locked: bool = False


class AiPredictionRunRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    race_id: str
    race_date: date | None = None
    ai_mode: str
    model_name: str
    prompt_version: str
    python_prediction_run_id: str | None = None
    status: str
    race_summary: str | None = None
    pace_prediction: str | None = None
    python_trust_level: str | None = None
    raw_request: dict[str, Any] | None = None
    raw_response: dict[str, Any] | None = None
    error_message: str | None = None
    created_at: datetime
    finished_at: datetime | None = None


class AiHorseEvaluationRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    ai_run_id: str
    race_id: str
    horse_no: int
    horse_name: str
    python_rank: int | None = None
    ai_rank: int | None = None
    ai_action: str
    ai_adjust_score: float
    ai_reason: str
    ai_risk: str | None = None
    ai_bet_role: str
    ai_confidence: float | None = None
    created_at: datetime


class FinalPredictionRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    ai_run_id: str
    race_id: str
    horse_no: int
    horse_name: str
    python_rank: int | None = None
    python_score: float | None = None
    ai_rank: int | None = None
    ai_adjust_score: float
    final_score: float
    final_rank: int | None = None
    final_bet_role: str
    created_at: datetime


class AiBetStrategyRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    ai_run_id: str
    race_id: str
    strategy_type: str
    main_axis: list[int] | None = None
    second_axis_candidates: list[int] | None = None
    main_opponents: list[int] | None = None
    wide_opponents: list[int] | None = None
    exclude_candidates: list[int] | None = None
    suggestion: str | None = None
    bet_count: int
    created_at: datetime


class AiRaceStatusRead(BaseModel):
    race_id: str
    latest_run_id: str
    ai_mode: str
    status: str
    evaluations: int
    final_predictions: int
    has_upgrade: bool
    has_downgrade: bool
    python_rank_full_match: bool
    created_at: datetime | None = None


class BetCandidateRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    prediction_run_id: str
    source_type: str
    ai_analysis_id: str | None = None
    race_id: str
    race_date: date | None = None
    rank: str
    status: str
    bet_type: str
    strategy: str
    strategy_mode: str
    bet_rule_version: str
    axis_horse_nos: list[int] | None = None
    opponent_horse_nos: list[int] | None = None
    combinations: list[list[int]] | None = None
    points: int
    stake_per_point: int
    total_amount: int
    max_race_amount: int
    max_day_amount: int
    expected_value: float | None = None
    reason: str | None = None
    skip_reason: str | None = None
    warning_codes: list[str] = []
    requires_confirmation: bool
    purchase_execution_enabled: bool
    source_snapshot_hash: str | None = None
    created_at: datetime
    updated_at: datetime


class BetGenerateRequest(BaseModel):
    race_date: date | None = None
    race_id: str | None = None
    prediction_run_id: str | None = None
    source_modes: list[Literal["python", "ai_integrated"]] = Field(
        default_factory=lambda: ["python"],
        min_length=1,
        max_length=2,
    )
    bet_types: list[Literal["3連複", "ワイド"]] = Field(
        default_factory=lambda: ["3連複"],
        min_length=1,
        max_length=2,
    )
    strategy_modes: list[Literal["formation", "box", "wheel"]] = Field(
        default_factory=lambda: ["formation"],
        min_length=1,
        max_length=3,
    )
    ai_analysis_id: str | None = Field(default=None, min_length=1, max_length=36)
    stake_per_point: int = Field(default=500, ge=100, le=100000, multiple_of=100)
    max_race_amount: int = Field(default=3000, ge=100, le=1000000)
    max_day_amount: int = Field(default=12000, ge=100, le=10000000)
    max_points: int = Field(default=20, ge=1, le=500)
    allow_manual_review: bool = False

    @model_validator(mode="after")
    def validate_bet_limits(self):
        if len(set(self.source_modes)) != len(self.source_modes):
            raise ValueError("source_modesに重複があります")
        if len(set(self.bet_types)) != len(self.bet_types):
            raise ValueError("bet_typesに重複があります")
        if len(set(self.strategy_modes)) != len(self.strategy_modes):
            raise ValueError("strategy_modesに重複があります")
        if self.max_race_amount > self.max_day_amount:
            raise ValueError("1レース上限は1日上限以下にしてください")
        if self.stake_per_point > self.max_race_amount:
            raise ValueError("1点金額は1レース上限以下にしてください")
        return self


class BetGenerationSummary(BaseModel):
    race_date: date | None = None
    race_id: str | None = None
    prediction_run_id: str | None = None
    generated: int = 0
    candidates: int = 0
    skipped: int = 0
    blocked: int = 0
    review_required: int = 0
    total_planned_amount: int = 0
    warnings: list[str] = []


class BetStatusUpdate(BaseModel):
    status: str
    reason: str | None = None


class PayoutItem(BaseModel):
    bet_type: Literal["3連複", "ワイド"]
    combination: list[int] = Field(min_length=2, max_length=3)
    payout_per_100: int = Field(ge=0)
    status: Literal["normal", "refund"] = "normal"

    @model_validator(mode="after")
    def validate_combination(self):
        expected = 3 if self.bet_type == "3連複" else 2
        if len(self.combination) != expected:
            raise ValueError(f"{self.bet_type}の組合せは{expected}頭必要です")
        if len(set(self.combination)) != len(self.combination):
            raise ValueError("払戻組合せに馬番重複があります")
        self.combination = sorted(self.combination)
        return self


class RaceResultCreate(BaseModel):
    race_id: str
    race_date: date | None = None
    result_status: Literal["provisional", "confirmed", "cancelled"] = "confirmed"
    finish_order: list[int] = Field(default_factory=list, max_length=40)
    payout_amount: int = Field(default=0, ge=0)
    payout_type: str = "3連複"
    payouts: list[PayoutItem] = Field(default_factory=list, max_length=200)
    cancelled_horse_nos: list[int] = Field(default_factory=list, max_length=40)
    disqualified_horse_nos: list[int] = Field(default_factory=list, max_length=40)
    has_dead_heat: bool = False
    source_file: str | None = None
    raw: dict[str, Any] | None = None

    @model_validator(mode="after")
    def validate_result(self):
        if self.result_status == "confirmed" and len(self.finish_order) < 3:
            raise ValueError("confirmed結果には3着までの着順が必要です")
        if len(set(self.finish_order)) != len(self.finish_order):
            raise ValueError("着順に馬番重複があります。同着はrawとhas_dead_heatで記録してください")
        payout_keys = [(item.bet_type, tuple(item.combination)) for item in self.payouts]
        if len(payout_keys) != len(set(payout_keys)):
            raise ValueError("払戻情報に券種・組合せ重複があります")
        return self


class RaceResultRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    race_id: str
    race_date: date | None = None
    source_file: str | None = None
    finish_order: list[int]
    result_status: str
    payout_amount: int
    payout_type: str
    payouts_json: list[dict]
    cancelled_horse_nos: list[int]
    disqualified_horse_nos: list[int]
    has_dead_heat: bool
    confirmed_at: datetime | None = None
    raw: dict[str, Any] | None = None
    imported_at: datetime


class BetSettlementRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    bet_candidate_id: int
    race_id: str
    race_date: date | None = None
    bet_type: str
    source_type: str
    is_hit: bool
    hit_count: int
    winning_combinations: list[list[int]]
    payout_details_json: list[dict]
    result_status: str
    payout_amount: int
    stake_amount: int
    profit_loss: int
    roi: float
    message: str | None = None
    settled_at: datetime


class ReviewNoteCreate(BaseModel):
    race_id: str
    race_date: date | None = None
    bet_candidate_id: int | None = None
    prediction_run_id: str | None = None
    note: str
    ai_vs_result: dict[str, Any] | None = None


class ReviewNoteRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    race_id: str
    race_date: date | None = None
    bet_candidate_id: int | None = None
    prediction_run_id: str | None = None
    note: str
    ai_vs_result: dict[str, Any] | None = None
    created_at: datetime
    updated_at: datetime


class AnalyticsBreakdownRead(BaseModel):
    dimension: str
    value: str
    bets: int = 0
    settled_bets: int = 0
    hits: int = 0
    hit_rate: float = 0.0
    stake_amount: int = 0
    payout_amount: int = 0
    profit_loss: int = 0
    roi: float = 0.0


class AnalyticsSummaryRead(BaseModel):
    race_date: date | None = None
    date_from: date | None = None
    date_to: date | None = None
    bets: int = 0
    settled_bets: int = 0
    hits: int = 0
    hit_rate: float = 0.0
    stake_amount: int = 0
    payout_amount: int = 0
    profit_loss: int = 0
    roi: float = 0.0
    max_consecutive_losses: int = 0
    max_drawdown: int = 0
    breakdown: list[AnalyticsBreakdownRead] = []


class DataQualityIssueRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    severity: str
    code: str
    message: str
    source_file: str | None = None
    race_id: str | None = None
    row_number: int | None = None
    created_at: datetime


class NotificationRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    category: str
    severity: str
    title: str
    message: str
    source_type: str
    source_id: str
    race_id: str | None = None
    race_date: date | None = None
    action_anchor: str | None = None
    is_read: bool
    read_at: datetime | None = None
    created_at: datetime
    updated_at: datetime


class NotificationReadUpdate(BaseModel):
    is_read: bool


class NotificationSummaryRead(BaseModel):
    total_count: int = 0
    unread_count: int = 0
    error_count: int = 0
    warning_count: int = 0


class RaceQualityStatusRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    race_id: str
    status: str
    summary: str | None = None
    issue_count: int
    red_count: int
    yellow_count: int
    checked_at: datetime


class DataQualityCheckRequest(BaseModel):
    race_date: date | None = None
    race_id: str | None = None


class DataQualityRunSummary(BaseModel):
    race_date: date | None = None
    race_id: str | None = None
    checked_races: int = 0
    green: int = 0
    yellow: int = 0
    red: int = 0
    gray: int = 0
    issues_written: int = 0


class ImportSummaryRead(BaseModel):
    source_file: str
    race_date: date | None = None
    races: int = 0
    entries: int = 0
    past_performances: int = 0
    odds: int = 0
    results: int = 0
    issues: int = 0


class RaceWorkbookSelectionRead(BaseModel):
    workbook: RaceWorkbookFileRead
    import_summary: ImportSummaryRead
    quality_summary: DataQualityRunSummary


class CollectionRunRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    job_run_id: str
    source_code: str
    data_kind: str
    status: str
    mode: str
    race_date: date | None = None
    race_id: str | None = None
    force: bool
    cache_hit: bool
    attempt_count: int
    retry_count: int
    request_count: int
    raw_file_record_id: int | None = None
    raw_artifact_id: str | None = None
    normalized_artifact_id: str | None = None
    quality_status: str | None = None
    summary_json: dict[str, Any]
    warnings_json: list[Any]
    error_code: str | None = None
    error_message: str | None = None
    started_at: datetime | None = None
    finished_at: datetime | None = None
    created_at: datetime
    updated_at: datetime


class CollectionSourceRead(BaseModel):
    job_type: str
    source_code: str
    data_kind: str
    reliability_grade: str
    adapter_configured: bool
    execution_approved: bool
    cache_ttl_seconds: int
    min_interval_seconds: int
    max_retries: int
