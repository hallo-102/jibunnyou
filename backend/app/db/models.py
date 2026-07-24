from datetime import date, datetime, timezone
from uuid import uuid4

from sqlalchemy import (
    Boolean,
    BigInteger,
    CheckConstraint,
    Date,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    Index,
    JSON,
    String,
    Text,
    UniqueConstraint,
    false,
    text,
    true,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base


class RaceDay(Base):
    __tablename__ = "race_days"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    race_date: Mapped[date] = mapped_column(Date, unique=True, index=True, nullable=False)
    status: Mapped[str] = mapped_column(String(32), default="imported", nullable=False)
    source: Mapped[str | None] = mapped_column(String(255))
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime,
        default=datetime.utcnow,
        onupdate=datetime.utcnow,
        nullable=False,
    )


class Race(Base):
    __tablename__ = "races"

    race_id: Mapped[str] = mapped_column(String(32), primary_key=True, index=True)
    race_date: Mapped[date | None] = mapped_column(Date, index=True)
    race_number: Mapped[int | None] = mapped_column(Integer, index=True)
    venue: Mapped[str | None] = mapped_column(String(64), index=True)
    name: Mapped[str | None] = mapped_column(String(255))
    start_time: Mapped[str | None] = mapped_column(String(32))
    course: Mapped[str | None] = mapped_column(String(64))
    track_condition: Mapped[str | None] = mapped_column(String(64))
    race_type: Mapped[str | None] = mapped_column(String(64))
    race_class: Mapped[str | None] = mapped_column(String(64))
    headcount: Mapped[int | None] = mapped_column(Integer)
    raw: Mapped[dict | None] = mapped_column(JSON)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime,
        default=datetime.utcnow,
        onupdate=datetime.utcnow,
        nullable=False,
    )

    entries: Mapped[list["RaceEntry"]] = relationship(
        back_populates="race",
        cascade="all, delete-orphan",
    )
    quality_status: Mapped["RaceQualityStatus | None"] = relationship(
        back_populates="race",
        cascade="all, delete-orphan",
        uselist=False,
    )


class Horse(Base):
    __tablename__ = "horses"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    name: Mapped[str] = mapped_column(String(255), unique=True, index=True, nullable=False)
    normalized_name: Mapped[str] = mapped_column(String(255), index=True, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)

    entries: Mapped[list["RaceEntry"]] = relationship(back_populates="horse")


class RaceEntry(Base):
    __tablename__ = "race_entries"
    __table_args__ = (UniqueConstraint("race_id", "horse_no", name="uq_race_entries_race_horse_no"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    race_id: Mapped[str] = mapped_column(ForeignKey("races.race_id"), index=True, nullable=False)
    horse_id: Mapped[int | None] = mapped_column(ForeignKey("horses.id"), index=True)
    horse_no: Mapped[int] = mapped_column(Integer, nullable=False)
    frame_no: Mapped[int | None] = mapped_column(Integer)
    horse_name: Mapped[str] = mapped_column(String(255), index=True, nullable=False)
    age: Mapped[int | None] = mapped_column(Integer)
    carried_weight: Mapped[float | None] = mapped_column(Float)
    jockey: Mapped[str | None] = mapped_column(String(128))
    trainer: Mapped[str | None] = mapped_column(String(128))
    popularity: Mapped[int | None] = mapped_column(Integer)
    win_odds: Mapped[float | None] = mapped_column(Float)
    place_odds: Mapped[float | None] = mapped_column(Float)
    prediction_rank: Mapped[int | None] = mapped_column(Integer)
    prediction_score: Mapped[float | None] = mapped_column(Float)
    estimated_in3_rate: Mapped[float | None] = mapped_column(Float)
    expected_value: Mapped[float | None] = mapped_column(Float)
    raw: Mapped[dict | None] = mapped_column(JSON)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime,
        default=datetime.utcnow,
        onupdate=datetime.utcnow,
        nullable=False,
    )

    race: Mapped[Race] = relationship(back_populates="entries")
    horse: Mapped[Horse | None] = relationship(back_populates="entries")


class PredictionRun(Base):
    __tablename__ = "prediction_runs"

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    status: Mapped[str] = mapped_column(String(32), index=True, default="completed", nullable=False)
    race_date: Mapped[date | None] = mapped_column(Date, index=True)
    race_id: Mapped[str | None] = mapped_column(String(32), index=True)
    prediction_version: Mapped[str] = mapped_column(String(64), default="legacy-v1", nullable=False)
    feature_version: Mapped[str] = mapped_column(String(64), default="legacy-feature-v1", nullable=False)
    weight_version: Mapped[str] = mapped_column(String(64), default="legacy-weight-v1", nullable=False)
    model_version: Mapped[str] = mapped_column(String(64), default="legacy-python", nullable=False)
    config_version_id: Mapped[str | None] = mapped_column(
        ForeignKey(
            "config_versions.id",
            name="fk_prediction_runs_config_version_id",
            ondelete="RESTRICT",
        ),
        index=True,
    )
    feature_weight_version_id: Mapped[str | None] = mapped_column(
        ForeignKey(
            "feature_weight_versions.id",
            name="fk_prediction_runs_feature_weight_version_id",
            ondelete="RESTRICT",
        ),
        index=True,
    )
    code_version: Mapped[str] = mapped_column(
        String(200),
        default="unknown",
        server_default="unknown",
        nullable=False,
    )
    parameters: Mapped[dict] = mapped_column(
        JSON,
        default=dict,
        server_default=text("'{}'"),
        nullable=False,
    )
    input_manifest_sha256: Mapped[str | None] = mapped_column(String(64))
    source_file: Mapped[str | None] = mapped_column(String(512))
    output_file: Mapped[str | None] = mapped_column(String(1024))
    result_json_file: Mapped[str | None] = mapped_column(String(1024))
    manifest_file: Mapped[str | None] = mapped_column(String(1024))
    input_checksum: Mapped[str | None] = mapped_column(String(64))
    result_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    matched_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    mismatch_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    message: Mapped[str | None] = mapped_column(Text)
    started_at: Mapped[datetime | None] = mapped_column(DateTime)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)

    results: Mapped[list["PredictionResult"]] = relationship(
        back_populates="prediction_run",
        cascade="all, delete-orphan",
    )


class PredictionResult(Base):
    __tablename__ = "prediction_results"
    __table_args__ = (
        UniqueConstraint(
            "prediction_run_id",
            "race_id",
            "horse_no",
            name="uq_prediction_results_run_race_horse_no",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    prediction_run_id: Mapped[str] = mapped_column(
        ForeignKey("prediction_runs.id"),
        index=True,
        nullable=False,
    )
    race_id: Mapped[str] = mapped_column(String(32), index=True, nullable=False)
    horse_no: Mapped[int] = mapped_column(Integer, index=True, nullable=False)
    horse_name: Mapped[str] = mapped_column(String(255), index=True, nullable=False)
    popularity: Mapped[int | None] = mapped_column(Integer)
    win_odds: Mapped[float | None] = mapped_column(Float)
    place_odds: Mapped[float | None] = mapped_column(Float)
    prediction_rank: Mapped[int | None] = mapped_column(Integer, index=True)
    prediction_score: Mapped[float | None] = mapped_column(Float)
    estimated_in3_rate: Mapped[float | None] = mapped_column(Float)
    expected_value: Mapped[float | None] = mapped_column(Float)
    risk_flag: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    risk_score: Mapped[float | None] = mapped_column(Float)
    risk_reason: Mapped[str | None] = mapped_column(Text)
    evaluation_reason: Mapped[str | None] = mapped_column(Text)
    feature_summary: Mapped[dict | None] = mapped_column(JSON)
    raw: Mapped[dict | None] = mapped_column(JSON)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)

    prediction_run: Mapped[PredictionRun] = relationship(back_populates="results")


class AiPredictionRun(Base):
    __tablename__ = "ai_prediction_runs"

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    race_id: Mapped[str] = mapped_column(String(32), index=True, nullable=False)
    race_date: Mapped[date | None] = mapped_column(Date, index=True)
    ai_mode: Mapped[str] = mapped_column(String(64), index=True, nullable=False)
    model_name: Mapped[str] = mapped_column(String(128), default="heuristic-second-opinion-v1", nullable=False)
    prompt_version: Mapped[str] = mapped_column(String(64), default="ai-second-opinion-v1", nullable=False)
    python_prediction_run_id: Mapped[str | None] = mapped_column(String(36), index=True)
    status: Mapped[str] = mapped_column(String(32), index=True, default="success", nullable=False)
    race_summary: Mapped[str | None] = mapped_column(Text)
    pace_prediction: Mapped[str | None] = mapped_column(String(128))
    python_trust_level: Mapped[str | None] = mapped_column(String(32))
    raw_request: Mapped[dict | None] = mapped_column(JSON)
    raw_response: Mapped[dict | None] = mapped_column(JSON)
    error_message: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime)


class AiHorseEvaluation(Base):
    __tablename__ = "ai_horse_evaluations"
    __table_args__ = (
        UniqueConstraint(
            "ai_run_id",
            "race_id",
            "horse_no",
            name="uq_ai_horse_evaluations_run_race_horse",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    ai_run_id: Mapped[str] = mapped_column(String(64), index=True, nullable=False)
    race_id: Mapped[str] = mapped_column(String(32), index=True, nullable=False)
    horse_no: Mapped[int] = mapped_column(Integer, index=True, nullable=False)
    horse_name: Mapped[str] = mapped_column(String(255), index=True, nullable=False)
    python_rank: Mapped[int | None] = mapped_column(Integer)
    ai_rank: Mapped[int | None] = mapped_column(Integer, index=True)
    ai_action: Mapped[str] = mapped_column(String(32), index=True, nullable=False)
    ai_adjust_score: Mapped[float] = mapped_column(Float, default=0.0, nullable=False)
    ai_reason: Mapped[str] = mapped_column(Text, nullable=False)
    ai_risk: Mapped[str | None] = mapped_column(Text)
    ai_bet_role: Mapped[str] = mapped_column(String(64), index=True, nullable=False)
    ai_confidence: Mapped[float | None] = mapped_column(Float)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)


class FinalPrediction(Base):
    __tablename__ = "final_predictions"
    __table_args__ = (
        UniqueConstraint(
            "ai_run_id",
            "race_id",
            "horse_no",
            name="uq_final_predictions_run_race_horse",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    ai_run_id: Mapped[str] = mapped_column(String(64), index=True, nullable=False)
    race_id: Mapped[str] = mapped_column(String(32), index=True, nullable=False)
    horse_no: Mapped[int] = mapped_column(Integer, index=True, nullable=False)
    horse_name: Mapped[str] = mapped_column(String(255), index=True, nullable=False)
    python_rank: Mapped[int | None] = mapped_column(Integer)
    python_score: Mapped[float | None] = mapped_column(Float)
    ai_rank: Mapped[int | None] = mapped_column(Integer)
    ai_adjust_score: Mapped[float] = mapped_column(Float, default=0.0, nullable=False)
    final_score: Mapped[float] = mapped_column(Float, default=0.0, nullable=False)
    final_rank: Mapped[int | None] = mapped_column(Integer, index=True)
    final_bet_role: Mapped[str] = mapped_column(String(64), index=True, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)


class AiBetStrategy(Base):
    __tablename__ = "ai_bet_strategies"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    ai_run_id: Mapped[str] = mapped_column(String(64), index=True, nullable=False)
    race_id: Mapped[str] = mapped_column(String(32), index=True, nullable=False)
    strategy_type: Mapped[str] = mapped_column(String(64), default="3連複", nullable=False)
    main_axis: Mapped[list[int] | None] = mapped_column(JSON)
    second_axis_candidates: Mapped[list[int] | None] = mapped_column(JSON)
    main_opponents: Mapped[list[int] | None] = mapped_column(JSON)
    wide_opponents: Mapped[list[int] | None] = mapped_column(JSON)
    exclude_candidates: Mapped[list[int] | None] = mapped_column(JSON)
    suggestion: Mapped[str | None] = mapped_column(Text)
    bet_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)


class BetCandidate(Base):
    __tablename__ = "bet_candidates"
    __table_args__ = (
        UniqueConstraint(
            "prediction_run_id",
            "race_id",
            "bet_type",
            "strategy",
            name="uq_bet_candidates_run_race_strategy",
        ),
        CheckConstraint("points >= 0", name="ck_bet_candidates_points_nonnegative"),
        CheckConstraint("stake_per_point >= 0", name="ck_bet_candidates_stake_nonnegative"),
        CheckConstraint("total_amount >= 0", name="ck_bet_candidates_total_nonnegative"),
        CheckConstraint(
            "source_type IN ('python','ai_integrated','legacy_ai','manual')",
            name="ck_bet_candidates_source_type",
        ),
        CheckConstraint(
            "strategy_mode IN ('formation','box','wheel','manual')",
            name="ck_bet_candidates_strategy_mode",
        ),
        CheckConstraint(
            "source_snapshot_hash IS NULL OR length(source_snapshot_hash) = 64",
            name="ck_bet_candidates_source_hash",
        ),
        CheckConstraint(
            "purchase_execution_enabled = false",
            name="ck_bet_candidates_purchase_execution_disabled",
        ),
        CheckConstraint(
            "status IN ('draft','candidate','review_required','planned','purchased',"
            "'awaiting_result','settled','skipped','cancelled','blocked')",
            name="ck_bet_candidates_status",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    prediction_run_id: Mapped[str] = mapped_column(String(36), index=True, nullable=False)
    source_type: Mapped[str] = mapped_column(
        String(32),
        index=True,
        default="python",
        server_default="python",
        nullable=False,
    )
    ai_analysis_id: Mapped[str | None] = mapped_column(
        ForeignKey("ai_analyses.id", ondelete="RESTRICT"),
        index=True,
    )
    race_id: Mapped[str] = mapped_column(String(32), index=True, nullable=False)
    race_date: Mapped[date | None] = mapped_column(Date, index=True)
    rank: Mapped[str] = mapped_column(String(16), index=True, nullable=False)
    status: Mapped[str] = mapped_column(String(32), index=True, default="candidate", nullable=False)
    bet_type: Mapped[str] = mapped_column(String(64), index=True, nullable=False)
    strategy: Mapped[str] = mapped_column(String(128), nullable=False)
    strategy_mode: Mapped[str] = mapped_column(
        String(32),
        default="formation",
        server_default="formation",
        nullable=False,
    )
    bet_rule_version: Mapped[str] = mapped_column(
        String(64),
        default="bet-rules-v1.0.0",
        server_default="bet-rules-v1.0.0",
        nullable=False,
    )
    axis_horse_nos: Mapped[list[int] | None] = mapped_column(JSON)
    opponent_horse_nos: Mapped[list[int] | None] = mapped_column(JSON)
    combinations: Mapped[list[list[int]] | None] = mapped_column(JSON)
    points: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    stake_per_point: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    total_amount: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    max_race_amount: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    max_day_amount: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    expected_value: Mapped[float | None] = mapped_column(Float)
    reason: Mapped[str | None] = mapped_column(Text)
    skip_reason: Mapped[str | None] = mapped_column(Text)
    warning_codes: Mapped[list[str]] = mapped_column(
        JSON,
        default=list,
        server_default=text("'[]'"),
        nullable=False,
    )
    requires_confirmation: Mapped[bool] = mapped_column(
        Boolean,
        default=True,
        server_default=true(),
        nullable=False,
    )
    # 自動投票経路をDB制約でも禁止し、購入済みstatusは手動記録だけに限定する。
    purchase_execution_enabled: Mapped[bool] = mapped_column(
        Boolean,
        default=False,
        server_default=false(),
        nullable=False,
    )
    source_snapshot_hash: Mapped[str | None] = mapped_column(String(64))
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime,
        default=datetime.utcnow,
        onupdate=datetime.utcnow,
        nullable=False,
    )


class RaceResult(Base):
    __tablename__ = "race_results"
    __table_args__ = (
        UniqueConstraint("race_id", name="uq_race_results_race_id"),
        CheckConstraint(
            "result_status IN ('provisional','confirmed','cancelled')",
            name="ck_race_results_status",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    race_id: Mapped[str] = mapped_column(String(32), index=True, nullable=False)
    race_date: Mapped[date | None] = mapped_column(Date, index=True)
    source_file: Mapped[str | None] = mapped_column(String(1024))
    finish_order: Mapped[list[int]] = mapped_column(JSON, nullable=False)
    payout_amount: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    payout_type: Mapped[str] = mapped_column(String(64), default="3連複", nullable=False)
    result_status: Mapped[str] = mapped_column(String(32), default="confirmed", server_default="confirmed", nullable=False)
    payouts_json: Mapped[list[dict]] = mapped_column(JSON, default=list, server_default=text("'[]'"), nullable=False)
    cancelled_horse_nos: Mapped[list[int]] = mapped_column(JSON, default=list, server_default=text("'[]'"), nullable=False)
    disqualified_horse_nos: Mapped[list[int]] = mapped_column(JSON, default=list, server_default=text("'[]'"), nullable=False)
    has_dead_heat: Mapped[bool] = mapped_column(Boolean, default=False, server_default=false(), nullable=False)
    confirmed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    raw: Mapped[dict | None] = mapped_column(JSON)
    imported_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)


class BetSettlement(Base):
    __tablename__ = "bet_settlements"
    __table_args__ = (
        UniqueConstraint("bet_candidate_id", name="uq_bet_settlements_candidate"),
        CheckConstraint("hit_count >= 0", name="ck_bet_settlements_hit_count"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    bet_candidate_id: Mapped[int] = mapped_column(Integer, index=True, nullable=False)
    race_id: Mapped[str] = mapped_column(String(32), index=True, nullable=False)
    race_date: Mapped[date | None] = mapped_column(Date, index=True)
    bet_type: Mapped[str] = mapped_column(String(64), default="3連複", server_default="3連複", nullable=False)
    source_type: Mapped[str] = mapped_column(String(32), default="python", server_default="python", nullable=False)
    is_hit: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    hit_count: Mapped[int] = mapped_column(Integer, default=0, server_default="0", nullable=False)
    winning_combinations: Mapped[list[list[int]]] = mapped_column(JSON, default=list, server_default=text("'[]'"), nullable=False)
    payout_details_json: Mapped[list[dict]] = mapped_column(JSON, default=list, server_default=text("'[]'"), nullable=False)
    result_status: Mapped[str] = mapped_column(String(32), default="confirmed", server_default="confirmed", nullable=False)
    payout_amount: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    stake_amount: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    profit_loss: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    roi: Mapped[float] = mapped_column(Float, default=0.0, nullable=False)
    message: Mapped[str | None] = mapped_column(Text)
    settled_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)


class ReviewNote(Base):
    __tablename__ = "review_notes"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    race_id: Mapped[str] = mapped_column(String(32), index=True, nullable=False)
    race_date: Mapped[date | None] = mapped_column(Date, index=True)
    bet_candidate_id: Mapped[int | None] = mapped_column(Integer, index=True)
    prediction_run_id: Mapped[str | None] = mapped_column(String(36), index=True)
    note: Mapped[str] = mapped_column(Text, nullable=False)
    ai_vs_result: Mapped[dict | None] = mapped_column(JSON)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime,
        default=datetime.utcnow,
        onupdate=datetime.utcnow,
        nullable=False,
    )


class JobRun(Base):
    __tablename__ = "job_runs"

    id: Mapped[str] = mapped_column(
        String(36),
        primary_key=True,
        default=lambda: str(uuid4()),
    )
    job_type: Mapped[str] = mapped_column(String(128), index=True, nullable=False)
    status: Mapped[str] = mapped_column(String(32), index=True, default="queued", nullable=False)
    race_date: Mapped[date | None] = mapped_column(Date, index=True)
    race_id: Mapped[str | None] = mapped_column(String(32), index=True)
    force: Mapped[bool] = mapped_column(default=False, nullable=False)
    params: Mapped[dict | None] = mapped_column(JSON)
    message: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
    started_at: Mapped[datetime | None] = mapped_column(DateTime)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime)


class DataQualityIssue(Base):
    __tablename__ = "data_quality_issues"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    severity: Mapped[str] = mapped_column(String(16), index=True, nullable=False)
    code: Mapped[str] = mapped_column(String(128), index=True, nullable=False)
    message: Mapped[str] = mapped_column(Text, nullable=False)
    source_file: Mapped[str | None] = mapped_column(String(512), index=True)
    race_id: Mapped[str | None] = mapped_column(String(32), index=True)
    row_number: Mapped[int | None] = mapped_column(Integer)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)


class Notification(Base):
    __tablename__ = "notifications"
    __table_args__ = (
        UniqueConstraint(
            "category",
            "source_type",
            "source_id",
            name="uq_notifications_source",
        ),
        CheckConstraint(
            "severity IN ('info','warning','error')",
            name="ck_notifications_severity",
        ),
        Index("ix_notifications_unread_time", "is_read", "created_at"),
        Index("ix_notifications_race_time", "race_id", "created_at"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid4()))
    category: Mapped[str] = mapped_column(String(50), index=True, nullable=False)
    severity: Mapped[str] = mapped_column(String(16), index=True, nullable=False)
    title: Mapped[str] = mapped_column(String(200), nullable=False)
    message: Mapped[str] = mapped_column(Text, nullable=False)
    source_type: Mapped[str] = mapped_column(String(50), nullable=False)
    source_id: Mapped[str] = mapped_column(String(200), nullable=False)
    race_id: Mapped[str | None] = mapped_column(String(32), index=True)
    race_date: Mapped[date | None] = mapped_column(Date, index=True)
    action_anchor: Mapped[str | None] = mapped_column(String(200))
    is_read: Mapped[bool] = mapped_column(Boolean, default=False, server_default=false(), nullable=False)
    read_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        nullable=False,
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
        nullable=False,
    )


class RaceQualityStatus(Base):
    __tablename__ = "race_quality_statuses"
    __table_args__ = (UniqueConstraint("race_id", name="uq_race_quality_status_race_id"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    race_id: Mapped[str] = mapped_column(ForeignKey("races.race_id"), index=True, nullable=False)
    status: Mapped[str] = mapped_column(String(16), index=True, default="GRAY", nullable=False)
    summary: Mapped[str | None] = mapped_column(Text)
    issue_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    red_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    yellow_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    checked_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)

    race: Mapped[Race] = relationship(back_populates="quality_status")


def utc_now() -> datetime:
    """Return a timezone-aware UTC timestamp for new immutable records."""

    return datetime.now(timezone.utc)


class RawFileRecord(Base):
    __tablename__ = "raw_file_records"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    file_path: Mapped[str] = mapped_column(String(1024), unique=True, nullable=False)
    file_name: Mapped[str] = mapped_column(String(512), index=True, nullable=False)
    file_type: Mapped[str] = mapped_column(String(64), index=True, nullable=False)
    race_date: Mapped[date | None] = mapped_column(Date, index=True)
    checksum: Mapped[str] = mapped_column(String(64), index=True, nullable=False)
    row_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    source_code: Mapped[str | None] = mapped_column(String(64), index=True)
    source_uri: Mapped[str | None] = mapped_column(Text)
    original_file_path: Mapped[str | None] = mapped_column(Text)
    fetched_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), index=True)
    is_immutable: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    imported_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)


class CollectionRun(Base):
    __tablename__ = "collection_runs"
    __table_args__ = (
        CheckConstraint(
            "status IN ('queued','running','succeeded','cached','partial','failed','blocked')",
            name="ck_collection_runs_status",
        ),
        CheckConstraint(
            "mode IN ('dry_run','execute','import_only')",
            name="ck_collection_runs_mode",
        ),
        CheckConstraint(
            "quality_status IS NULL OR quality_status IN ('GREEN','YELLOW','RED','GRAY')",
            name="ck_collection_runs_quality_status",
        ),
        CheckConstraint("attempt_count >= 0", name="ck_collection_runs_attempt_count"),
        CheckConstraint("retry_count >= 0", name="ck_collection_runs_retry_count"),
        CheckConstraint("request_count >= 0", name="ck_collection_runs_request_count"),
        CheckConstraint("length(cache_key) = 64", name="ck_collection_runs_cache_key"),
        Index("ix_collection_runs_target", "data_kind", "race_date", "race_id"),
        Index("ix_collection_runs_source_status", "source_code", "status", "created_at"),
        Index("ix_collection_runs_job", "job_run_id"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid4()))
    job_run_id: Mapped[str] = mapped_column(
        ForeignKey(
            "job_runs.id",
            ondelete="CASCADE",
            name="fk_collection_runs_job_run_id",
        ),
        unique=True,
        nullable=False,
    )
    source_code: Mapped[str] = mapped_column(String(64), nullable=False)
    data_kind: Mapped[str] = mapped_column(String(64), nullable=False)
    status: Mapped[str] = mapped_column(String(32), default="queued", nullable=False)
    mode: Mapped[str] = mapped_column(String(32), default="dry_run", nullable=False)
    race_date: Mapped[date | None] = mapped_column(Date, index=True)
    race_id: Mapped[str | None] = mapped_column(String(32), index=True)
    force: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    cache_key: Mapped[str] = mapped_column(String(64), nullable=False)
    cache_hit: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    attempt_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    retry_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    request_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    raw_file_record_id: Mapped[int | None] = mapped_column(
        ForeignKey(
            "raw_file_records.id",
            ondelete="RESTRICT",
            name="fk_collection_runs_raw_file_record_id",
        )
    )
    raw_artifact_id: Mapped[str | None] = mapped_column(
        ForeignKey(
            "artifact_files.id",
            ondelete="RESTRICT",
            name="fk_collection_runs_raw_artifact_id",
        )
    )
    normalized_artifact_id: Mapped[str | None] = mapped_column(
        ForeignKey(
            "artifact_files.id",
            ondelete="RESTRICT",
            name="fk_collection_runs_normalized_artifact_id",
        )
    )
    quality_status: Mapped[str | None] = mapped_column(String(16))
    summary_json: Mapped[dict] = mapped_column(JSON, default=dict, nullable=False)
    warnings_json: Mapped[list] = mapped_column(JSON, default=list, nullable=False)
    error_code: Mapped[str | None] = mapped_column(String(128))
    error_message: Mapped[str | None] = mapped_column(Text)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=utc_now,
        nullable=False,
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=utc_now,
        onupdate=utc_now,
        nullable=False,
    )


class CollectionCacheEntry(Base):
    __tablename__ = "collection_cache_entries"
    __table_args__ = (
        CheckConstraint("length(cache_key) = 64", name="ck_collection_cache_entries_key"),
        CheckConstraint(
            "length(content_sha256) = 64",
            name="ck_collection_cache_entries_content_hash",
        ),
        CheckConstraint(
            "status IN ('active','stale','invalid')",
            name="ck_collection_cache_entries_status",
        ),
        CheckConstraint("hit_count >= 0", name="ck_collection_cache_entries_hit_count"),
        Index("ix_collection_cache_entries_expiry", "status", "expires_at"),
        Index(
            "ix_collection_cache_entries_target",
            "source_code",
            "data_kind",
            "race_date",
            "race_id",
        ),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid4()))
    cache_key: Mapped[str] = mapped_column(String(64), unique=True, nullable=False)
    source_code: Mapped[str] = mapped_column(String(64), nullable=False)
    data_kind: Mapped[str] = mapped_column(String(64), nullable=False)
    race_date: Mapped[date | None] = mapped_column(Date, index=True)
    race_id: Mapped[str | None] = mapped_column(String(32), index=True)
    raw_file_record_id: Mapped[int] = mapped_column(
        ForeignKey(
            "raw_file_records.id",
            ondelete="RESTRICT",
            name="fk_collection_cache_entries_raw_file_record_id",
        ),
        nullable=False,
    )
    artifact_file_id: Mapped[str] = mapped_column(
        ForeignKey(
            "artifact_files.id",
            ondelete="RESTRICT",
            name="fk_collection_cache_entries_artifact_file_id",
        ),
        nullable=False,
    )
    content_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    status: Mapped[str] = mapped_column(String(16), default="active", nullable=False)
    fetched_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    hit_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    last_used_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=utc_now,
        nullable=False,
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=utc_now,
        onupdate=utc_now,
        nullable=False,
    )


class HorsePastPerformance(Base):
    __tablename__ = "horse_past_performances"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    source_file: Mapped[str] = mapped_column(String(512), index=True, nullable=False)
    source_sheet: Mapped[str] = mapped_column(String(64), index=True, nullable=False)
    target_race_id: Mapped[str] = mapped_column(String(32), index=True, nullable=False)
    past_race_id: Mapped[str | None] = mapped_column(String(32), index=True)
    horse_name: Mapped[str | None] = mapped_column(String(255), index=True)
    race_date: Mapped[date | None] = mapped_column(Date, index=True)
    race_name: Mapped[str | None] = mapped_column(String(255))
    horse_no: Mapped[int | None] = mapped_column(Integer)
    finish_position: Mapped[int | None] = mapped_column(Integer)
    popularity: Mapped[int | None] = mapped_column(Integer)
    odds: Mapped[float | None] = mapped_column(Float)
    distance: Mapped[str | None] = mapped_column(String(64))
    jockey: Mapped[str | None] = mapped_column(String(128))
    raw: Mapped[dict | None] = mapped_column(JSON)


class OddsSnapshot(Base):
    __tablename__ = "odds_snapshots"
    __table_args__ = (
        UniqueConstraint(
            "source_file",
            "race_date",
            "racecourse",
            "race_no",
            "horse_no",
            "bet_type",
            "combination",
            name="uq_odds_snapshot_source_key",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    source_file: Mapped[str] = mapped_column(String(512), index=True, nullable=False)
    race_date: Mapped[date] = mapped_column(Date, index=True, nullable=False)
    racecourse: Mapped[str] = mapped_column(String(64), index=True, nullable=False)
    race_no: Mapped[int] = mapped_column(Integer, index=True, nullable=False)
    horse_no: Mapped[int | None] = mapped_column(Integer, index=True)
    horse_name: Mapped[str | None] = mapped_column(String(255), index=True)
    bet_type: Mapped[str] = mapped_column(String(64), index=True, nullable=False)
    combination: Mapped[str | None] = mapped_column(String(64))
    raw_odds: Mapped[str | None] = mapped_column(String(64))
    odds: Mapped[float | None] = mapped_column(Float)
    odds_min: Mapped[float | None] = mapped_column(Float)
    odds_max: Mapped[float | None] = mapped_column(Float)
    fetched_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), index=True)
    imported_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)


class ConfigVersion(Base):
    __tablename__ = "config_versions"
    __table_args__ = (
        UniqueConstraint("config_key", "version_name", name="uq_config_versions_key_name"),
        CheckConstraint(
            "environment IN ('production','research','development','test')",
            name="ck_config_versions_environment",
        ),
        CheckConstraint("length(sha256) = 64", name="ck_config_versions_sha256"),
        Index("ix_config_versions_active", "environment", "is_active"),
        Index("ix_config_versions_created_at", "created_at"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid4()))
    config_key: Mapped[str] = mapped_column(String(100), nullable=False)
    version_name: Mapped[str] = mapped_column(String(120), nullable=False)
    environment: Mapped[str] = mapped_column(String(30), nullable=False)
    config_json: Mapped[dict] = mapped_column(JSON, nullable=False)
    sha256: Mapped[str] = mapped_column(String(64), unique=True, nullable=False)
    is_active: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    created_by: Mapped[str | None] = mapped_column(String(200))
    note: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=utc_now,
        nullable=False,
    )


class FeatureWeightVersion(Base):
    __tablename__ = "feature_weight_versions"
    __table_args__ = (
        UniqueConstraint(
            "weight_key",
            "version_name",
            name="uq_feature_weight_versions_key_name",
        ),
        CheckConstraint("length(sha256) = 64", name="ck_feature_weight_versions_sha256"),
        Index("ix_feature_weight_versions_active", "weight_key", "is_active"),
        Index("ix_feature_weight_versions_created_at", "created_at"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid4()))
    weight_key: Mapped[str] = mapped_column(String(100), nullable=False)
    version_name: Mapped[str] = mapped_column(String(120), nullable=False)
    weights_json: Mapped[dict] = mapped_column(JSON, nullable=False)
    source_file_path: Mapped[str | None] = mapped_column(Text)
    sha256: Mapped[str] = mapped_column(String(64), unique=True, nullable=False)
    is_active: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    created_by: Mapped[str | None] = mapped_column(String(200))
    note: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=utc_now,
        nullable=False,
    )


class ArtifactFile(Base):
    __tablename__ = "artifact_files"
    __table_args__ = (
        UniqueConstraint(
            "prediction_run_id",
            "logical_name",
            name="uq_artifact_files_prediction_run_name",
        ),
        UniqueConstraint("job_run_id", "logical_name", name="uq_artifact_files_job_run_name"),
        UniqueConstraint("storage_path", name="uq_artifact_files_storage_path"),
        CheckConstraint("size_bytes IS NULL OR size_bytes >= 0", name="ck_artifact_files_size"),
        CheckConstraint("length(sha256) = 64", name="ck_artifact_files_sha256"),
        Index("ix_artifact_files_run_kind", "prediction_run_id", "artifact_kind"),
        Index("ix_artifact_files_job_kind", "job_run_id", "artifact_kind"),
        Index("ix_artifact_files_sha256", "sha256"),
        Index("ix_artifact_files_created_at", "created_at"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid4()))
    prediction_run_id: Mapped[str | None] = mapped_column(
        ForeignKey("prediction_runs.id", ondelete="RESTRICT")
    )
    job_run_id: Mapped[str | None] = mapped_column(ForeignKey("job_runs.id", ondelete="RESTRICT"))
    artifact_kind: Mapped[str] = mapped_column(String(100), nullable=False)
    logical_name: Mapped[str] = mapped_column(String(300), nullable=False)
    storage_path: Mapped[str] = mapped_column(Text, nullable=False)
    content_type: Mapped[str | None] = mapped_column(String(150))
    size_bytes: Mapped[int | None] = mapped_column(BigInteger)
    sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    is_immutable: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    metadata_json: Mapped[dict] = mapped_column(JSON, default=dict, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=utc_now,
        nullable=False,
    )


class EvidenceRecord(Base):
    __tablename__ = "evidence_records"
    __table_args__ = (
        CheckConstraint(
            "reliability_grade IN ('A','B','C','D')",
            name="ck_evidence_records_reliability",
        ),
        CheckConstraint(
            "impact_hint IS NULL OR impact_hint IN ('positive','negative','neutral')",
            name="ck_evidence_records_impact_hint",
        ),
        Index("ix_evidence_records_run_race", "prediction_run_id", "race_id"),
        Index("ix_evidence_records_horse", "race_id", "horse_no"),
        Index("ix_evidence_records_category", "category", "reliability_grade"),
        Index("ix_evidence_records_fetched_at", "fetched_at"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid4()))
    evidence_id: Mapped[str] = mapped_column(String(100), unique=True, nullable=False)
    prediction_run_id: Mapped[str] = mapped_column(
        ForeignKey("prediction_runs.id", ondelete="RESTRICT"),
        nullable=False,
    )
    race_id: Mapped[str] = mapped_column(String(32), nullable=False)
    horse_no: Mapped[int | None] = mapped_column(Integer)
    category: Mapped[str] = mapped_column(String(50), nullable=False)
    source_type: Mapped[str] = mapped_column(String(50), nullable=False)
    reliability_grade: Mapped[str] = mapped_column(String(1), nullable=False)
    source_name: Mapped[str | None] = mapped_column(String(300))
    source_url: Mapped[str | None] = mapped_column(Text)
    fetched_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    observed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    freshness_status: Mapped[str | None] = mapped_column(String(30))
    summary: Mapped[str] = mapped_column(Text, nullable=False)
    structured_values_json: Mapped[dict] = mapped_column(JSON, default=dict, nullable=False)
    impact_hint: Mapped[str | None] = mapped_column(String(30))
    impact_strength: Mapped[str | None] = mapped_column(String(30))
    source_hash: Mapped[str | None] = mapped_column(String(64))
    artifact_file_id: Mapped[str | None] = mapped_column(
        ForeignKey("artifact_files.id", ondelete="RESTRICT")
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=utc_now,
        nullable=False,
    )


class AiAnalysis(Base):
    __tablename__ = "ai_analyses"
    __table_args__ = (
        UniqueConstraint("race_id", "analysis_sequence", name="uq_ai_analyses_race_sequence"),
        CheckConstraint(
            "status IN ('queued','running','succeeded','failed','degraded')",
            name="ck_ai_analyses_status",
        ),
        CheckConstraint("analysis_sequence > 0", name="ck_ai_analyses_sequence"),
        CheckConstraint(
            "input_snapshot_hash IS NULL OR length(input_snapshot_hash) = 64",
            name="ck_ai_analyses_input_hash",
        ),
        Index("ix_ai_analyses_race_created", "race_id", "created_at"),
        Index("ix_ai_analyses_prediction_run", "prediction_run_id"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid4()))
    race_id: Mapped[str] = mapped_column(String(32), nullable=False)
    race_date: Mapped[date | None] = mapped_column(Date, index=True)
    prediction_run_id: Mapped[str | None] = mapped_column(
        ForeignKey("prediction_runs.id", ondelete="RESTRICT")
    )
    parent_analysis_id: Mapped[str | None] = mapped_column(
        ForeignKey("ai_analyses.id", ondelete="RESTRICT")
    )
    analysis_sequence: Mapped[int] = mapped_column(Integer, nullable=False)
    status: Mapped[str] = mapped_column(String(32), default="queued", nullable=False)
    model_name: Mapped[str] = mapped_column(String(128), nullable=False)
    prompt_version: Mapped[str] = mapped_column(String(64), nullable=False)
    input_data_version: Mapped[str | None] = mapped_column(String(128))
    input_snapshot_hash: Mapped[str | None] = mapped_column(String(64))
    input_artifact_id: Mapped[str | None] = mapped_column(
        ForeignKey("artifact_files.id", ondelete="RESTRICT")
    )
    prompt_tokens: Mapped[int | None] = mapped_column(Integer)
    completion_tokens: Mapped[int | None] = mapped_column(Integer)
    duration_ms: Mapped[int | None] = mapped_column(Integer)
    error_message: Mapped[str | None] = mapped_column(Text)
    rerun_reason: Mapped[str | None] = mapped_column(Text)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=utc_now,
        nullable=False,
    )


class AiAnalysisOutput(Base):
    __tablename__ = "ai_analysis_outputs"
    __table_args__ = (
        UniqueConstraint("analysis_id", "stage", name="uq_ai_analysis_outputs_stage"),
        CheckConstraint(
            "stage IN ('independent','comparison','integration')",
            name="ck_ai_analysis_outputs_stage",
        ),
        CheckConstraint(
            "stage <> 'independent' OR python_result_visible = false",
            name="ck_ai_analysis_outputs_independent_input",
        ),
        CheckConstraint(
            "confidence IS NULL OR (confidence >= 0 AND confidence <= 1)",
            name="ck_ai_analysis_outputs_confidence",
        ),
        CheckConstraint("length(output_hash) = 64", name="ck_ai_analysis_outputs_hash"),
        Index("ix_ai_analysis_outputs_analysis_stage", "analysis_id", "stage"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid4()))
    analysis_id: Mapped[str] = mapped_column(
        ForeignKey("ai_analyses.id", ondelete="RESTRICT"),
        nullable=False,
    )
    stage: Mapped[str] = mapped_column(String(32), nullable=False)
    output_schema_version: Mapped[str] = mapped_column(String(64), nullable=False)
    output_json: Mapped[dict] = mapped_column(JSON, nullable=False)
    output_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    confidence: Mapped[float | None] = mapped_column(Float)
    python_result_visible: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    is_locked: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    locked_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=utc_now,
        nullable=False,
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=utc_now,
        nullable=False,
    )


class ChatgptManualPrediction(Base):
    """Store user-mediated ChatGPT prompts and pasted responses without API calls."""

    __tablename__ = "chatgpt_manual_predictions"
    __table_args__ = (
        CheckConstraint(
            "source = 'chatgpt_manual'",
            name="ck_chatgpt_manual_predictions_source",
        ),
        Index(
            "ix_chatgpt_manual_predictions_race_created",
            "race_id",
            "created_at",
        ),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid4()))
    race_id: Mapped[str] = mapped_column(
        ForeignKey("races.race_id", ondelete="RESTRICT"),
        index=True,
        nullable=False,
    )
    source: Mapped[str] = mapped_column(
        String(32),
        default="chatgpt_manual",
        server_default="chatgpt_manual",
        nullable=False,
    )
    prompt_text: Mapped[str] = mapped_column(Text, nullable=False)
    response_text: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=utc_now,
        nullable=False,
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=utc_now,
        onupdate=utc_now,
        nullable=False,
    )


class BetStatusHistory(Base):
    __tablename__ = "bet_status_history"
    __table_args__ = (
        CheckConstraint(
            "new_status IN ('draft','candidate','review_required','planned','purchased',"
            "'awaiting_result','settled','skipped','cancelled','blocked')",
            name="ck_bet_status_history_new_status",
        ),
        Index("ix_bet_status_history_bet_time", "bet_candidate_id", "created_at"),
        Index("ix_bet_status_history_status", "new_status", "created_at"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid4()))
    bet_candidate_id: Mapped[int] = mapped_column(
        ForeignKey("bet_candidates.id", ondelete="RESTRICT"),
        nullable=False,
    )
    old_status: Mapped[str | None] = mapped_column(String(30))
    new_status: Mapped[str] = mapped_column(String(30), nullable=False)
    changed_by: Mapped[str | None] = mapped_column(String(200))
    reason: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=utc_now,
        nullable=False,
    )


class AuditLog(Base):
    __tablename__ = "audit_logs"
    __table_args__ = (
        CheckConstraint(
            "actor_type IN ('user','device','system','codex')",
            name="ck_audit_logs_actor_type",
        ),
        Index("ix_audit_logs_entity", "entity_type", "entity_id", "occurred_at"),
        Index("ix_audit_logs_actor", "actor_type", "actor_id", "occurred_at"),
        Index("ix_audit_logs_run", "prediction_run_id", "occurred_at"),
        Index("ix_audit_logs_action", "action", "occurred_at"),
    )

    id: Mapped[int] = mapped_column(
        BigInteger().with_variant(Integer, "sqlite"),
        primary_key=True,
        autoincrement=True,
    )
    occurred_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=utc_now,
        nullable=False,
    )
    actor_type: Mapped[str] = mapped_column(String(50), nullable=False)
    actor_id: Mapped[str | None] = mapped_column(String(200))
    action: Mapped[str] = mapped_column(String(150), nullable=False)
    entity_type: Mapped[str] = mapped_column(String(100), nullable=False)
    entity_id: Mapped[str] = mapped_column(String(200), nullable=False)
    prediction_run_id: Mapped[str | None] = mapped_column(
        ForeignKey("prediction_runs.id", ondelete="RESTRICT")
    )
    trace_id: Mapped[str | None] = mapped_column(String(100))
    before_json: Mapped[dict] = mapped_column(JSON, default=dict, nullable=False)
    after_json: Mapped[dict] = mapped_column(JSON, default=dict, nullable=False)
    metadata_json: Mapped[dict] = mapped_column(JSON, default=dict, nullable=False)
    ip_address: Mapped[str | None] = mapped_column(String(64))
    user_agent: Mapped[str | None] = mapped_column(Text)


class IdempotencyRecord(Base):
    __tablename__ = "idempotency_records"
    __table_args__ = (
        UniqueConstraint("scope", "idempotency_key", name="uq_idempotency_records_scope_key"),
        CheckConstraint(
            "status IN ('processing','completed','failed')",
            name="ck_idempotency_records_status",
        ),
        CheckConstraint("length(request_hash) = 64", name="ck_idempotency_records_hash"),
        Index("ix_idempotency_records_expiry", "expires_at"),
        Index("ix_idempotency_records_resource", "resource_type", "resource_id"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid4()))
    scope: Mapped[str] = mapped_column(String(150), nullable=False)
    idempotency_key: Mapped[str] = mapped_column(String(200), nullable=False)
    request_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    status: Mapped[str] = mapped_column(String(32), default="processing", nullable=False)
    response_status: Mapped[int | None] = mapped_column(Integer)
    response_body: Mapped[dict | None] = mapped_column(JSON)
    resource_type: Mapped[str | None] = mapped_column(String(100))
    resource_id: Mapped[str | None] = mapped_column(String(200))
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=utc_now,
        nullable=False,
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=utc_now,
        onupdate=utc_now,
        nullable=False,
    )


class JobLog(Base):
    __tablename__ = "job_logs"
    __table_args__ = (
        Index("ix_job_logs_job_time", "job_run_id", "created_at"),
        Index("ix_job_logs_event_code", "event_code"),
        Index("ix_job_logs_trace_id", "trace_id"),
    )

    id: Mapped[int] = mapped_column(
        BigInteger().with_variant(Integer, "sqlite"),
        primary_key=True,
        autoincrement=True,
    )
    job_run_id: Mapped[str] = mapped_column(
        ForeignKey("job_runs.id", ondelete="CASCADE"),
        nullable=False,
    )
    level: Mapped[str] = mapped_column(String(20), nullable=False)
    event_code: Mapped[str] = mapped_column(String(128), nullable=False)
    message: Mapped[str] = mapped_column(Text, nullable=False)
    context_json: Mapped[dict] = mapped_column(JSON, default=dict, nullable=False)
    trace_id: Mapped[str | None] = mapped_column(String(100))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=utc_now,
        nullable=False,
    )
