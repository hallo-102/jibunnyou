"""add collection runs, raw metadata, and cache

Revision ID: 0003_collector_layers
Revises: 0002_core_history
Create Date: 2026-07-10 23:10:00
"""

from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa


revision: str = "0003_collector_layers"
down_revision: str | None = "0002_core_history"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    with op.batch_alter_table("raw_file_records", schema=None) as batch_op:
        batch_op.add_column(sa.Column("source_code", sa.String(length=64), nullable=True))
        batch_op.add_column(sa.Column("source_uri", sa.Text(), nullable=True))
        batch_op.add_column(sa.Column("original_file_path", sa.Text(), nullable=True))
        batch_op.add_column(sa.Column("fetched_at", sa.DateTime(timezone=True), nullable=True))
        batch_op.add_column(
            sa.Column(
                "is_immutable",
                sa.Boolean(),
                server_default=sa.false(),
                nullable=False,
            )
        )
        batch_op.create_index(
            batch_op.f("ix_raw_file_records_source_code"),
            ["source_code"],
            unique=False,
        )
        batch_op.create_index(
            batch_op.f("ix_raw_file_records_fetched_at"),
            ["fetched_at"],
            unique=False,
        )

    with op.batch_alter_table("raw_file_records", schema=None) as batch_op:
        batch_op.alter_column("is_immutable", server_default=None)

    with op.batch_alter_table("odds_snapshots", schema=None) as batch_op:
        batch_op.add_column(sa.Column("raw_odds", sa.String(length=64), nullable=True))
        batch_op.add_column(sa.Column("odds_min", sa.Float(), nullable=True))
        batch_op.add_column(sa.Column("odds_max", sa.Float(), nullable=True))
        batch_op.add_column(sa.Column("fetched_at", sa.DateTime(timezone=True), nullable=True))
        batch_op.create_index(
            batch_op.f("ix_odds_snapshots_fetched_at"),
            ["fetched_at"],
            unique=False,
        )

    op.create_table(
        "collection_runs",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("job_run_id", sa.String(length=36), nullable=False),
        sa.Column("source_code", sa.String(length=64), nullable=False),
        sa.Column("data_kind", sa.String(length=64), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("mode", sa.String(length=32), nullable=False),
        sa.Column("race_date", sa.Date(), nullable=True),
        sa.Column("race_id", sa.String(length=32), nullable=True),
        sa.Column("force", sa.Boolean(), nullable=False),
        sa.Column("cache_key", sa.String(length=64), nullable=False),
        sa.Column("cache_hit", sa.Boolean(), nullable=False),
        sa.Column("attempt_count", sa.Integer(), nullable=False),
        sa.Column("retry_count", sa.Integer(), nullable=False),
        sa.Column("request_count", sa.Integer(), nullable=False),
        sa.Column("raw_file_record_id", sa.Integer(), nullable=True),
        sa.Column("raw_artifact_id", sa.String(length=36), nullable=True),
        sa.Column("normalized_artifact_id", sa.String(length=36), nullable=True),
        sa.Column("quality_status", sa.String(length=16), nullable=True),
        sa.Column("summary_json", sa.JSON(), nullable=False),
        sa.Column("warnings_json", sa.JSON(), nullable=False),
        sa.Column("error_code", sa.String(length=128), nullable=True),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint("attempt_count >= 0", name="ck_collection_runs_attempt_count"),
        sa.CheckConstraint("length(cache_key) = 64", name="ck_collection_runs_cache_key"),
        sa.CheckConstraint(
            "mode IN ('dry_run','execute','import_only')",
            name="ck_collection_runs_mode",
        ),
        sa.CheckConstraint(
            "quality_status IS NULL OR quality_status IN ('GREEN','YELLOW','RED','GRAY')",
            name="ck_collection_runs_quality_status",
        ),
        sa.CheckConstraint("request_count >= 0", name="ck_collection_runs_request_count"),
        sa.CheckConstraint("retry_count >= 0", name="ck_collection_runs_retry_count"),
        sa.CheckConstraint(
            "status IN ('queued','running','succeeded','cached','partial','failed','blocked')",
            name="ck_collection_runs_status",
        ),
        sa.ForeignKeyConstraint(
            ["job_run_id"],
            ["job_runs.id"],
            name="fk_collection_runs_job_run_id",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["normalized_artifact_id"],
            ["artifact_files.id"],
            name="fk_collection_runs_normalized_artifact_id",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["raw_artifact_id"],
            ["artifact_files.id"],
            name="fk_collection_runs_raw_artifact_id",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["raw_file_record_id"],
            ["raw_file_records.id"],
            name="fk_collection_runs_raw_file_record_id",
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("job_run_id"),
    )
    with op.batch_alter_table("collection_runs", schema=None) as batch_op:
        batch_op.create_index(batch_op.f("ix_collection_runs_race_date"), ["race_date"], unique=False)
        batch_op.create_index(batch_op.f("ix_collection_runs_race_id"), ["race_id"], unique=False)
        batch_op.create_index("ix_collection_runs_job", ["job_run_id"], unique=False)
        batch_op.create_index(
            "ix_collection_runs_source_status",
            ["source_code", "status", "created_at"],
            unique=False,
        )
        batch_op.create_index(
            "ix_collection_runs_target",
            ["data_kind", "race_date", "race_id"],
            unique=False,
        )

    op.create_table(
        "collection_cache_entries",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("cache_key", sa.String(length=64), nullable=False),
        sa.Column("source_code", sa.String(length=64), nullable=False),
        sa.Column("data_kind", sa.String(length=64), nullable=False),
        sa.Column("race_date", sa.Date(), nullable=True),
        sa.Column("race_id", sa.String(length=32), nullable=True),
        sa.Column("raw_file_record_id", sa.Integer(), nullable=False),
        sa.Column("artifact_file_id", sa.String(length=36), nullable=False),
        sa.Column("content_sha256", sa.String(length=64), nullable=False),
        sa.Column("status", sa.String(length=16), nullable=False),
        sa.Column("fetched_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("hit_count", sa.Integer(), nullable=False),
        sa.Column("last_used_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(
            "length(content_sha256) = 64",
            name="ck_collection_cache_entries_content_hash",
        ),
        sa.CheckConstraint("hit_count >= 0", name="ck_collection_cache_entries_hit_count"),
        sa.CheckConstraint("length(cache_key) = 64", name="ck_collection_cache_entries_key"),
        sa.CheckConstraint(
            "status IN ('active','stale','invalid')",
            name="ck_collection_cache_entries_status",
        ),
        sa.ForeignKeyConstraint(
            ["artifact_file_id"],
            ["artifact_files.id"],
            name="fk_collection_cache_entries_artifact_file_id",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["raw_file_record_id"],
            ["raw_file_records.id"],
            name="fk_collection_cache_entries_raw_file_record_id",
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("cache_key"),
    )
    with op.batch_alter_table("collection_cache_entries", schema=None) as batch_op:
        batch_op.create_index(
            "ix_collection_cache_entries_expiry",
            ["status", "expires_at"],
            unique=False,
        )
        batch_op.create_index(batch_op.f("ix_collection_cache_entries_race_date"), ["race_date"], unique=False)
        batch_op.create_index(batch_op.f("ix_collection_cache_entries_race_id"), ["race_id"], unique=False)
        batch_op.create_index(
            "ix_collection_cache_entries_target",
            ["source_code", "data_kind", "race_date", "race_id"],
            unique=False,
        )


def downgrade() -> None:
    with op.batch_alter_table("collection_cache_entries", schema=None) as batch_op:
        batch_op.drop_index("ix_collection_cache_entries_target")
        batch_op.drop_index(batch_op.f("ix_collection_cache_entries_race_id"))
        batch_op.drop_index(batch_op.f("ix_collection_cache_entries_race_date"))
        batch_op.drop_index("ix_collection_cache_entries_expiry")
    op.drop_table("collection_cache_entries")

    with op.batch_alter_table("collection_runs", schema=None) as batch_op:
        batch_op.drop_index("ix_collection_runs_target")
        batch_op.drop_index("ix_collection_runs_source_status")
        batch_op.drop_index("ix_collection_runs_job")
        batch_op.drop_index(batch_op.f("ix_collection_runs_race_id"))
        batch_op.drop_index(batch_op.f("ix_collection_runs_race_date"))
    op.drop_table("collection_runs")

    with op.batch_alter_table("odds_snapshots", schema=None) as batch_op:
        batch_op.drop_index(batch_op.f("ix_odds_snapshots_fetched_at"))
        batch_op.drop_column("fetched_at")
        batch_op.drop_column("odds_max")
        batch_op.drop_column("odds_min")
        batch_op.drop_column("raw_odds")

    with op.batch_alter_table("raw_file_records", schema=None) as batch_op:
        batch_op.drop_index(batch_op.f("ix_raw_file_records_fetched_at"))
        batch_op.drop_index(batch_op.f("ix_raw_file_records_source_code"))
        batch_op.drop_column("is_immutable")
        batch_op.drop_column("fetched_at")
        batch_op.drop_column("original_file_path")
        batch_op.drop_column("source_uri")
        batch_op.drop_column("source_code")
