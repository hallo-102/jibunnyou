"""add bet proposal sources and purchase safety metadata

Revision ID: 0004_bet_plan_safety
Revises: 0003_collector_layers
Create Date: 2026-07-10 23:50:00
"""

from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa


revision: str = "0004_bet_plan_safety"
down_revision: str | None = "0003_collector_layers"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    with op.batch_alter_table("bet_candidates", schema=None) as batch_op:
        batch_op.add_column(
            sa.Column(
                "source_type",
                sa.String(length=32),
                server_default="python",
                nullable=False,
            )
        )
        batch_op.add_column(sa.Column("ai_analysis_id", sa.String(length=36), nullable=True))
        batch_op.add_column(
            sa.Column(
                "strategy_mode",
                sa.String(length=32),
                server_default="formation",
                nullable=False,
            )
        )
        batch_op.add_column(
            sa.Column(
                "bet_rule_version",
                sa.String(length=64),
                server_default="bet-rules-v1.0.0",
                nullable=False,
            )
        )
        batch_op.add_column(
            sa.Column(
                "warning_codes",
                sa.JSON(),
                server_default=sa.text("'[]'"),
                nullable=False,
            )
        )
        batch_op.add_column(
            sa.Column(
                "requires_confirmation",
                sa.Boolean(),
                server_default=sa.true(),
                nullable=False,
            )
        )
        batch_op.add_column(
            sa.Column(
                "purchase_execution_enabled",
                sa.Boolean(),
                server_default=sa.false(),
                nullable=False,
            )
        )
        batch_op.add_column(sa.Column("source_snapshot_hash", sa.String(length=64), nullable=True))
        batch_op.create_foreign_key(
            "fk_bet_candidates_ai_analysis_id",
            "ai_analyses",
            ["ai_analysis_id"],
            ["id"],
            ondelete="RESTRICT",
        )
        batch_op.create_index(
            batch_op.f("ix_bet_candidates_source_type"),
            ["source_type"],
            unique=False,
        )
        batch_op.create_index(
            batch_op.f("ix_bet_candidates_ai_analysis_id"),
            ["ai_analysis_id"],
            unique=False,
        )
        batch_op.create_check_constraint(
            "ck_bet_candidates_source_type",
            "source_type IN ('python','ai_integrated','legacy_ai','manual')",
        )
        batch_op.create_check_constraint(
            "ck_bet_candidates_strategy_mode",
            "strategy_mode IN ('formation','box','wheel','manual')",
        )
        batch_op.create_check_constraint(
            "ck_bet_candidates_source_hash",
            "source_snapshot_hash IS NULL OR length(source_snapshot_hash) = 64",
        )
        batch_op.create_check_constraint(
            "ck_bet_candidates_purchase_execution_disabled",
            "purchase_execution_enabled = false",
        )


def downgrade() -> None:
    with op.batch_alter_table("bet_candidates", schema=None) as batch_op:
        batch_op.drop_constraint(
            "ck_bet_candidates_purchase_execution_disabled",
            type_="check",
        )
        batch_op.drop_constraint("ck_bet_candidates_source_hash", type_="check")
        batch_op.drop_constraint("ck_bet_candidates_strategy_mode", type_="check")
        batch_op.drop_constraint("ck_bet_candidates_source_type", type_="check")
        batch_op.drop_index(batch_op.f("ix_bet_candidates_ai_analysis_id"))
        batch_op.drop_index(batch_op.f("ix_bet_candidates_source_type"))
        batch_op.drop_constraint("fk_bet_candidates_ai_analysis_id", type_="foreignkey")
        batch_op.drop_column("source_snapshot_hash")
        batch_op.drop_column("purchase_execution_enabled")
        batch_op.drop_column("requires_confirmation")
        batch_op.drop_column("warning_codes")
        batch_op.drop_column("bet_rule_version")
        batch_op.drop_column("strategy_mode")
        batch_op.drop_column("ai_analysis_id")
        batch_op.drop_column("source_type")

