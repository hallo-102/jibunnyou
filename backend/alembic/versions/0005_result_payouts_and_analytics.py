"""add structured result payouts and settlement details

Revision ID: 0005_result_analytics
Revises: 0004_bet_plan_safety
Create Date: 2026-07-11 01:00:00
"""

from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa


revision: str = "0005_result_analytics"
down_revision: str | None = "0004_bet_plan_safety"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    with op.batch_alter_table("race_results", schema=None) as batch_op:
        batch_op.add_column(sa.Column("result_status", sa.String(length=32), server_default="confirmed", nullable=False))
        batch_op.add_column(sa.Column("payouts_json", sa.JSON(), server_default=sa.text("'[]'"), nullable=False))
        batch_op.add_column(sa.Column("cancelled_horse_nos", sa.JSON(), server_default=sa.text("'[]'"), nullable=False))
        batch_op.add_column(sa.Column("disqualified_horse_nos", sa.JSON(), server_default=sa.text("'[]'"), nullable=False))
        batch_op.add_column(sa.Column("has_dead_heat", sa.Boolean(), server_default=sa.false(), nullable=False))
        batch_op.add_column(sa.Column("confirmed_at", sa.DateTime(timezone=True), nullable=True))
        batch_op.create_check_constraint(
            "ck_race_results_status",
            "result_status IN ('provisional','confirmed','cancelled')",
        )

    with op.batch_alter_table("bet_settlements", schema=None) as batch_op:
        batch_op.add_column(sa.Column("bet_type", sa.String(length=64), server_default="3連複", nullable=False))
        batch_op.add_column(sa.Column("source_type", sa.String(length=32), server_default="python", nullable=False))
        batch_op.add_column(sa.Column("hit_count", sa.Integer(), server_default="0", nullable=False))
        batch_op.add_column(sa.Column("winning_combinations", sa.JSON(), server_default=sa.text("'[]'"), nullable=False))
        batch_op.add_column(sa.Column("payout_details_json", sa.JSON(), server_default=sa.text("'[]'"), nullable=False))
        batch_op.add_column(sa.Column("result_status", sa.String(length=32), server_default="confirmed", nullable=False))
        batch_op.create_check_constraint("ck_bet_settlements_hit_count", "hit_count >= 0")


def downgrade() -> None:
    with op.batch_alter_table("bet_settlements", schema=None) as batch_op:
        batch_op.drop_constraint("ck_bet_settlements_hit_count", type_="check")
        batch_op.drop_column("result_status")
        batch_op.drop_column("payout_details_json")
        batch_op.drop_column("winning_combinations")
        batch_op.drop_column("hit_count")
        batch_op.drop_column("source_type")
        batch_op.drop_column("bet_type")

    with op.batch_alter_table("race_results", schema=None) as batch_op:
        batch_op.drop_constraint("ck_race_results_status", type_="check")
        batch_op.drop_column("confirmed_at")
        batch_op.drop_column("has_dead_heat")
        batch_op.drop_column("disqualified_horse_nos")
        batch_op.drop_column("cancelled_horse_nos")
        batch_op.drop_column("payouts_json")
        batch_op.drop_column("result_status")

