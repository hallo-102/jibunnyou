"""add persistent notification history and read state

Revision ID: 0006_notifications
Revises: 0005_result_analytics
Create Date: 2026-07-22 21:40:00
"""

from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa


revision: str = "0006_notifications"
down_revision: str | None = "0005_result_analytics"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "notifications",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("category", sa.String(length=50), nullable=False),
        sa.Column("severity", sa.String(length=16), nullable=False),
        sa.Column("title", sa.String(length=200), nullable=False),
        sa.Column("message", sa.Text(), nullable=False),
        sa.Column("source_type", sa.String(length=50), nullable=False),
        sa.Column("source_id", sa.String(length=200), nullable=False),
        sa.Column("race_id", sa.String(length=32), nullable=True),
        sa.Column("race_date", sa.Date(), nullable=True),
        sa.Column("action_anchor", sa.String(length=200), nullable=True),
        sa.Column("is_read", sa.Boolean(), server_default=sa.false(), nullable=False),
        sa.Column("read_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(
            "severity IN ('info','warning','error')",
            name="ck_notifications_severity",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "category",
            "source_type",
            "source_id",
            name="uq_notifications_source",
        ),
    )
    op.create_index("ix_notifications_category", "notifications", ["category"], unique=False)
    op.create_index("ix_notifications_severity", "notifications", ["severity"], unique=False)
    op.create_index("ix_notifications_race_id", "notifications", ["race_id"], unique=False)
    op.create_index("ix_notifications_race_date", "notifications", ["race_date"], unique=False)
    op.create_index(
        "ix_notifications_unread_time",
        "notifications",
        ["is_read", "created_at"],
        unique=False,
    )
    op.create_index(
        "ix_notifications_race_time",
        "notifications",
        ["race_id", "created_at"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_notifications_race_time", table_name="notifications")
    op.drop_index("ix_notifications_unread_time", table_name="notifications")
    op.drop_index("ix_notifications_race_date", table_name="notifications")
    op.drop_index("ix_notifications_race_id", table_name="notifications")
    op.drop_index("ix_notifications_severity", table_name="notifications")
    op.drop_index("ix_notifications_category", table_name="notifications")
    op.drop_table("notifications")
