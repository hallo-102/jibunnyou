"""Add manual ChatGPT prompt and response history.

Revision ID: 0007_chatgpt_manual
Revises: 0006_notifications
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op


revision: str = "0007_chatgpt_manual"
down_revision: str | None = "0006_notifications"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Add a new table without modifying or deleting historical AI tables."""

    op.create_table(
        "chatgpt_manual_predictions",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("race_id", sa.String(length=32), nullable=False),
        sa.Column(
            "source",
            sa.String(length=32),
            server_default="chatgpt_manual",
            nullable=False,
        ),
        sa.Column("prompt_text", sa.Text(), nullable=False),
        sa.Column("response_text", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(
            "source = 'chatgpt_manual'",
            name="ck_chatgpt_manual_predictions_source",
        ),
        sa.ForeignKeyConstraint(
            ["race_id"],
            ["races.race_id"],
            name="fk_chatgpt_manual_predictions_race_id",
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_chatgpt_manual_predictions_race_id",
        "chatgpt_manual_predictions",
        ["race_id"],
        unique=False,
    )
    op.create_index(
        "ix_chatgpt_manual_predictions_race_created",
        "chatgpt_manual_predictions",
        ["race_id", "created_at"],
        unique=False,
    )


def downgrade() -> None:
    """Remove only the manual ChatGPT table when explicitly downgrading."""

    op.drop_index(
        "ix_chatgpt_manual_predictions_race_created",
        table_name="chatgpt_manual_predictions",
    )
    op.drop_index(
        "ix_chatgpt_manual_predictions_race_id",
        table_name="chatgpt_manual_predictions",
    )
    op.drop_table("chatgpt_manual_predictions")
