"""Add shares_outstanding table and shares_tendered column

Revision ID: 002
Revises: 001
Create Date: 2026-04-06
"""
from typing import Sequence, Union
from alembic import op
import sqlalchemy as sa

revision: str = "002"
down_revision: Union[str, None] = "001"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "shares_outstanding",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("fund_id", sa.Integer(), sa.ForeignKey("funds.id"), nullable=False),
        sa.Column("filing_id", sa.Integer(), sa.ForeignKey("filings.id")),
        sa.Column("as_of_date", sa.Date(), nullable=False),
        sa.Column("total_shares_outstanding", sa.Numeric(18, 0)),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.UniqueConstraint("fund_id", "as_of_date", name="uq_shares_outstanding"),
    )

    with op.batch_alter_table("redemptions") as batch_op:
        batch_op.add_column(sa.Column("shares_tendered", sa.Numeric(18, 4)))


def downgrade() -> None:
    with op.batch_alter_table("redemptions") as batch_op:
        batch_op.drop_column("shares_tendered")
    op.drop_table("shares_outstanding")
