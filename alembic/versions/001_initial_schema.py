"""Initial schema

Revision ID: 001
Revises:
Create Date: 2026-04-01
"""
from typing import Sequence, Union
from alembic import op
import sqlalchemy as sa

revision: str = "001"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "funds",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("ticker", sa.String(20), nullable=False, unique=True),
        sa.Column("name", sa.String(255), nullable=False),
        sa.Column("cik", sa.String(10), nullable=False, unique=True),
        sa.Column("active", sa.Boolean(), server_default=sa.text("true")),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    )

    op.create_table(
        "filings",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("fund_id", sa.Integer(), sa.ForeignKey("funds.id"), nullable=False),
        sa.Column("accession_number", sa.String(25), nullable=False, unique=True),
        sa.Column("form_type", sa.String(20), nullable=False),
        sa.Column("filing_date", sa.Date(), nullable=False),
        sa.Column("primary_document_url", sa.Text()),
        sa.Column("raw_html", sa.Text()),
        sa.Column("parsed_at", sa.DateTime(timezone=True)),
        sa.Column("parse_status", sa.String(20), server_default="pending"),
        sa.Column("parse_error", sa.Text()),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    )
    op.create_index("idx_filings_fund_form", "filings", ["fund_id", "form_type"])
    op.create_index("idx_filings_filing_date", "filings", ["filing_date"])

    op.create_table(
        "nav_per_share",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("fund_id", sa.Integer(), sa.ForeignKey("funds.id"), nullable=False),
        sa.Column("filing_id", sa.Integer(), sa.ForeignKey("filings.id")),
        sa.Column("as_of_date", sa.Date(), nullable=False),
        sa.Column("share_class", sa.String(50), nullable=False),
        sa.Column("nav_per_share", sa.Numeric(18, 4)),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.UniqueConstraint("fund_id", "as_of_date", "share_class", name="uq_nav_per_share"),
    )

    op.create_table(
        "distributions",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("fund_id", sa.Integer(), sa.ForeignKey("funds.id"), nullable=False),
        sa.Column("filing_id", sa.Integer(), sa.ForeignKey("filings.id")),
        sa.Column("as_of_date", sa.Date(), nullable=False),
        sa.Column("share_class", sa.String(50), nullable=False),
        sa.Column("distribution_per_share", sa.Numeric(18, 6)),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.UniqueConstraint("fund_id", "as_of_date", "share_class", name="uq_distributions"),
    )

    op.create_table(
        "shares_issued",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("fund_id", sa.Integer(), sa.ForeignKey("funds.id"), nullable=False),
        sa.Column("filing_id", sa.Integer(), sa.ForeignKey("filings.id")),
        sa.Column("as_of_date", sa.Date(), nullable=False),
        sa.Column("share_class", sa.String(50), nullable=False),
        sa.Column("offering_type", sa.String(50), nullable=False),
        sa.Column("cumulative_shares", sa.Numeric(18, 4)),
        sa.Column("cumulative_consideration", sa.Numeric(18, 2)),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.UniqueConstraint("fund_id", "as_of_date", "share_class", "offering_type",
                            name="uq_shares_issued"),
    )

    op.create_table(
        "redemptions",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("fund_id", sa.Integer(), sa.ForeignKey("funds.id"), nullable=False),
        sa.Column("filing_id", sa.Integer(), sa.ForeignKey("filings.id")),
        sa.Column("as_of_date", sa.Date(), nullable=False),
        sa.Column("shares_redeemed", sa.Numeric(18, 4)),
        sa.Column("value_redeemed", sa.Numeric(18, 2)),
        sa.Column("source_form_type", sa.String(20)),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.UniqueConstraint("fund_id", "as_of_date", name="uq_redemptions"),
    )

    op.create_table(
        "total_nav",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("fund_id", sa.Integer(), sa.ForeignKey("funds.id"), nullable=False),
        sa.Column("filing_id", sa.Integer(), sa.ForeignKey("filings.id")),
        sa.Column("as_of_date", sa.Date(), nullable=False),
        sa.Column("total_nav", sa.Numeric(18, 2)),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.UniqueConstraint("fund_id", "as_of_date", name="uq_total_nav"),
    )

    op.create_table(
        "update_log",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("completed_at", sa.DateTime(timezone=True)),
        sa.Column("trigger_type", sa.String(20), nullable=False),
        sa.Column("status", sa.String(20), server_default="running"),
        sa.Column("filings_processed", sa.Integer(), server_default="0"),
        sa.Column("errors", sa.Text()),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    )

    # Seed initial funds
    op.execute("""
        INSERT INTO funds (ticker, name, cik) VALUES
        ('BCRED', 'Blackstone Private Credit Fund', '1803498'),
        ('OCIC', 'Blue Owl Credit Income Corp', '1812554'),
        ('ADS', 'Apollo Debt Solutions BDC', '1782981'),
        ('HLEND', 'HPS Corporate Lending Fund', '1838126'),
        ('ASIF', 'Ares Strategic Income Fund', '1918712')
    """)


def downgrade() -> None:
    op.drop_table("update_log")
    op.drop_table("total_nav")
    op.drop_table("redemptions")
    op.drop_table("shares_issued")
    op.drop_table("distributions")
    op.drop_table("nav_per_share")
    op.drop_table("filings")
    op.drop_table("funds")
