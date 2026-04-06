from datetime import date, datetime
from sqlalchemy import (
    Column, Integer, String, Boolean, Date, DateTime, Numeric, Text,
    ForeignKey, UniqueConstraint, Index, func,
)
from src.database import Base


class Fund(Base):
    __tablename__ = "funds"

    id = Column(Integer, primary_key=True)
    ticker = Column(String(20), nullable=False, unique=True)
    name = Column(String(255), nullable=False)
    cik = Column(String(10), nullable=False, unique=True)
    active = Column(Boolean, default=True, server_default="1")
    created_at = Column(DateTime(timezone=True), server_default=func.now())


class Filing(Base):
    __tablename__ = "filings"

    id = Column(Integer, primary_key=True)
    fund_id = Column(Integer, ForeignKey("funds.id"), nullable=False)
    accession_number = Column(String(25), nullable=False, unique=True)
    form_type = Column(String(20), nullable=False)
    filing_date = Column(Date, nullable=False)
    primary_document_url = Column(Text)
    raw_html = Column(Text)
    parsed_at = Column(DateTime(timezone=True))
    parse_status = Column(String(20), default="pending")
    parse_error = Column(Text)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    __table_args__ = (
        Index("idx_filings_fund_form", "fund_id", "form_type"),
        Index("idx_filings_filing_date", "filing_date"),
    )


class NavPerShare(Base):
    __tablename__ = "nav_per_share"

    id = Column(Integer, primary_key=True)
    fund_id = Column(Integer, ForeignKey("funds.id"), nullable=False)
    filing_id = Column(Integer, ForeignKey("filings.id"))
    as_of_date = Column(Date, nullable=False)
    share_class = Column(String(50), nullable=False)
    nav_per_share = Column(Numeric(18, 4))
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    __table_args__ = (
        UniqueConstraint("fund_id", "as_of_date", "share_class", name="uq_nav_per_share"),
    )


class Distribution(Base):
    __tablename__ = "distributions"

    id = Column(Integer, primary_key=True)
    fund_id = Column(Integer, ForeignKey("funds.id"), nullable=False)
    filing_id = Column(Integer, ForeignKey("filings.id"))
    as_of_date = Column(Date, nullable=False)
    share_class = Column(String(50), nullable=False)
    distribution_per_share = Column(Numeric(18, 6))
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    __table_args__ = (
        UniqueConstraint("fund_id", "as_of_date", "share_class", name="uq_distributions"),
    )


class SharesIssued(Base):
    __tablename__ = "shares_issued"

    id = Column(Integer, primary_key=True)
    fund_id = Column(Integer, ForeignKey("funds.id"), nullable=False)
    filing_id = Column(Integer, ForeignKey("filings.id"))
    as_of_date = Column(Date, nullable=False)
    share_class = Column(String(50), nullable=False)
    offering_type = Column(String(50), nullable=False)
    cumulative_shares = Column(Numeric(18, 4))
    cumulative_consideration = Column(Numeric(18, 2))
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    __table_args__ = (
        UniqueConstraint("fund_id", "as_of_date", "share_class", "offering_type",
                         name="uq_shares_issued"),
    )


class Redemption(Base):
    __tablename__ = "redemptions"

    id = Column(Integer, primary_key=True)
    fund_id = Column(Integer, ForeignKey("funds.id"), nullable=False)
    filing_id = Column(Integer, ForeignKey("filings.id"))
    as_of_date = Column(Date, nullable=False)
    shares_tendered = Column(Numeric(18, 4))
    shares_redeemed = Column(Numeric(18, 4))
    value_redeemed = Column(Numeric(18, 2))
    source_form_type = Column(String(20))
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    __table_args__ = (
        UniqueConstraint("fund_id", "as_of_date", name="uq_redemptions"),
    )


class TotalNav(Base):
    __tablename__ = "total_nav"

    id = Column(Integer, primary_key=True)
    fund_id = Column(Integer, ForeignKey("funds.id"), nullable=False)
    filing_id = Column(Integer, ForeignKey("filings.id"))
    as_of_date = Column(Date, nullable=False)
    total_nav = Column(Numeric(18, 2))
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    __table_args__ = (
        UniqueConstraint("fund_id", "as_of_date", name="uq_total_nav"),
    )


class SharesOutstanding(Base):
    __tablename__ = "shares_outstanding"

    id = Column(Integer, primary_key=True)
    fund_id = Column(Integer, ForeignKey("funds.id"), nullable=False)
    filing_id = Column(Integer, ForeignKey("filings.id"))
    as_of_date = Column(Date, nullable=False)
    total_shares_outstanding = Column(Numeric(18, 0))
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    __table_args__ = (
        UniqueConstraint("fund_id", "as_of_date", name="uq_shares_outstanding"),
    )


class UpdateLog(Base):
    __tablename__ = "update_log"

    id = Column(Integer, primary_key=True)
    started_at = Column(DateTime(timezone=True), nullable=False)
    completed_at = Column(DateTime(timezone=True))
    trigger_type = Column(String(20), nullable=False)
    status = Column(String(20), default="running")
    filings_processed = Column(Integer, default=0)
    errors = Column(Text)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
