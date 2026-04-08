"""Base parser and data structures for parsed filing data."""

from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal


@dataclass
class NavRecord:
    as_of_date: date
    share_class: str
    nav_per_share: Decimal | None  # None = N/A


@dataclass
class DistributionRecord:
    as_of_date: date
    share_class: str
    distribution_per_share: Decimal | None


@dataclass
class SharesIssuedRecord:
    as_of_date: date
    share_class: str
    offering_type: str
    cumulative_shares: Decimal | None
    cumulative_consideration: Decimal | None


@dataclass
class RedemptionRecord:
    as_of_date: date
    shares_redeemed: Decimal | None
    value_redeemed: Decimal | None
    source_form_type: str
    shares_tendered: Decimal | None = None
    pct_tendered_of_os: Decimal | None = None  # % of shares O/S tendered (e.g. 11.2)


@dataclass
class TotalNavRecord:
    as_of_date: date
    total_nav: Decimal | None


@dataclass
class SharesOutstandingRecord:
    as_of_date: date
    total_shares_outstanding: Decimal | None


@dataclass
class ParsedFiling:
    """Container for all data extracted from a single filing."""
    nav_records: list[NavRecord] = field(default_factory=list)
    distribution_records: list[DistributionRecord] = field(default_factory=list)
    shares_issued_records: list[SharesIssuedRecord] = field(default_factory=list)
    redemption_records: list[RedemptionRecord] = field(default_factory=list)
    total_nav_records: list[TotalNavRecord] = field(default_factory=list)
    shares_outstanding_records: list[SharesOutstandingRecord] = field(default_factory=list)

    @property
    def has_data(self) -> bool:
        return bool(
            self.nav_records
            or self.distribution_records
            or self.shares_issued_records
            or self.redemption_records
            or self.total_nav_records
            or self.shares_outstanding_records
        )
