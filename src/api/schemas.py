"""Pydantic response models for the API."""

from datetime import date, datetime
from decimal import Decimal
from pydantic import BaseModel, field_serializer


class FundResponse(BaseModel):
    ticker: str
    name: str
    cik: str
    active: bool

    model_config = {"from_attributes": True}


class NavPerShareResponse(BaseModel):
    as_of_date: date
    share_class: str
    nav_per_share: Decimal | None

    @field_serializer("nav_per_share")
    def serialize_nav(self, v):
        return str(v) if v is not None else "N/A"

    model_config = {"from_attributes": True}


class DistributionResponse(BaseModel):
    as_of_date: date
    share_class: str
    distribution_per_share: Decimal | None

    @field_serializer("distribution_per_share")
    def serialize_dist(self, v):
        return str(v) if v is not None else "N/A"

    model_config = {"from_attributes": True}


class SharesIssuedResponse(BaseModel):
    as_of_date: date
    share_class: str
    offering_type: str
    cumulative_shares: Decimal | None
    cumulative_consideration: Decimal | None

    @field_serializer("cumulative_shares")
    def serialize_shares(self, v):
        return str(v) if v is not None else "N/A"

    @field_serializer("cumulative_consideration")
    def serialize_consid(self, v):
        return str(v) if v is not None else "N/A"

    model_config = {"from_attributes": True}


class RedemptionResponse(BaseModel):
    as_of_date: date
    shares_redeemed: Decimal | None
    value_redeemed: Decimal | None
    source_form_type: str | None

    @field_serializer("shares_redeemed")
    def serialize_shares(self, v):
        return str(v) if v is not None else "N/A"

    @field_serializer("value_redeemed")
    def serialize_value(self, v):
        return str(v) if v is not None else "N/A"

    model_config = {"from_attributes": True}


class TotalNavResponse(BaseModel):
    as_of_date: date
    total_nav: Decimal | None

    @field_serializer("total_nav")
    def serialize_nav(self, v):
        return str(v) if v is not None else "N/A"

    model_config = {"from_attributes": True}


class UpdateTriggerResponse(BaseModel):
    update_id: int
    status: str
    message: str


class UpdateStatusResponse(BaseModel):
    id: int
    started_at: datetime
    completed_at: datetime | None
    trigger_type: str
    status: str
    filings_processed: int
    errors: str | None

    model_config = {"from_attributes": True}


class FundMetricsResponse(BaseModel):
    fund: FundResponse
    nav_per_share: list[NavPerShareResponse]
    distributions: list[DistributionResponse]
    shares_issued: list[SharesIssuedResponse]
    redemptions: list[RedemptionResponse]
    total_nav: list[TotalNavResponse]
