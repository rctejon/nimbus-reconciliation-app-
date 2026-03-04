from __future__ import annotations

from datetime import datetime
from typing import Dict, Generic, List, Optional, TypeVar

from pydantic import BaseModel, ConfigDict

T = TypeVar("T")


# ── Transaction ──────────────────────────────────────────────────────────────

class TransactionCreate(BaseModel):
    transaction_id: str
    amount: float
    currency: str
    processor: str
    capture_timestamp: datetime
    status: str = "completed"


class TransactionResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    transaction_id: str
    amount: float
    currency: str
    processor: str
    capture_timestamp: datetime
    status: str
    created_at: datetime


# ── Payout ───────────────────────────────────────────────────────────────────

class PayoutCreate(BaseModel):
    payout_id: str
    amount: float
    currency: str
    recipient: str
    payout_timestamp: datetime
    status: str = "completed"


class PayoutResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    payout_id: str
    amount: float
    currency: str
    recipient: str
    payout_timestamp: datetime
    status: str
    created_at: datetime


# ── Settlement Record ────────────────────────────────────────────────────────

class SettlementRecordCreate(BaseModel):
    settlement_id: str
    transaction_reference: Optional[str] = None
    settled_amount: float
    settlement_currency: str
    settlement_date: datetime
    fees: float = 0.0
    batch_id: Optional[str] = None


class SettlementRecordResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    settlement_id: str
    transaction_reference: Optional[str]
    settled_amount: float
    settlement_currency: str
    settlement_date: datetime
    fees: float
    batch_id: Optional[str]
    created_at: datetime


# ── Reconciliation Run ──────────────────────────────────────────────────────

class ReconciliationRunResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    run_timestamp: datetime
    total_transactions: int
    matched_count: int
    unmatched_count: int
    discrepant_count: int
    total_discrepancy_amount: float
    status: str


# ── Reconciliation Result ───────────────────────────────────────────────────

class ReconciliationResultResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    record_type: str
    record_id: str
    settlement_id: Optional[str]
    status: str
    expected_amount: float
    actual_amount: Optional[float]
    currency: str
    delta: float
    discrepancy_type: Optional[str]
    notes: Optional[str]
    reconciliation_run_id: int
    created_at: datetime


# ── Ingestion ────────────────────────────────────────────────────────────────

class IngestionData(BaseModel):
    count: int


class IngestionResponse(BaseModel):
    status: str = "success"
    data: IngestionData


# ── Discrepancy ──────────────────────────────────────────────────────────────

class DiscrepancyResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    record_type: str
    record_id: str
    settlement_id: Optional[str]
    expected_amount: float
    actual_amount: Optional[float]
    currency: str
    delta: float
    discrepancy_type: Optional[str]
    notes: Optional[str]


# ── Summary ──────────────────────────────────────────────────────────────────

class SummaryResponse(BaseModel):
    total_transactions: int
    matched: int
    unmatched: int
    discrepancies: int
    total_discrepancy_amount: float
    match_rate: float
    currency_breakdown: Optional[Dict[str, float]] = None


# ── Alert ────────────────────────────────────────────────────────────────────

class AlertResponse(BaseModel):
    alert_type: str
    severity: str
    message: str
    details: Optional[Dict] = None


# ── Batch Summary ────────────────────────────────────────────────────────────

class BatchSummaryResponse(BaseModel):
    batch_id: str
    total_records: int
    matched: int
    unmatched: int
    discrepancies: int
    total_settled: float
    total_expected: float
    net_difference: float


# ── Paginated ────────────────────────────────────────────────────────────────

class PaginatedResponse(BaseModel, Generic[T]):
    items: List[T]
    total: int
    page: int
    page_size: int
    pages: int
