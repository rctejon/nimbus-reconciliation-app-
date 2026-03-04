"""
Comprehensive tests for the reconciliation matching engine and alert system.

Uses an in-memory SQLite database via pytest-asyncio fixtures.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
import pytest_asyncio
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.database import Base
from app.models import (
    Transaction,
    Payout,
    SettlementRecord,
    ReconciliationRun,
    ReconciliationResult,
)
from app.services.matching import run_reconciliation
from app.services.alerts import generate_alerts

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

NOW = datetime.now(timezone.utc)
YESTERDAY = NOW - timedelta(days=1)
TWO_DAYS_AGO = NOW - timedelta(days=2)
TWENTY_DAYS_AGO = NOW - timedelta(days=20)


@pytest_asyncio.fixture
async def engine():
    eng = create_async_engine("sqlite+aiosqlite:///:memory:", echo=False)
    async with eng.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield eng
    await eng.dispose()


@pytest_asyncio.fixture
async def db(engine):
    session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    async with session_factory() as session:
        yield session


# ---------------------------------------------------------------------------
# Helpers to seed data
# ---------------------------------------------------------------------------

def _txn(
    transaction_id: str,
    amount: float = 100.0,
    currency: str = "USD",
    capture_timestamp: datetime | None = None,
    status: str = "captured",
    processor: str = "stripe",
) -> Transaction:
    return Transaction(
        transaction_id=transaction_id,
        amount=amount,
        currency=currency,
        capture_timestamp=capture_timestamp or YESTERDAY,
        status=status,
        processor=processor,
    )


def _payout(
    payout_id: str,
    amount: float = 200.0,
    currency: str = "USD",
    payout_timestamp: datetime | None = None,
    status: str = "completed",
    recipient: str = "merchant_001",
) -> Payout:
    return Payout(
        payout_id=payout_id,
        amount=amount,
        currency=currency,
        payout_timestamp=payout_timestamp or YESTERDAY,
        status=status,
        recipient=recipient,
    )


def _settlement(
    settlement_id: str,
    transaction_reference: str | None = None,
    settled_amount: float = 100.0,
    settlement_currency: str = "USD",
    settlement_date: datetime | None = None,
    batch_id: str | None = None,
    fees: float = 0.0,
) -> SettlementRecord:
    return SettlementRecord(
        settlement_id=settlement_id,
        transaction_reference=transaction_reference,
        settled_amount=settled_amount,
        settlement_currency=settlement_currency,
        settlement_date=settlement_date or NOW,
        batch_id=batch_id,
        fees=fees,
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_exact_id_match(db: AsyncSession):
    """Transaction with matching settlement by transaction_reference -> matched."""
    db.add(_txn("TXN-001", amount=100.0))
    db.add(_settlement("SET-001", transaction_reference="TXN-001", settled_amount=100.0))
    await db.flush()

    run = await run_reconciliation(db)

    assert run.status == "completed"
    assert run.matched_count == 1
    assert run.unmatched_count == 0
    assert run.discrepant_count == 0

    results = (
        (await db.execute(
            select(ReconciliationResult).where(
                ReconciliationResult.reconciliation_run_id == run.id
            )
        ))
        .scalars()
        .all()
    )
    assert len(results) == 1
    assert results[0].status == "matched"
    assert results[0].expected_amount == 100.0
    assert results[0].actual_amount == 100.0
    assert results[0].delta == 0.0


@pytest.mark.asyncio
async def test_fuzzy_amount_date_match(db: AsyncSession):
    """Settlement with slightly different amount and within date window -> fuzzy matched."""
    db.add(_txn("TXN-002", amount=100.0, capture_timestamp=TWO_DAYS_AGO))
    # No exact reference, but amount is within 2% and date within 7 days
    db.add(
        _settlement(
            "SET-002",
            transaction_reference=None,
            settled_amount=101.5,  # 1.5% diff
            settlement_date=YESTERDAY,
        )
    )
    await db.flush()

    run = await run_reconciliation(db)

    results = (
        (await db.execute(
            select(ReconciliationResult).where(
                ReconciliationResult.reconciliation_run_id == run.id,
                ReconciliationResult.record_id == "TXN-002",
            )
        ))
        .scalars()
        .all()
    )
    assert len(results) == 1
    r = results[0]
    # Should be fuzzy matched (amount within 2%)
    assert r.status == "matched"
    assert r.notes == "fuzzy_match"


@pytest.mark.asyncio
async def test_currency_conversion_matching(db: AsyncSession):
    """Transaction in MXN matched against USD settlement using currency conversion."""
    # 1950 MXN at rate 19.5 = 100 USD
    db.add(_txn("TXN-003", amount=1950.0, currency="MXN"))
    db.add(_settlement("SET-003", transaction_reference="TXN-003", settled_amount=100.0, settlement_currency="USD"))
    await db.flush()

    run = await run_reconciliation(db)

    results = (
        (await db.execute(
            select(ReconciliationResult).where(
                ReconciliationResult.reconciliation_run_id == run.id,
                ReconciliationResult.record_id == "TXN-003",
            )
        ))
        .scalars()
        .all()
    )
    assert len(results) == 1
    assert results[0].status == "matched"
    assert abs(results[0].expected_amount - 100.0) < 0.01
    assert abs(results[0].actual_amount - 100.0) < 0.01


@pytest.mark.asyncio
async def test_currency_slippage_detection(db: AsyncSession):
    """Cross-currency mismatch > 5% flagged as currency_slippage."""
    # 1950 MXN = 100 USD, but settlement is 90 USD (10% diff)
    db.add(_txn("TXN-004", amount=1950.0, currency="MXN"))
    db.add(_settlement("SET-004", transaction_reference="TXN-004", settled_amount=90.0, settlement_currency="USD"))
    await db.flush()

    run = await run_reconciliation(db)

    results = (
        (await db.execute(
            select(ReconciliationResult).where(
                ReconciliationResult.reconciliation_run_id == run.id,
                ReconciliationResult.record_id == "TXN-004",
            )
        ))
        .scalars()
        .all()
    )
    assert len(results) == 1
    assert results[0].status == "discrepant"
    assert results[0].discrepancy_type == "currency_slippage"


@pytest.mark.asyncio
async def test_duplicate_detection(db: AsyncSession):
    """Multiple settlements with same transaction_reference -> duplicates detected."""
    db.add(_txn("TXN-005", amount=100.0))
    db.add(_settlement("SET-005a", transaction_reference="TXN-005", settled_amount=100.0))
    db.add(_settlement("SET-005b", transaction_reference="TXN-005", settled_amount=100.0))
    await db.flush()

    run = await run_reconciliation(db)

    results = (
        (await db.execute(
            select(ReconciliationResult).where(
                ReconciliationResult.reconciliation_run_id == run.id
            )
        ))
        .scalars()
        .all()
    )
    dup_results = [r for r in results if r.discrepancy_type == "duplicate"]
    assert len(dup_results) >= 1


@pytest.mark.asyncio
async def test_orphaned_settlement(db: AsyncSession):
    """Settlement with no matching transaction or payout -> orphaned."""
    db.add(
        _settlement("SET-006", transaction_reference="UNKNOWN-REF", settled_amount=500.0)
    )
    await db.flush()

    run = await run_reconciliation(db)

    results = (
        (await db.execute(
            select(ReconciliationResult).where(
                ReconciliationResult.reconciliation_run_id == run.id
            )
        ))
        .scalars()
        .all()
    )
    assert len(results) == 1
    assert results[0].status == "discrepant"
    assert results[0].discrepancy_type == "orphaned"


@pytest.mark.asyncio
async def test_unmatched_transaction(db: AsyncSession):
    """Transaction with no settlement at all -> unmatched."""
    db.add(_txn("TXN-007", amount=250.0))
    await db.flush()

    run = await run_reconciliation(db)

    assert run.unmatched_count == 1
    results = (
        (await db.execute(
            select(ReconciliationResult).where(
                ReconciliationResult.reconciliation_run_id == run.id
            )
        ))
        .scalars()
        .all()
    )
    assert len(results) == 1
    assert results[0].status == "unmatched"
    assert results[0].settlement_id is None


@pytest.mark.asyncio
async def test_full_reconciliation_mixed(db: AsyncSession):
    """Full run with a mix of matched, unmatched, discrepant, and orphaned records."""
    # Exact match
    db.add(_txn("TXN-100", amount=100.0))
    db.add(_settlement("SET-100", transaction_reference="TXN-100", settled_amount=100.0))

    # Payout exact match
    db.add(_payout("PAY-100", amount=200.0))
    db.add(_settlement("SET-101", transaction_reference="PAY-100", settled_amount=200.0))

    # Unmatched transaction
    db.add(_txn("TXN-101", amount=300.0))

    # Orphaned settlement
    db.add(_settlement("SET-102", transaction_reference="GONE", settled_amount=50.0))

    # Amount mismatch (>2% diff)
    db.add(_txn("TXN-102", amount=1000.0))
    db.add(_settlement("SET-103", transaction_reference="TXN-102", settled_amount=900.0))

    await db.flush()

    run = await run_reconciliation(db)

    assert run.status == "completed"
    assert run.total_transactions == 4  # 3 txns + 1 payout

    results = (
        (await db.execute(
            select(ReconciliationResult).where(
                ReconciliationResult.reconciliation_run_id == run.id
            )
        ))
        .scalars()
        .all()
    )

    statuses = {r.record_id: r.status for r in results}
    assert statuses["TXN-100"] == "matched"
    assert statuses["PAY-100"] == "matched"
    assert statuses["TXN-101"] == "unmatched"
    assert statuses["TXN-102"] == "discrepant"

    orphans = [r for r in results if r.discrepancy_type == "orphaned"]
    assert len(orphans) == 1


@pytest.mark.asyncio
async def test_alert_generation(db: AsyncSession):
    """Alerts are generated correctly for different discrepancy types."""
    # Large discrepancy > $500
    db.add(_txn("TXN-A1", amount=2000.0))
    db.add(_settlement("SET-A1", transaction_reference="TXN-A1", settled_amount=1000.0))

    # Unmatched > 14 days old
    db.add(_txn("TXN-A2", amount=50.0, capture_timestamp=TWENTY_DAYS_AGO))

    # Currency slippage
    db.add(_txn("TXN-A3", amount=1950.0, currency="MXN"))
    db.add(_settlement("SET-A3", transaction_reference="TXN-A3", settled_amount=90.0, settlement_currency="USD"))

    # Fee discrepancy: settlement has fees=5.0, settled_amount=95.0, txn=100
    db.add(_txn("TXN-A4", amount=100.0))
    db.add(_settlement("SET-A4", transaction_reference="TXN-A4", settled_amount=95.0, fees=5.0))

    await db.flush()

    run = await run_reconciliation(db)
    alerts = await generate_alerts(db, run.id)

    types_found = {a["type"] for a in alerts}
    assert "large_discrepancy" in types_found
    assert "stale_unmatched" in types_found
    assert "currency_slippage" in types_found
    assert "fee_discrepancy" in types_found

    # Verify sorted by priority (HIGH first)
    priorities = [a["priority"] for a in alerts]
    high_indices = [i for i, p in enumerate(priorities) if p == "HIGH"]
    medium_indices = [i for i, p in enumerate(priorities) if p == "MEDIUM"]
    low_indices = [i for i, p in enumerate(priorities) if p == "LOW"]

    if high_indices and medium_indices:
        assert max(high_indices) < min(medium_indices)
    if medium_indices and low_indices:
        assert max(medium_indices) < min(low_indices)


@pytest.mark.asyncio
async def test_payout_exact_match(db: AsyncSession):
    """Payout matched by exact ID via settlement transaction_reference."""
    db.add(_payout("PAY-001", amount=500.0))
    db.add(_settlement("SET-P1", transaction_reference="PAY-001", settled_amount=500.0))
    await db.flush()

    run = await run_reconciliation(db)

    assert run.matched_count == 1
    results = (
        (await db.execute(
            select(ReconciliationResult).where(
                ReconciliationResult.reconciliation_run_id == run.id
            )
        ))
        .scalars()
        .all()
    )
    assert len(results) == 1
    assert results[0].record_type == "payout"
    assert results[0].status == "matched"
