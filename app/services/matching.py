"""
Reconciliation matching engine.

Performs multi-strategy matching between transactions/payouts and settlement
records, including exact ID matching, fuzzy amount+date matching, duplicate
detection, orphaned settlement detection, and batch reconciliation.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import (
    Transaction,
    Payout,
    SettlementRecord,
    ReconciliationRun,
    ReconciliationResult,
)
from app.services.currency import convert, to_usd

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
AMOUNT_TOLERANCE_PCT = 0.02  # 2 % tolerance for amount matching
CROSS_CURRENCY_EXTRA_PCT = 0.01  # extra 1 % for cross-currency pairs
DATE_WINDOW_DAYS = 7  # fuzzy matching date window
CURRENCY_SLIPPAGE_THRESHOLD = 0.05  # 5 % slippage flag
FUZZY_SCORE_AMOUNT_WEIGHT = 0.6
FUZZY_SCORE_DATE_WEIGHT = 0.4


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _safe_to_usd(amount: float, currency: str) -> float:
    """Convert to USD, returning the original amount if already USD."""
    if currency.upper() == "USD":
        return float(amount)
    return to_usd(float(amount), currency)


def _amount_diff_pct(expected: float, actual: float) -> float:
    """Return the absolute percentage difference between two amounts."""
    if expected == 0:
        return 0.0 if actual == 0 else 1.0
    return abs(expected - actual) / abs(expected)


def _days_between(dt1: datetime, dt2: datetime) -> float:
    """Return the absolute number of days between two datetimes."""
    delta = abs((dt2 - dt1).total_seconds())
    return delta / 86400.0


def _classify_discrepancy(
    expected_usd: float,
    actual_usd: float,
    source_currency: str,
    settlement_currency: str,
    fee_amount_usd: float,
    settlement_fee_usd: float,
) -> Optional[str]:
    """Determine the specific discrepancy type for a matched pair.

    Returns None when the amounts are within tolerance (i.e. clean match),
    or a discrepancy type string.
    """
    diff_pct = _amount_diff_pct(expected_usd, actual_usd)
    tolerance = AMOUNT_TOLERANCE_PCT
    if source_currency.upper() != settlement_currency.upper():
        tolerance += CROSS_CURRENCY_EXTRA_PCT

    if diff_pct <= tolerance:
        return None  # clean match

    # Currency slippage – cross-currency conversion delta > 5 %
    if source_currency.upper() != settlement_currency.upper() and diff_pct > CURRENCY_SLIPPAGE_THRESHOLD:
        return "currency_slippage"

    # Fee discrepancy – diff is roughly the fee amount
    if fee_amount_usd > 0:
        amount_minus_fee = expected_usd - fee_amount_usd
        if _amount_diff_pct(amount_minus_fee, actual_usd) <= tolerance:
            return "fee_discrepancy"

    if settlement_fee_usd > 0:
        if _amount_diff_pct(expected_usd - settlement_fee_usd, actual_usd) <= tolerance:
            return "fee_discrepancy"

    return "amount_mismatch"


# ---------------------------------------------------------------------------
# Core matching strategies
# ---------------------------------------------------------------------------

async def _fetch_records(db: AsyncSession):
    """Fetch all transactions, payouts, and settlements."""
    txns = (await db.execute(select(Transaction))).scalars().all()
    payouts = (await db.execute(select(Payout))).scalars().all()
    settlements = (await db.execute(select(SettlementRecord))).scalars().all()
    return list(txns), list(payouts), list(settlements)


def _exact_id_match(
    record_id: str,
    settlements: list[SettlementRecord],
    matched_settlement_ids: set[int],
) -> Optional[SettlementRecord]:
    """Find an exact settlement match by transaction_reference."""
    for s in settlements:
        if s.id not in matched_settlement_ids and s.transaction_reference == record_id:
            return s
    return None


def _fuzzy_match(
    amount_usd: float,
    capture_ts: datetime,
    currency: str,
    settlements: list[SettlementRecord],
    matched_settlement_ids: set[int],
) -> Optional[tuple[SettlementRecord, float]]:
    """Find the best fuzzy match among unmatched settlements.

    Returns (settlement, score) or None.
    """
    best: Optional[tuple[SettlementRecord, float]] = None
    for s in settlements:
        if s.id in matched_settlement_ids:
            continue

        s_amount_usd = _safe_to_usd(float(s.settled_amount), s.settlement_currency)
        diff_pct = _amount_diff_pct(amount_usd, s_amount_usd)

        # Tolerance depends on whether currencies differ
        tolerance = AMOUNT_TOLERANCE_PCT
        if currency.upper() != s.settlement_currency.upper():
            tolerance += CROSS_CURRENCY_EXTRA_PCT
        if diff_pct > tolerance:
            continue

        # Date window check
        days_diff = _days_between(capture_ts, s.settlement_date)
        if days_diff > DATE_WINDOW_DAYS:
            continue

        # Score: weighted combination of amount closeness and date closeness
        score = (1.0 - diff_pct) * FUZZY_SCORE_AMOUNT_WEIGHT + (
            1.0 - days_diff / DATE_WINDOW_DAYS
        ) * FUZZY_SCORE_DATE_WEIGHT

        if best is None or score > best[1]:
            best = (s, score)

    return best


def _detect_duplicates(
    settlements: list[SettlementRecord],
) -> dict[str, list[SettlementRecord]]:
    """Group settlements by transaction_reference and return groups with >1."""
    groups: dict[str, list[SettlementRecord]] = defaultdict(list)
    for s in settlements:
        if s.transaction_reference:
            groups[s.transaction_reference].append(s)
    return {ref: slist for ref, slist in groups.items() if len(slist) > 1}


def _make_result(
    run_id: int,
    record_type: str,
    record_id: str,
    currency: str,
    expected_usd: float,
    actual_usd: float | None,
    settlement_id: str | None,
    status: str,
    discrepancy_type: str | None,
    notes: str | None = None,
) -> ReconciliationResult:
    """Build a ReconciliationResult with correct field names."""
    delta = 0.0
    if actual_usd is not None:
        delta = round(expected_usd - actual_usd, 2)
    return ReconciliationResult(
        reconciliation_run_id=run_id,
        record_type=record_type,
        record_id=record_id,
        settlement_id=settlement_id,
        status=status,
        expected_amount=round(expected_usd, 2),
        actual_amount=round(actual_usd, 2) if actual_usd is not None else None,
        currency="USD",
        delta=delta,
        discrepancy_type=discrepancy_type,
        notes=notes,
    )


# ---------------------------------------------------------------------------
# Main reconciliation entry-point
# ---------------------------------------------------------------------------

async def run_reconciliation(db: AsyncSession) -> ReconciliationRun:
    """Execute a full reconciliation run.

    1. Create a ReconciliationRun (status=in_progress)
    2. Fetch all records
    3. Exact ID matching for transactions and payouts
    4. Fuzzy matching for unmatched records
    5. Duplicate detection
    6. Orphaned settlement detection
    7. Batch reconciliation
    8. Persist results and finalise the run
    """
    # Step 1 – create the run record
    run = ReconciliationRun(
        status="in_progress",
        matched_count=0,
        unmatched_count=0,
        discrepant_count=0,
        total_transactions=0,
        total_discrepancy_amount=0.0,
    )
    db.add(run)
    await db.flush()  # get run.id

    # Step 2 – fetch data
    transactions, payouts, settlements = await _fetch_records(db)

    matched_settlement_ids: set[int] = set()
    results: list[ReconciliationResult] = []
    total_discrepancy = 0.0

    # ------------------------------------------------------------------
    # Step 3+4 – match transactions
    # ------------------------------------------------------------------
    for txn in transactions:
        txn_amount_usd = _safe_to_usd(float(txn.amount), txn.currency)
        settlement = _exact_id_match(txn.transaction_id, settlements, matched_settlement_ids)
        match_type = "exact"

        if settlement is None:
            fuzzy = _fuzzy_match(
                txn_amount_usd, txn.capture_timestamp, txn.currency,
                settlements, matched_settlement_ids,
            )
            if fuzzy is not None:
                settlement, _score = fuzzy
                match_type = "fuzzy"

        if settlement is not None:
            matched_settlement_ids.add(settlement.id)
            s_amount_usd = _safe_to_usd(float(settlement.settled_amount), settlement.settlement_currency)
            s_fee_usd = _safe_to_usd(float(settlement.fees), settlement.settlement_currency) if settlement.fees else 0.0

            disc_type = _classify_discrepancy(
                txn_amount_usd, s_amount_usd,
                txn.currency, settlement.settlement_currency,
                0.0,  # Transaction model has no fee_amount
                s_fee_usd,
            )

            status = "matched" if disc_type is None else "discrepant"
            if disc_type:
                total_discrepancy += abs(txn_amount_usd - s_amount_usd)
            notes_str = f"{match_type}_match" if match_type == "fuzzy" else None
            result = _make_result(
                run.id, "transaction", txn.transaction_id, txn.currency,
                txn_amount_usd, s_amount_usd, settlement.settlement_id,
                status, disc_type, notes=notes_str,
            )
        else:
            result = _make_result(
                run.id, "transaction", txn.transaction_id, txn.currency,
                txn_amount_usd, None, None, "unmatched", None,
            )

        results.append(result)

    # ------------------------------------------------------------------
    # Match payouts
    # ------------------------------------------------------------------
    for payout in payouts:
        payout_amount_usd = _safe_to_usd(float(payout.amount), payout.currency)
        settlement = _exact_id_match(payout.payout_id, settlements, matched_settlement_ids)
        match_type = "exact"

        if settlement is None:
            fuzzy = _fuzzy_match(
                payout_amount_usd, payout.payout_timestamp, payout.currency,
                settlements, matched_settlement_ids,
            )
            if fuzzy is not None:
                settlement, _score = fuzzy
                match_type = "fuzzy"

        if settlement is not None:
            matched_settlement_ids.add(settlement.id)
            s_amount_usd = _safe_to_usd(float(settlement.settled_amount), settlement.settlement_currency)
            s_fee_usd = _safe_to_usd(float(settlement.fees), settlement.settlement_currency) if settlement.fees else 0.0

            disc_type = _classify_discrepancy(
                payout_amount_usd, s_amount_usd,
                payout.currency, settlement.settlement_currency,
                0.0,  # Payout model has no fee_amount
                s_fee_usd,
            )

            status = "matched" if disc_type is None else "discrepant"
            if disc_type:
                total_discrepancy += abs(payout_amount_usd - s_amount_usd)
            notes_str = f"{match_type}_match" if match_type == "fuzzy" else None
            result = _make_result(
                run.id, "payout", payout.payout_id, payout.currency,
                payout_amount_usd, s_amount_usd, settlement.settlement_id,
                status, disc_type, notes=notes_str,
            )
        else:
            result = _make_result(
                run.id, "payout", payout.payout_id, payout.currency,
                payout_amount_usd, None, None, "unmatched", None,
            )

        results.append(result)

    # ------------------------------------------------------------------
    # Step 5 – duplicate detection
    # ------------------------------------------------------------------
    duplicates = _detect_duplicates(settlements)
    for ref, dup_settlements in duplicates.items():
        # The first one is the "real" one; extras are duplicates
        for extra in dup_settlements[1:]:
            if extra.id in matched_settlement_ids:
                # Already matched – update the existing result to discrepant
                for r in results:
                    if r.settlement_id == extra.settlement_id and r.status == "matched":
                        r.status = "discrepant"
                        r.discrepancy_type = "duplicate"
                        break
            else:
                matched_settlement_ids.add(extra.id)
                s_amount_usd = _safe_to_usd(float(extra.settled_amount), extra.settlement_currency)
                results.append(_make_result(
                    run.id, "settlement", ref, extra.settlement_currency,
                    0.0, s_amount_usd, extra.settlement_id,
                    "discrepant", "duplicate",
                ))

    # ------------------------------------------------------------------
    # Step 6 – orphaned settlements
    # ------------------------------------------------------------------
    for s in settlements:
        if s.id not in matched_settlement_ids:
            s_amount_usd = _safe_to_usd(float(s.settled_amount), s.settlement_currency)
            results.append(_make_result(
                run.id, "settlement",
                s.transaction_reference or s.settlement_id,
                s.settlement_currency,
                0.0, s_amount_usd, s.settlement_id,
                "discrepant", "orphaned",
            ))
            matched_settlement_ids.add(s.id)

    # ------------------------------------------------------------------
    # Step 7 – batch reconciliation
    # ------------------------------------------------------------------
    batch_groups: dict[str, list[SettlementRecord]] = defaultdict(list)
    for s in settlements:
        if s.batch_id:
            batch_groups[s.batch_id].append(s)

    # Map settlement_id → matched result for quick lookup
    settlement_to_result: dict[str, ReconciliationResult] = {}
    for r in results:
        if r.settlement_id:
            settlement_to_result[r.settlement_id] = r

    for batch_id, batch_settlements in batch_groups.items():
        batch_settlement_sum_usd = sum(
            _safe_to_usd(float(s.settled_amount), s.settlement_currency)
            for s in batch_settlements
        )
        # Sum of the expected amounts for records matched to this batch
        matched_expected_sum_usd = 0.0
        for s in batch_settlements:
            r = settlement_to_result.get(s.settlement_id)
            if r and r.expected_amount is not None:
                matched_expected_sum_usd += r.expected_amount

        if matched_expected_sum_usd > 0:
            batch_diff = _amount_diff_pct(matched_expected_sum_usd, batch_settlement_sum_usd)
            if batch_diff > AMOUNT_TOLERANCE_PCT:
                logger.warning(
                    "Batch %s mismatch: expected=%.2f actual=%.2f diff=%.4f",
                    batch_id, matched_expected_sum_usd, batch_settlement_sum_usd, batch_diff,
                )

    # ------------------------------------------------------------------
    # Step 8 – persist and finalise
    # ------------------------------------------------------------------
    matched = sum(1 for r in results if r.status == "matched")
    unmatched = sum(1 for r in results if r.status == "unmatched")
    discrepant = sum(1 for r in results if r.status == "discrepant")

    run.total_transactions = len(transactions) + len(payouts)
    run.matched_count = matched
    run.unmatched_count = unmatched
    run.discrepant_count = discrepant
    run.total_discrepancy_amount = round(total_discrepancy, 2)
    run.status = "completed"

    db.add_all(results)
    await db.commit()
    await db.refresh(run)

    logger.info(
        "Reconciliation run %d complete: %d matched, %d unmatched, %d discrepant",
        run.id, matched, unmatched, discrepant,
    )

    return run
