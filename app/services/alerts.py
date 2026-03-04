"""
Alert generation for reconciliation results.

Scans ReconciliationResult entries for a given run and produces alerts
sorted by priority (HIGH > MEDIUM > LOW).
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import ReconciliationResult, Transaction, Payout

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Thresholds
# ---------------------------------------------------------------------------
HIGH_DISCREPANCY_USD = 500.0
UNMATCHED_AGE_DAYS = 14
CURRENCY_SLIPPAGE_THRESHOLD_PCT = 0.05

PRIORITY_ORDER = {"HIGH": 0, "MEDIUM": 1, "LOW": 2}


def _priority_sort_key(alert: dict) -> tuple[int, float]:
    """Sort alerts by priority (HIGH first) then by amount descending."""
    prio = PRIORITY_ORDER.get(alert["priority"], 99)
    amount = alert.get("amount_usd") or 0.0
    return (prio, -amount)


async def generate_alerts(
    db: AsyncSession, run_id: int
) -> list[dict]:
    """Generate alerts for a completed reconciliation run.

    Alert types:
        HIGH   – single discrepancy > $500 USD
        MEDIUM – unmatched transaction older than 14 days
        HIGH   – currency slippage > 5 %
        LOW    – fee discrepancy

    Returns a list of alert dicts sorted by priority.
    """
    results = (
        (await db.execute(
            select(ReconciliationResult).where(
                ReconciliationResult.reconciliation_run_id == run_id
            )
        ))
        .scalars()
        .all()
    )

    # Pre-fetch capture timestamps for unmatched-age check
    txn_map: dict[str, datetime] = {}
    payout_map: dict[str, datetime] = {}
    txns = (await db.execute(select(Transaction))).scalars().all()
    for t in txns:
        txn_map[t.transaction_id] = t.capture_timestamp
    payouts_list = (await db.execute(select(Payout))).scalars().all()
    for p in payouts_list:
        payout_map[p.payout_id] = p.payout_timestamp

    alerts: list[dict] = []
    now = datetime.now(timezone.utc)

    for result in results:
        # -----------------------------------------------------------------
        # HIGH: single discrepancy > $500 USD
        # -----------------------------------------------------------------
        if result.status == "discrepant" and result.discrepancy_type not in (
            "fee_discrepancy",
            "currency_slippage",
        ):
            expected = result.expected_amount or 0.0
            actual = result.actual_amount or 0.0
            diff = abs(expected - actual)
            if diff > HIGH_DISCREPANCY_USD:
                alerts.append(
                    {
                        "priority": "HIGH",
                        "type": "large_discrepancy",
                        "run_id": run_id,
                        "result_id": result.id,
                        "record_type": result.record_type,
                        "record_id": result.record_id,
                        "message": (
                            f"Discrepancy of ${diff:,.2f} USD on "
                            f"{result.record_type} {result.record_id}"
                        ),
                        "amount_usd": diff,
                    }
                )

        # -----------------------------------------------------------------
        # HIGH: currency slippage > 5 %
        # -----------------------------------------------------------------
        if result.discrepancy_type == "currency_slippage":
            expected = result.expected_amount or 0.0
            actual = result.actual_amount or 0.0
            diff = abs(expected - actual)
            pct = (diff / expected * 100) if expected else 0.0
            alerts.append(
                {
                    "priority": "HIGH",
                    "type": "currency_slippage",
                    "run_id": run_id,
                    "result_id": result.id,
                    "record_type": result.record_type,
                    "record_id": result.record_id,
                    "message": (
                        f"Currency slippage of {pct:.1f}% (${diff:,.2f} USD) on "
                        f"{result.record_type} {result.record_id}"
                    ),
                    "amount_usd": diff,
                }
            )

        # -----------------------------------------------------------------
        # MEDIUM: unmatched transaction older than 14 days
        # -----------------------------------------------------------------
        if result.status == "unmatched":
            capture_ts = txn_map.get(result.record_id) or payout_map.get(result.record_id)
            if capture_ts:
                # Ensure timezone-aware comparison (SQLite may strip tzinfo)
                if capture_ts.tzinfo is None:
                    capture_ts = capture_ts.replace(tzinfo=timezone.utc)
                age_days = (now - capture_ts).total_seconds() / 86400.0
                if age_days > UNMATCHED_AGE_DAYS:
                    alerts.append(
                        {
                            "priority": "MEDIUM",
                            "type": "stale_unmatched",
                            "run_id": run_id,
                            "result_id": result.id,
                            "record_type": result.record_type,
                            "record_id": result.record_id,
                            "message": (
                                f"Unmatched {result.record_type} {result.record_id} "
                                f"is {int(age_days)} days old"
                            ),
                            "amount_usd": result.expected_amount or 0.0,
                        }
                    )

        # -----------------------------------------------------------------
        # LOW: fee discrepancy
        # -----------------------------------------------------------------
        if result.discrepancy_type == "fee_discrepancy":
            expected = result.expected_amount or 0.0
            actual = result.actual_amount or 0.0
            diff = abs(expected - actual)
            alerts.append(
                {
                    "priority": "LOW",
                    "type": "fee_discrepancy",
                    "run_id": run_id,
                    "result_id": result.id,
                    "record_type": result.record_type,
                    "record_id": result.record_id,
                    "message": (
                        f"Fee discrepancy of ${diff:,.2f} USD on "
                        f"{result.record_type} {result.record_id}"
                    ),
                    "amount_usd": diff,
                }
            )

    alerts.sort(key=_priority_sort_key)
    logger.info("Generated %d alerts for run %d", len(alerts), run_id)
    return alerts
