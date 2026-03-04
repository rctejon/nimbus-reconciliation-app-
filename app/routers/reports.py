from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.models import (
    Payout,
    ReconciliationResult,
    ReconciliationRun,
    SettlementRecord,
    Transaction,
)
from app.schemas import (
    AlertResponse,
    BatchSummaryResponse,
    ReconciliationResultResponse,
    SummaryResponse,
)
from app.services.alerts import generate_alerts
from app.services.currency import to_usd

router = APIRouter(prefix="/api/v1/reports", tags=["reports"])


async def _get_latest_run(db: AsyncSession):
    result = await db.execute(
        select(ReconciliationRun)
        .order_by(ReconciliationRun.run_timestamp.desc())
        .limit(1)
    )
    return result.scalar_one_or_none()


@router.get("/summary")
async def summary(
    run_id: Optional[int] = None,
    db: AsyncSession = Depends(get_db),
):
    if run_id:
        result = await db.execute(
            select(ReconciliationRun).where(ReconciliationRun.id == run_id)
        )
        run = result.scalar_one_or_none()
        if not run:
            raise HTTPException(status_code=404, detail="Run not found")
    else:
        run = await _get_latest_run(db)

    total_txn = (await db.execute(select(func.count(Transaction.id)))).scalar_one()
    total_payouts = (await db.execute(select(func.count(Payout.id)))).scalar_one()
    total_settlements = (
        await db.execute(select(func.count(SettlementRecord.id)))
    ).scalar_one()

    if run:
        matched = run.matched_count
        unmatched = run.unmatched_count
        discrepancies = run.discrepant_count
        total_discrepancy = run.total_discrepancy_amount
        total = run.total_transactions
    else:
        matched = 0
        unmatched = 0
        discrepancies = 0
        total_discrepancy = 0.0
        total = total_txn + total_payouts

    match_rate = (matched / total * 100) if total > 0 else 0.0

    # Currency breakdown of discrepancies
    currency_breakdown = None
    if run:
        breakdown_result = await db.execute(
            select(
                ReconciliationResult.currency,
                func.sum(func.abs(ReconciliationResult.delta)),
            )
            .where(
                ReconciliationResult.reconciliation_run_id == run.id,
                ReconciliationResult.discrepancy_type.is_not(None),
            )
            .group_by(ReconciliationResult.currency)
        )
        rows = breakdown_result.all()
        if rows:
            currency_breakdown = {row[0]: round(to_usd(row[1], row[0]), 2) for row in rows}

    return {
        "status": "success",
        "data": SummaryResponse(
            total_transactions=total,
            matched=matched,
            unmatched=unmatched,
            discrepancies=discrepancies,
            total_discrepancy_amount=total_discrepancy,
            match_rate=round(match_rate, 2),
            currency_breakdown=currency_breakdown,
        ),
        "meta": {
            "total_payouts": total_payouts,
            "total_settlements": total_settlements,
        },
    }


@router.get("/discrepancies")
async def list_discrepancies(
    run_id: Optional[int] = None,
    date_from: Optional[datetime] = None,
    date_to: Optional[datetime] = None,
    currency: Optional[str] = None,
    processor: Optional[str] = None,
    status: Optional[str] = None,
    discrepancy_type: Optional[str] = None,
    min_amount: Optional[float] = None,
    max_amount: Optional[float] = None,
    sort_by: str = Query("delta", pattern="^(delta|expected_amount|actual_amount|created_at)$"),
    sort_order: str = Query("desc", pattern="^(asc|desc)$"),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    db: AsyncSession = Depends(get_db),
):
    if not run_id:
        latest = await _get_latest_run(db)
        if not latest:
            return {
                "status": "success",
                "data": [],
                "meta": {"total": 0, "page": page, "page_size": page_size},
            }
        run_id = latest.id

    query = select(ReconciliationResult).where(
        ReconciliationResult.reconciliation_run_id == run_id,
        ReconciliationResult.discrepancy_type.is_not(None),
    )
    count_query = select(func.count(ReconciliationResult.id)).where(
        ReconciliationResult.reconciliation_run_id == run_id,
        ReconciliationResult.discrepancy_type.is_not(None),
    )

    if currency:
        query = query.where(ReconciliationResult.currency == currency)
        count_query = count_query.where(ReconciliationResult.currency == currency)
    if status:
        query = query.where(ReconciliationResult.status == status)
        count_query = count_query.where(ReconciliationResult.status == status)
    if discrepancy_type:
        query = query.where(ReconciliationResult.discrepancy_type == discrepancy_type)
        count_query = count_query.where(
            ReconciliationResult.discrepancy_type == discrepancy_type
        )
    if min_amount is not None:
        query = query.where(func.abs(ReconciliationResult.delta) >= min_amount)
        count_query = count_query.where(
            func.abs(ReconciliationResult.delta) >= min_amount
        )
    if max_amount is not None:
        query = query.where(func.abs(ReconciliationResult.delta) <= max_amount)
        count_query = count_query.where(
            func.abs(ReconciliationResult.delta) <= max_amount
        )
    if date_from:
        query = query.where(ReconciliationResult.created_at >= date_from)
        count_query = count_query.where(ReconciliationResult.created_at >= date_from)
    if date_to:
        query = query.where(ReconciliationResult.created_at <= date_to)
        count_query = count_query.where(ReconciliationResult.created_at <= date_to)

    # Join for processor filter
    if processor:
        query = query.join(
            Transaction,
            ReconciliationResult.record_id == Transaction.transaction_id,
            isouter=True,
        ).where(Transaction.processor == processor)
        count_query = count_query.join(
            Transaction,
            ReconciliationResult.record_id == Transaction.transaction_id,
            isouter=True,
        ).where(Transaction.processor == processor)

    # Sorting
    sort_col = getattr(ReconciliationResult, sort_by, ReconciliationResult.delta)
    if sort_order == "desc":
        query = query.order_by(sort_col.desc())
    else:
        query = query.order_by(sort_col.asc())

    total = (await db.execute(count_query)).scalar_one()

    offset = (page - 1) * page_size
    result = await db.execute(query.offset(offset).limit(page_size))
    records = result.scalars().all()

    return {
        "status": "success",
        "data": [ReconciliationResultResponse.model_validate(r) for r in records],
        "meta": {"total": total, "page": page, "page_size": page_size},
    }


@router.get("/discrepancies/{record_id}")
async def get_discrepancy(record_id: str, db: AsyncSession = Depends(get_db)):
    result = await db.execute(
        select(ReconciliationResult).where(
            ReconciliationResult.record_id == record_id,
            ReconciliationResult.discrepancy_type.is_not(None),
        )
        .order_by(ReconciliationResult.created_at.desc())
        .limit(1)
    )
    record = result.scalar_one_or_none()
    if not record:
        raise HTTPException(status_code=404, detail="Discrepancy not found")

    # Fetch original transaction or payout
    original = None
    if record.record_type == "transaction":
        txn_result = await db.execute(
            select(Transaction).where(Transaction.transaction_id == record.record_id)
        )
        txn = txn_result.scalar_one_or_none()
        if txn:
            original = {
                "type": "transaction",
                "transaction_id": txn.transaction_id,
                "amount": txn.amount,
                "currency": txn.currency,
                "processor": txn.processor,
                "capture_timestamp": txn.capture_timestamp.isoformat(),
            }
    elif record.record_type == "payout":
        pay_result = await db.execute(
            select(Payout).where(Payout.payout_id == record.record_id)
        )
        pay = pay_result.scalar_one_or_none()
        if pay:
            original = {
                "type": "payout",
                "payout_id": pay.payout_id,
                "amount": pay.amount,
                "currency": pay.currency,
                "recipient": pay.recipient,
                "payout_timestamp": pay.payout_timestamp.isoformat(),
            }

    # Fetch settlement
    settlement = None
    if record.settlement_id:
        sett_result = await db.execute(
            select(SettlementRecord).where(
                SettlementRecord.settlement_id == record.settlement_id
            )
        )
        sett = sett_result.scalar_one_or_none()
        if sett:
            settlement = {
                "settlement_id": sett.settlement_id,
                "settled_amount": sett.settled_amount,
                "settlement_currency": sett.settlement_currency,
                "fees": sett.fees,
                "batch_id": sett.batch_id,
            }

    return {
        "status": "success",
        "data": {
            "discrepancy": ReconciliationResultResponse.model_validate(record),
            "original_record": original,
            "settlement": settlement,
        },
    }


@router.get("/alerts")
async def get_alerts(
    run_id: Optional[int] = None,
    priority: Optional[str] = None,
    db: AsyncSession = Depends(get_db),
):
    if not run_id:
        latest = await _get_latest_run(db)
        if not latest:
            return {"status": "success", "data": []}
        run_id = latest.id

    alerts = await generate_alerts(db, run_id)

    if priority:
        alerts = [a for a in alerts if a.get("priority") == priority]

    def _to_alert_response(a: dict) -> AlertResponse:
        details = {k: v for k, v in a.items() if k not in ("type", "priority", "message")}
        return AlertResponse(
            alert_type=a["type"],
            severity=a["priority"],
            message=a["message"],
            details=details or None,
        )

    return {
        "status": "success",
        "data": [_to_alert_response(a) for a in alerts],
    }


@router.get("/batches")
async def batch_summary(
    run_id: Optional[int] = None,
    db: AsyncSession = Depends(get_db),
):
    if not run_id:
        latest = await _get_latest_run(db)
        if not latest:
            return {"status": "success", "data": []}
        run_id = latest.id

    # Get all settlement records grouped by batch_id
    settlements_result = await db.execute(
        select(SettlementRecord).where(SettlementRecord.batch_id.is_not(None))
    )
    settlements = settlements_result.scalars().all()

    batches = {}
    for s in settlements:
        bid = s.batch_id
        if bid not in batches:
            batches[bid] = {
                "batch_id": bid,
                "total_records": 0,
                "total_settled": 0.0,
                "settlement_ids": [],
            }
        batches[bid]["total_records"] += 1
        batches[bid]["total_settled"] += s.settled_amount
        batches[bid]["settlement_ids"].append(s.settlement_id)

    # Cross-reference with reconciliation results for this run
    results_data = await db.execute(
        select(ReconciliationResult).where(
            ReconciliationResult.reconciliation_run_id == run_id
        )
    )
    results_by_settlement = {}
    for r in results_data.scalars().all():
        if r.settlement_id:
            results_by_settlement[r.settlement_id] = r

    batch_summaries = []
    for bid, batch_info in batches.items():
        matched = 0
        unmatched = 0
        discrepancies = 0
        total_expected = 0.0

        for sid in batch_info["settlement_ids"]:
            rec = results_by_settlement.get(sid)
            if rec:
                total_expected += rec.expected_amount
                if rec.status == "matched":
                    matched += 1
                elif rec.discrepancy_type:
                    discrepancies += 1
                else:
                    unmatched += 1
            else:
                unmatched += 1

        batch_summaries.append(
            BatchSummaryResponse(
                batch_id=bid,
                total_records=batch_info["total_records"],
                matched=matched,
                unmatched=unmatched,
                discrepancies=discrepancies,
                total_settled=round(batch_info["total_settled"], 2),
                total_expected=round(total_expected, 2),
                net_difference=round(batch_info["total_settled"] - total_expected, 2),
            )
        )

    return {"status": "success", "data": batch_summaries}
