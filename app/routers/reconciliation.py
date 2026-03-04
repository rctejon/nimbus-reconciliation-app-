from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.models import ReconciliationResult, ReconciliationRun
from app.schemas import ReconciliationResultResponse, ReconciliationRunResponse
from app.services.matching import run_reconciliation

router = APIRouter(prefix="/api/v1/reconciliation", tags=["reconciliation"])


@router.post("/run")
async def trigger_reconciliation(db: AsyncSession = Depends(get_db)):
    run = await run_reconciliation(db)
    return {
        "status": "success",
        "data": ReconciliationRunResponse.model_validate(run),
    }


@router.get("/runs")
async def list_runs(
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    db: AsyncSession = Depends(get_db),
):
    total_result = await db.execute(select(func.count(ReconciliationRun.id)))
    total = total_result.scalar_one()

    offset = (page - 1) * page_size
    result = await db.execute(
        select(ReconciliationRun)
        .order_by(ReconciliationRun.run_timestamp.desc())
        .offset(offset)
        .limit(page_size)
    )
    runs = result.scalars().all()

    return {
        "status": "success",
        "data": [ReconciliationRunResponse.model_validate(r) for r in runs],
        "meta": {"total": total, "page": page, "page_size": page_size},
    }


@router.get("/runs/{run_id}")
async def get_run(run_id: int, db: AsyncSession = Depends(get_db)):
    result = await db.execute(
        select(ReconciliationRun).where(ReconciliationRun.id == run_id)
    )
    run = result.scalar_one_or_none()
    if not run:
        raise HTTPException(status_code=404, detail="Run not found")
    return {
        "status": "success",
        "data": ReconciliationRunResponse.model_validate(run),
    }


@router.get("/runs/{run_id}/compare/{other_run_id}")
async def compare_runs(
    run_id: int, other_run_id: int, db: AsyncSession = Depends(get_db)
):
    run_a_result = await db.execute(
        select(ReconciliationRun).where(ReconciliationRun.id == run_id)
    )
    run_a = run_a_result.scalar_one_or_none()
    if not run_a:
        raise HTTPException(status_code=404, detail=f"Run {run_id} not found")

    run_b_result = await db.execute(
        select(ReconciliationRun).where(ReconciliationRun.id == other_run_id)
    )
    run_b = run_b_result.scalar_one_or_none()
    if not run_b:
        raise HTTPException(status_code=404, detail=f"Run {other_run_id} not found")

    results_a = await db.execute(
        select(ReconciliationResult).where(
            ReconciliationResult.reconciliation_run_id == run_id
        )
    )
    results_b = await db.execute(
        select(ReconciliationResult).where(
            ReconciliationResult.reconciliation_run_id == other_run_id
        )
    )

    set_a = {r.record_id: r for r in results_a.scalars().all()}
    set_b = {r.record_id: r for r in results_b.scalars().all()}

    newly_matched = []
    newly_unmatched = []
    resolved = []
    new_discrepancies = []

    all_ids = set(set_a.keys()) | set(set_b.keys())
    for rid in all_ids:
        a = set_a.get(rid)
        b = set_b.get(rid)

        if a and b:
            if a.status != "matched" and b.status == "matched":
                newly_matched.append(rid)
            elif a.status == "matched" and b.status != "matched":
                newly_unmatched.append(rid)
            if a.discrepancy_type and not b.discrepancy_type:
                resolved.append(rid)
            elif not a.discrepancy_type and b.discrepancy_type:
                new_discrepancies.append(rid)
        elif a and not b:
            if a.status == "matched":
                newly_unmatched.append(rid)
        elif b and not a:
            if b.status == "matched":
                newly_matched.append(rid)
            if b.discrepancy_type:
                new_discrepancies.append(rid)

    return {
        "status": "success",
        "data": {
            "run_a": ReconciliationRunResponse.model_validate(run_a),
            "run_b": ReconciliationRunResponse.model_validate(run_b),
            "newly_matched": newly_matched,
            "newly_unmatched": newly_unmatched,
            "resolved": resolved,
            "new_discrepancies": new_discrepancies,
        },
    }
