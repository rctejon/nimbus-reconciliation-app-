import json
from pathlib import Path

from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.schemas import (
    IngestionData,
    IngestionResponse,
    PayoutCreate,
    SettlementRecordCreate,
    TransactionCreate,
)
from app.services.ingestion import ingest_payouts, ingest_settlements, ingest_transactions

router = APIRouter(prefix="/api/v1/ingest", tags=["ingestion"])

TEST_DATA_DIR = Path(__file__).resolve().parent.parent.parent / "scripts" / "test_data"


@router.post("/transactions", response_model=IngestionResponse)
async def ingest_transactions_endpoint(
    items: list[TransactionCreate],
    db: AsyncSession = Depends(get_db),
):
    count = await ingest_transactions(db, items)
    return IngestionResponse(status="success", data=IngestionData(count=count))


@router.post("/payouts", response_model=IngestionResponse)
async def ingest_payouts_endpoint(
    items: list[PayoutCreate],
    db: AsyncSession = Depends(get_db),
):
    count = await ingest_payouts(db, items)
    return IngestionResponse(status="success", data=IngestionData(count=count))


@router.post("/settlements", response_model=IngestionResponse)
async def ingest_settlements_endpoint(
    items: list[SettlementRecordCreate],
    db: AsyncSession = Depends(get_db),
):
    count = await ingest_settlements(db, items)
    return IngestionResponse(status="success", data=IngestionData(count=count))


@router.post("/seed", response_model=IngestionResponse)
async def seed_data(db: AsyncSession = Depends(get_db)):
    """Load test data from scripts/test_data/ JSON files."""
    total = 0

    txn_file = TEST_DATA_DIR / "transactions.json"
    if txn_file.exists():
        with open(txn_file) as f:
            raw = json.load(f)
        items = [TransactionCreate(**r) for r in raw]
        total += await ingest_transactions(db, items)

    payout_file = TEST_DATA_DIR / "payouts.json"
    if payout_file.exists():
        with open(payout_file) as f:
            raw = json.load(f)
        items = [PayoutCreate(**r) for r in raw]
        total += await ingest_payouts(db, items)

    settlement_file = TEST_DATA_DIR / "settlements.json"
    if settlement_file.exists():
        with open(settlement_file) as f:
            raw = json.load(f)
        items = [SettlementRecordCreate(**r) for r in raw]
        total += await ingest_settlements(db, items)

    return IngestionResponse(status="success", data=IngestionData(count=total))
