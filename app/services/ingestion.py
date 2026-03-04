from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import Payout, SettlementRecord, Transaction
from app.schemas import PayoutCreate, SettlementRecordCreate, TransactionCreate


async def ingest_transactions(db: AsyncSession, items: list[TransactionCreate]) -> int:
    """Bulk-insert transactions, skipping duplicates by transaction_id."""
    if not items:
        return 0

    existing_ids_result = await db.execute(
        select(Transaction.transaction_id).where(
            Transaction.transaction_id.in_([i.transaction_id for i in items])
        )
    )
    existing_ids = set(existing_ids_result.scalars().all())

    new_records = []
    for item in items:
        if item.transaction_id not in existing_ids:
            new_records.append(Transaction(**item.model_dump()))
            existing_ids.add(item.transaction_id)

    if new_records:
        db.add_all(new_records)
        await db.commit()

    return len(new_records)


async def ingest_payouts(db: AsyncSession, items: list[PayoutCreate]) -> int:
    """Bulk-insert payouts, skipping duplicates by payout_id."""
    if not items:
        return 0

    existing_ids_result = await db.execute(
        select(Payout.payout_id).where(
            Payout.payout_id.in_([i.payout_id for i in items])
        )
    )
    existing_ids = set(existing_ids_result.scalars().all())

    new_records = []
    for item in items:
        if item.payout_id not in existing_ids:
            new_records.append(Payout(**item.model_dump()))
            existing_ids.add(item.payout_id)

    if new_records:
        db.add_all(new_records)
        await db.commit()

    return len(new_records)


async def ingest_settlements(db: AsyncSession, items: list[SettlementRecordCreate]) -> int:
    """Bulk-insert settlement records, skipping duplicates by settlement_id."""
    if not items:
        return 0

    existing_ids_result = await db.execute(
        select(SettlementRecord.settlement_id).where(
            SettlementRecord.settlement_id.in_([i.settlement_id for i in items])
        )
    )
    existing_ids = set(existing_ids_result.scalars().all())

    new_records = []
    for item in items:
        if item.settlement_id not in existing_ids:
            new_records.append(SettlementRecord(**item.model_dump()))
            existing_ids.add(item.settlement_id)

    if new_records:
        db.add_all(new_records)
        await db.commit()

    return len(new_records)
