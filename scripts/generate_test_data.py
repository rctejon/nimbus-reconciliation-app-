"""
Generate test data for the Settlement Reconciliation Engine.

Produces:
  - 150+ transactions
  - 80+ payouts
  - 120+ settlement records

Deliberate discrepancies:
  - 10-15 unmatched transactions (no corresponding settlement)
  - 5-8 amount mismatches (2-10% delta)
  - 2-3 duplicate settlement references
  - 3-5 orphaned settlements (reference non-existent transactions)
  - ~40% of settlements have a batch_id
"""

import json
import random
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

random.seed(42)

OUTPUT_DIR = Path(__file__).resolve().parent / "test_data"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

CURRENCIES = ["USD", "MXN", "BRL", "COP"]
PROCESSORS = ["stripe", "adyen", "dlocal", "mercadopago", "payu"]
RECIPIENTS = [
    "merchant_alpha", "merchant_beta", "merchant_gamma",
    "merchant_delta", "merchant_epsilon", "merchant_zeta",
    "merchant_eta", "merchant_theta", "merchant_iota", "merchant_kappa",
]
STATUSES = ["completed", "completed", "completed", "pending", "failed"]
BATCH_IDS = [f"BATCH-{i:04d}" for i in range(1, 11)]

BASE_TIME = datetime(2026, 1, 15, 10, 0, 0, tzinfo=timezone.utc)


def rand_amount(currency: str) -> float:
    """Generate a realistic random amount for a given currency."""
    ranges = {
        "USD": (5.0, 5000.0),
        "MXN": (100.0, 95000.0),
        "BRL": (25.0, 24000.0),
        "COP": (20000.0, 20000000.0),
    }
    lo, hi = ranges.get(currency, (10.0, 10000.0))
    return round(random.uniform(lo, hi), 2)


def rand_fee(amount: float) -> float:
    """Generate a realistic processing fee (1-4% of amount)."""
    return round(amount * random.uniform(0.01, 0.04), 2)


def random_ts(base: datetime, max_offset_hours: int = 720) -> str:
    """Random ISO timestamp within max_offset_hours of base."""
    offset = timedelta(hours=random.randint(0, max_offset_hours), minutes=random.randint(0, 59))
    return (base + offset).isoformat()


# ── Generate Transactions ────────────────────────────────────────────────────

transactions = []
for i in range(160):
    currency = random.choice(CURRENCIES)
    transactions.append({
        "transaction_id": f"TXN-{i:05d}",
        "amount": rand_amount(currency),
        "currency": currency,
        "processor": random.choice(PROCESSORS),
        "capture_timestamp": random_ts(BASE_TIME),
        "status": random.choice(STATUSES),
    })

# ── Generate Payouts ─────────────────────────────────────────────────────────

payouts = []
for i in range(85):
    currency = random.choice(CURRENCIES)
    payouts.append({
        "payout_id": f"PAY-{i:05d}",
        "amount": rand_amount(currency),
        "currency": currency,
        "recipient": random.choice(RECIPIENTS),
        "payout_timestamp": random_ts(BASE_TIME),
        "status": random.choice(["completed", "completed", "pending"]),
    })

# ── Generate Settlement Records ──────────────────────────────────────────────

settlements = []
settlement_idx = 0

# 1) Matched settlements — reference existing transactions with correct amounts
matched_txn_indices = list(range(0, 135))
random.shuffle(matched_txn_indices)

# Correctly matched (no discrepancy)
for txn_i in matched_txn_indices[:110]:
    txn = transactions[txn_i]
    fee = rand_fee(txn["amount"])
    assign_batch = random.random() < 0.40
    settlements.append({
        "settlement_id": f"STL-{settlement_idx:05d}",
        "transaction_reference": txn["transaction_id"],
        "settled_amount": round(txn["amount"] - fee, 2),
        "settlement_currency": txn["currency"],
        "settlement_date": random_ts(
            datetime.fromisoformat(txn["capture_timestamp"]) + timedelta(hours=24),
            max_offset_hours=48,
        ),
        "fees": fee,
        "batch_id": random.choice(BATCH_IDS) if assign_batch else None,
    })
    settlement_idx += 1

# 2) Amount mismatches — 7 settlements with 2-10% delta from transaction amount
mismatch_txn_indices = matched_txn_indices[110:117]
for txn_i in mismatch_txn_indices:
    txn = transactions[txn_i]
    fee = rand_fee(txn["amount"])
    mismatch_factor = 1.0 + random.uniform(0.02, 0.10) * random.choice([-1, 1])
    settled = round((txn["amount"] - fee) * mismatch_factor, 2)
    assign_batch = random.random() < 0.40
    settlements.append({
        "settlement_id": f"STL-{settlement_idx:05d}",
        "transaction_reference": txn["transaction_id"],
        "settled_amount": settled,
        "settlement_currency": txn["currency"],
        "settlement_date": random_ts(
            datetime.fromisoformat(txn["capture_timestamp"]) + timedelta(hours=24),
            max_offset_hours=48,
        ),
        "fees": fee,
        "batch_id": random.choice(BATCH_IDS) if assign_batch else None,
    })
    settlement_idx += 1

# 3) Duplicate settlement references — 3 duplicates referencing already-settled txns
for dup_i in range(3):
    src = settlements[dup_i * 10]
    settlements.append({
        "settlement_id": f"STL-{settlement_idx:05d}",
        "transaction_reference": src["transaction_reference"],
        "settled_amount": src["settled_amount"],
        "settlement_currency": src["settlement_currency"],
        "settlement_date": random_ts(BASE_TIME + timedelta(days=10), max_offset_hours=48),
        "fees": src["fees"],
        "batch_id": src["batch_id"],
    })
    settlement_idx += 1

# 4) Orphaned settlements — 5 settlements referencing non-existent transactions
for orphan_i in range(5):
    currency = random.choice(CURRENCIES)
    assign_batch = random.random() < 0.40
    settlements.append({
        "settlement_id": f"STL-{settlement_idx:05d}",
        "transaction_reference": f"TXN-GHOST-{orphan_i:03d}",
        "settled_amount": rand_amount(currency),
        "settlement_currency": currency,
        "settlement_date": random_ts(BASE_TIME, max_offset_hours=720),
        "fees": round(random.uniform(1.0, 50.0), 2),
        "batch_id": random.choice(BATCH_IDS) if assign_batch else None,
    })
    settlement_idx += 1

# Transactions 135-159 are unmatched (15 transactions with no settlement)
# They are already in the transactions list but have no corresponding settlement.

# ── Write Output ─────────────────────────────────────────────────────────────

with open(OUTPUT_DIR / "transactions.json", "w") as f:
    json.dump(transactions, f, indent=2, default=str)

with open(OUTPUT_DIR / "payouts.json", "w") as f:
    json.dump(payouts, f, indent=2, default=str)

with open(OUTPUT_DIR / "settlements.json", "w") as f:
    json.dump(settlements, f, indent=2, default=str)

print(f"Generated {len(transactions)} transactions")
print(f"Generated {len(payouts)} payouts")
print(f"Generated {len(settlements)} settlements")
print(f"  - ~110 correctly matched")
print(f"  - 7 amount mismatches (2-10%)")
print(f"  - 3 duplicate references")
print(f"  - 5 orphaned settlements")
print(f"  - {len(transactions) - 135} unmatched transactions (no settlement)")
print(f"  - ~{sum(1 for s in settlements if s['batch_id'])} with batch_id")
print(f"Output: {OUTPUT_DIR}")
