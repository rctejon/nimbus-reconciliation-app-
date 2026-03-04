# Nimbus Settlement Reconciliation Engine

Automated settlement reconciliation for Nimbus Logistics -- matches transactions and payouts against settlement records using exact ID and fuzzy amount/date matching, with multi-currency support, duplicate detection, and discrepancy alerting.

## Live Demo

The API is deployed and available at:

- **Base URL:** https://nimbus-reconciliation-app.onrender.com
- **Swagger UI:** https://nimbus-reconciliation-app.onrender.com/docs
- **ReDoc:** https://nimbus-reconciliation-app.onrender.com/redoc

> Note: The free tier spins down after ~15 min of inactivity. The first request may take ~30s to wake up.

---

## Prerequisites

- **Python 3.11+**
- **pip**

## Quick Start

```bash
git clone https://github.com/rctejon/nimbus-reconciliation-app-.git
cd nimbus-reconciliation-app-
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Generate test data (transactions, payouts, settlements)
python scripts/generate_test_data.py

# Start the server
uvicorn app.main:app --reload
```

The API is available at `http://localhost:8000`. Interactive docs at `http://localhost:8000/docs`.

### Seed Test Data

Load the generated JSON fixtures into the database:

```bash
curl -X POST http://localhost:8000/api/v1/ingest/seed
```

### Run Reconciliation

Execute the matching engine against all ingested records:

```bash
curl -X POST http://localhost:8000/api/v1/reconciliation/run
```

---

## API Reference

All endpoints return JSON. Successful responses include `"status": "success"` with a `"data"` field.

### Health Check

#### `GET /health`

Returns service health status.

```bash
curl http://localhost:8000/health
```

```json
{
  "status": "healthy",
  "service": "nimbus-reconciliation-engine"
}
```

---

### Ingestion

#### `POST /api/v1/ingest/transactions`

Bulk-insert transactions. Duplicates (by `transaction_id`) are silently skipped.

```bash
curl -X POST http://localhost:8000/api/v1/ingest/transactions \
  -H "Content-Type: application/json" \
  -d '[
    {
      "transaction_id": "TXN-00001",
      "amount": 150.00,
      "currency": "USD",
      "processor": "stripe",
      "capture_timestamp": "2026-01-20T14:30:00Z",
      "status": "completed"
    }
  ]'
```

```json
{
  "status": "success",
  "data": { "count": 1 }
}
```

#### `POST /api/v1/ingest/payouts`

Bulk-insert payouts. Duplicates (by `payout_id`) are silently skipped.

```bash
curl -X POST http://localhost:8000/api/v1/ingest/payouts \
  -H "Content-Type: application/json" \
  -d '[
    {
      "payout_id": "PAY-00001",
      "amount": 500.00,
      "currency": "MXN",
      "recipient": "merchant_alpha",
      "payout_timestamp": "2026-01-21T09:00:00Z",
      "status": "completed"
    }
  ]'
```

```json
{
  "status": "success",
  "data": { "count": 1 }
}
```

#### `POST /api/v1/ingest/settlements`

Bulk-insert settlement records. Duplicates (by `settlement_id`) are silently skipped.

```bash
curl -X POST http://localhost:8000/api/v1/ingest/settlements \
  -H "Content-Type: application/json" \
  -d '[
    {
      "settlement_id": "STL-00001",
      "transaction_reference": "TXN-00001",
      "settled_amount": 145.50,
      "settlement_currency": "USD",
      "settlement_date": "2026-01-22T00:00:00Z",
      "fees": 4.50,
      "batch_id": "BATCH-0001"
    }
  ]'
```

```json
{
  "status": "success",
  "data": { "count": 1 }
}
```

#### `POST /api/v1/ingest/seed`

Load test data from `scripts/test_data/` JSON fixtures (transactions, payouts, settlements). Run `python scripts/generate_test_data.py` first.

```bash
curl -X POST http://localhost:8000/api/v1/ingest/seed
```

```json
{
  "status": "success",
  "data": { "count": 370 }
}
```

---

### Reconciliation

#### `POST /api/v1/reconciliation/run`

Execute a full reconciliation run: exact ID matching, fuzzy matching, duplicate detection, orphan detection, and batch reconciliation.

```bash
curl -X POST http://localhost:8000/api/v1/reconciliation/run
```

```json
{
  "status": "success",
  "data": {
    "id": 1,
    "run_timestamp": "2026-01-25T12:00:00Z",
    "total_transactions": 245,
    "matched_count": 198,
    "unmatched_count": 30,
    "discrepant_count": 17,
    "total_discrepancy_amount": 0.0,
    "status": "completed"
  }
}
```

#### `GET /api/v1/reconciliation/runs`

List all reconciliation runs with pagination, ordered by most recent first.

| Parameter   | Type | Default | Description             |
|-------------|------|---------|-------------------------|
| `page`      | int  | 1       | Page number (min: 1)    |
| `page_size` | int  | 20      | Results per page (1-100)|

```bash
curl "http://localhost:8000/api/v1/reconciliation/runs?page=1&page_size=10"
```

```json
{
  "status": "success",
  "data": [
    {
      "id": 1,
      "run_timestamp": "2026-01-25T12:00:00Z",
      "total_transactions": 245,
      "matched_count": 198,
      "unmatched_count": 30,
      "discrepant_count": 17,
      "total_discrepancy_amount": 0.0,
      "status": "completed"
    }
  ],
  "meta": { "total": 1, "page": 1, "page_size": 10 }
}
```

#### `GET /api/v1/reconciliation/runs/{run_id}`

Get details of a specific reconciliation run.

```bash
curl http://localhost:8000/api/v1/reconciliation/runs/1
```

```json
{
  "status": "success",
  "data": {
    "id": 1,
    "run_timestamp": "2026-01-25T12:00:00Z",
    "total_transactions": 245,
    "matched_count": 198,
    "unmatched_count": 30,
    "discrepant_count": 17,
    "total_discrepancy_amount": 0.0,
    "status": "completed"
  }
}
```

#### `GET /api/v1/reconciliation/runs/{run_id}/compare/{other_run_id}`

Compare two reconciliation runs. Returns records that changed status between runs (newly matched, newly unmatched, resolved discrepancies, new discrepancies).

```bash
curl http://localhost:8000/api/v1/reconciliation/runs/1/compare/2
```

```json
{
  "status": "success",
  "data": {
    "run_a": { "id": 1, "matched_count": 198, "status": "completed", "...": "..." },
    "run_b": { "id": 2, "matched_count": 205, "status": "completed", "...": "..." },
    "newly_matched": ["TXN-00135", "TXN-00140"],
    "newly_unmatched": [],
    "resolved": ["TXN-00050"],
    "new_discrepancies": []
  }
}
```

---

### Reports

#### `GET /api/v1/reports/summary`

High-level reconciliation summary with match rate and currency breakdown of discrepancies.

| Parameter | Type     | Default | Description                                |
|-----------|----------|---------|--------------------------------------------|
| `run_id`  | int/null | latest  | Reconciliation run ID (defaults to latest) |

```bash
curl http://localhost:8000/api/v1/reports/summary
```

```json
{
  "status": "success",
  "data": {
    "total_transactions": 245,
    "matched": 198,
    "unmatched": 30,
    "discrepancies": 17,
    "total_discrepancy_amount": 0.0,
    "match_rate": 80.82,
    "currency_breakdown": {
      "USD": 320.50,
      "MXN": 85.20,
      "BRL": 42.10
    }
  },
  "meta": {
    "total_payouts": 85,
    "total_settlements": 125
  }
}
```

#### `GET /api/v1/reports/discrepancies`

List discrepancy records with filtering, sorting, and pagination.

| Parameter          | Type     | Default | Description                                                                                  |
|--------------------|----------|---------|----------------------------------------------------------------------------------------------|
| `run_id`           | int/null | latest  | Reconciliation run ID                                                                        |
| `date_from`        | datetime | null    | Filter by created_at >= date                                                                 |
| `date_to`          | datetime | null    | Filter by created_at <= date                                                                 |
| `currency`         | str/null | null    | Filter by currency code (e.g. `USD`)                                                         |
| `processor`        | str/null | null    | Filter by payment processor                                                                  |
| `status`           | str/null | null    | Filter by result status                                                                      |
| `discrepancy_type` | str/null | null    | Filter: `amount_mismatch`, `fee_discrepancy`, `currency_slippage`, `duplicate`, `orphaned`   |
| `min_amount`       | float    | null    | Minimum absolute delta                                                                       |
| `max_amount`       | float    | null    | Maximum absolute delta                                                                       |
| `sort_by`          | str      | `delta` | Sort field: `delta`, `expected_amount`, `actual_amount`, `created_at`                        |
| `sort_order`       | str      | `desc`  | Sort direction: `asc` or `desc`                                                              |
| `page`             | int      | 1       | Page number                                                                                  |
| `page_size`        | int      | 20      | Results per page (1-100)                                                                     |

```bash
curl "http://localhost:8000/api/v1/reports/discrepancies?currency=USD&sort_by=delta&sort_order=desc&page=1&page_size=5"
```

```json
{
  "status": "success",
  "data": [
    {
      "id": 42,
      "record_type": "transaction",
      "record_id": "TXN-00112",
      "settlement_id": "STL-00098",
      "status": "discrepant",
      "expected_amount": 1250.00,
      "actual_amount": 1180.50,
      "currency": "USD",
      "delta": -69.50,
      "discrepancy_type": "amount_mismatch",
      "notes": null,
      "reconciliation_run_id": 1,
      "created_at": "2026-01-25T12:00:00Z"
    }
  ],
  "meta": { "total": 8, "page": 1, "page_size": 5 }
}
```

#### `GET /api/v1/reports/discrepancies/{record_id}`

Get detailed discrepancy information for a specific record, including the original transaction/payout and the matched settlement.

```bash
curl http://localhost:8000/api/v1/reports/discrepancies/TXN-00112
```

```json
{
  "status": "success",
  "data": {
    "discrepancy": {
      "id": 42,
      "record_type": "transaction",
      "record_id": "TXN-00112",
      "settlement_id": "STL-00098",
      "status": "discrepant",
      "expected_amount": 1250.00,
      "actual_amount": 1180.50,
      "currency": "USD",
      "delta": -69.50,
      "discrepancy_type": "amount_mismatch",
      "notes": null,
      "reconciliation_run_id": 1,
      "created_at": "2026-01-25T12:00:00Z"
    },
    "original_record": {
      "type": "transaction",
      "transaction_id": "TXN-00112",
      "amount": 1250.00,
      "currency": "USD",
      "processor": "stripe",
      "capture_timestamp": "2026-01-20T14:30:00Z"
    },
    "settlement": {
      "settlement_id": "STL-00098",
      "settled_amount": 1180.50,
      "settlement_currency": "USD",
      "fees": 4.50,
      "batch_id": "BATCH-0003"
    }
  }
}
```

#### `GET /api/v1/reports/alerts`

Get priority-sorted alerts generated from reconciliation results. Alerts are classified as HIGH (large discrepancy >$500 or currency slippage >5%), MEDIUM (unmatched records >14 days old), or LOW (fee discrepancies).

| Parameter  | Type     | Default | Description                              |
|------------|----------|---------|------------------------------------------|
| `run_id`   | int/null | latest  | Reconciliation run ID                    |
| `priority` | str/null | null    | Filter by severity: `HIGH`, `MEDIUM`, `LOW` |

```bash
curl "http://localhost:8000/api/v1/reports/alerts?priority=HIGH"
```

```json
{
  "status": "success",
  "data": [
    {
      "alert_type": "large_discrepancy",
      "severity": "HIGH",
      "message": "Discrepancy of $725.00 USD on transaction TXN-00045",
      "details": null
    },
    {
      "alert_type": "currency_slippage",
      "severity": "HIGH",
      "message": "Currency slippage of 8.2% ($180.00 USD) on transaction TXN-00089",
      "details": null
    }
  ]
}
```

#### `GET /api/v1/reports/batches`

Get reconciliation summary grouped by settlement batch. Shows matched/unmatched/discrepancy counts and net difference for each batch.

| Parameter | Type     | Default | Description           |
|-----------|----------|---------|-----------------------|
| `run_id`  | int/null | latest  | Reconciliation run ID |

```bash
curl http://localhost:8000/api/v1/reports/batches
```

```json
{
  "status": "success",
  "data": [
    {
      "batch_id": "BATCH-0001",
      "total_records": 12,
      "matched": 10,
      "unmatched": 1,
      "discrepancies": 1,
      "total_settled": 15420.50,
      "total_expected": 15890.00,
      "net_difference": -469.50
    }
  ]
}
```

---

## Architecture

```
nimbus-reconciliation-app-/
├── app/
│   ├── main.py                  # FastAPI app, lifespan, CORS middleware
│   ├── database.py              # Async SQLAlchemy engine (SQLite + aiosqlite)
│   ├── models.py                # ORM models (5 tables)
│   ├── schemas.py               # Pydantic v2 request/response schemas
│   ├── routers/
│   │   ├── ingestion.py         # Data ingestion endpoints (4 routes)
│   │   ├── reconciliation.py    # Reconciliation execution & run history (4 routes)
│   │   └── reports.py           # Reporting & alerting endpoints (5 routes)
│   ├── services/
│   │   ├── ingestion.py         # Bulk insert with deduplication
│   │   ├── matching.py          # Two-phase reconciliation engine
│   │   ├── alerts.py            # Priority-based alert generation
│   │   └── currency.py          # FX conversion via static rates
│   └── utils/
│       └── exchange_rates.py    # Static exchange rate table
├── scripts/
│   └── generate_test_data.py    # Generates ~370 test records with deliberate discrepancies
├── tests/
│   ├── test_api.py              # API integration tests
│   └── test_matching.py         # Matching engine unit tests
└── requirements.txt
```

The application follows a **routers -> services -> models** layered architecture. FastAPI routers handle HTTP concerns, services contain business logic, and SQLAlchemy models manage persistence. All database operations are async via `aiosqlite`.

### Database Tables

| Table                    | Purpose                                    |
|--------------------------|--------------------------------------------|
| `transactions`           | Ingested payment transactions              |
| `payouts`                | Ingested merchant payouts                  |
| `settlement_records`     | Settlement reports from processors         |
| `reconciliation_runs`    | Metadata for each reconciliation execution |
| `reconciliation_results` | Per-record matching outcomes               |

### Matching Engine

The reconciliation engine (`app/services/matching.py`) uses a multi-strategy approach:

1. **Exact ID match** -- `settlement.transaction_reference == transaction.transaction_id`
2. **Fuzzy match** -- Weighted scoring of amount proximity (60%) and date proximity (40%) within a 2% tolerance and 7-day window
3. **Duplicate detection** -- Settlements grouped by `transaction_reference`; extras flagged
4. **Orphan detection** -- Unmatched settlements marked as orphaned
5. **Batch reconciliation** -- Aggregate totals compared per `batch_id`

### Currency Conversion

All amounts are normalized to USD using static rates (USD=1.0, MXN=19.5, BRL=4.9, COP=4200.0). Cross-currency matches receive an additional 1% tolerance buffer.

---

## Running Tests

```bash
pytest -v
```
