import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from app.database import Base, engine
from app.main import app

SAMPLE_TRANSACTIONS = [
    {
        "transaction_id": "TXN-001",
        "amount": 100.0,
        "currency": "USD",
        "processor": "stripe",
        "capture_timestamp": "2025-01-15T10:00:00Z",
        "status": "completed",
    },
    {
        "transaction_id": "TXN-002",
        "amount": 250.50,
        "currency": "USD",
        "processor": "stripe",
        "capture_timestamp": "2025-01-15T11:00:00Z",
        "status": "completed",
    },
    {
        "transaction_id": "TXN-003",
        "amount": 5000.0,
        "currency": "MXN",
        "processor": "conekta",
        "capture_timestamp": "2025-01-15T12:00:00Z",
        "status": "completed",
    },
]

SAMPLE_PAYOUTS = [
    {
        "payout_id": "PAY-001",
        "amount": 95.0,
        "currency": "USD",
        "recipient": "merchant-a",
        "payout_timestamp": "2025-01-16T10:00:00Z",
        "status": "completed",
    },
]

SAMPLE_SETTLEMENTS = [
    {
        "settlement_id": "STL-001",
        "transaction_reference": "TXN-001",
        "settled_amount": 98.0,
        "settlement_currency": "USD",
        "settlement_date": "2025-01-16T00:00:00Z",
        "fees": 2.0,
        "batch_id": "BATCH-A",
    },
    {
        "settlement_id": "STL-002",
        "transaction_reference": "TXN-002",
        "settled_amount": 250.50,
        "settlement_currency": "USD",
        "settlement_date": "2025-01-16T00:00:00Z",
        "fees": 0.0,
        "batch_id": "BATCH-A",
    },
]


@pytest_asyncio.fixture(autouse=True)
async def setup_db():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)


@pytest_asyncio.fixture
async def client():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac


@pytest.mark.asyncio
async def test_health(client: AsyncClient):
    resp = await client.get("/health")
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "healthy"
    assert data["service"] == "nimbus-reconciliation-engine"


@pytest.mark.asyncio
async def test_ingest_transactions(client: AsyncClient):
    resp = await client.post("/api/v1/ingest/transactions", json=SAMPLE_TRANSACTIONS)
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "success"
    assert body["data"]["count"] == 3


@pytest.mark.asyncio
async def test_ingest_payouts(client: AsyncClient):
    resp = await client.post("/api/v1/ingest/payouts", json=SAMPLE_PAYOUTS)
    assert resp.status_code == 200
    assert resp.json()["data"]["count"] == 1


@pytest.mark.asyncio
async def test_ingest_settlements(client: AsyncClient):
    resp = await client.post("/api/v1/ingest/settlements", json=SAMPLE_SETTLEMENTS)
    assert resp.status_code == 200
    assert resp.json()["data"]["count"] == 2


@pytest.mark.asyncio
async def test_reconciliation_run(client: AsyncClient):
    # Ingest data first
    await client.post("/api/v1/ingest/transactions", json=SAMPLE_TRANSACTIONS)
    await client.post("/api/v1/ingest/payouts", json=SAMPLE_PAYOUTS)
    await client.post("/api/v1/ingest/settlements", json=SAMPLE_SETTLEMENTS)

    # Trigger reconciliation
    resp = await client.post("/api/v1/reconciliation/run")
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "success"
    assert "id" in body["data"]
    assert body["data"]["status"] == "completed"


@pytest.mark.asyncio
async def test_list_runs(client: AsyncClient):
    await client.post("/api/v1/ingest/transactions", json=SAMPLE_TRANSACTIONS)
    await client.post("/api/v1/ingest/settlements", json=SAMPLE_SETTLEMENTS)
    await client.post("/api/v1/reconciliation/run")

    resp = await client.get("/api/v1/reconciliation/runs")
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "success"
    assert len(body["data"]) >= 1
    assert "meta" in body
    assert body["meta"]["total"] >= 1


@pytest.mark.asyncio
async def test_get_run_not_found(client: AsyncClient):
    resp = await client.get("/api/v1/reconciliation/runs/9999")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_summary(client: AsyncClient):
    await client.post("/api/v1/ingest/transactions", json=SAMPLE_TRANSACTIONS)
    await client.post("/api/v1/ingest/settlements", json=SAMPLE_SETTLEMENTS)
    await client.post("/api/v1/reconciliation/run")

    resp = await client.get("/api/v1/reports/summary")
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "success"
    assert "matched" in body["data"]
    assert "unmatched" in body["data"]
    assert "match_rate" in body["data"]


@pytest.mark.asyncio
async def test_discrepancies(client: AsyncClient):
    await client.post("/api/v1/ingest/transactions", json=SAMPLE_TRANSACTIONS)
    await client.post("/api/v1/ingest/settlements", json=SAMPLE_SETTLEMENTS)
    await client.post("/api/v1/reconciliation/run")

    resp = await client.get("/api/v1/reports/discrepancies")
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "success"
    assert isinstance(body["data"], list)
    assert "meta" in body


@pytest.mark.asyncio
async def test_discrepancies_with_filters(client: AsyncClient):
    await client.post("/api/v1/ingest/transactions", json=SAMPLE_TRANSACTIONS)
    await client.post("/api/v1/ingest/settlements", json=SAMPLE_SETTLEMENTS)
    await client.post("/api/v1/reconciliation/run")

    resp = await client.get(
        "/api/v1/reports/discrepancies",
        params={"currency": "USD", "sort_by": "delta", "sort_order": "asc"},
    )
    assert resp.status_code == 200
    assert resp.json()["status"] == "success"


@pytest.mark.asyncio
async def test_alerts(client: AsyncClient):
    await client.post("/api/v1/ingest/transactions", json=SAMPLE_TRANSACTIONS)
    await client.post("/api/v1/ingest/settlements", json=SAMPLE_SETTLEMENTS)
    await client.post("/api/v1/reconciliation/run")

    resp = await client.get("/api/v1/reports/alerts")
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "success"
    assert isinstance(body["data"], list)


@pytest.mark.asyncio
async def test_batches(client: AsyncClient):
    await client.post("/api/v1/ingest/transactions", json=SAMPLE_TRANSACTIONS)
    await client.post("/api/v1/ingest/settlements", json=SAMPLE_SETTLEMENTS)
    await client.post("/api/v1/reconciliation/run")

    resp = await client.get("/api/v1/reports/batches")
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "success"
    assert isinstance(body["data"], list)


@pytest.mark.asyncio
async def test_compare_runs(client: AsyncClient):
    await client.post("/api/v1/ingest/transactions", json=SAMPLE_TRANSACTIONS)
    await client.post("/api/v1/ingest/settlements", json=SAMPLE_SETTLEMENTS)

    # Two runs
    r1 = await client.post("/api/v1/reconciliation/run")
    r2 = await client.post("/api/v1/reconciliation/run")
    run_id_1 = r1.json()["data"]["id"]
    run_id_2 = r2.json()["data"]["id"]

    resp = await client.get(f"/api/v1/reconciliation/runs/{run_id_1}/compare/{run_id_2}")
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "success"
    assert "newly_matched" in body["data"]
    assert "newly_unmatched" in body["data"]
    assert "resolved" in body["data"]
    assert "new_discrepancies" in body["data"]


@pytest.mark.asyncio
async def test_duplicate_ingestion_skips(client: AsyncClient):
    resp1 = await client.post("/api/v1/ingest/transactions", json=SAMPLE_TRANSACTIONS)
    assert resp1.json()["data"]["count"] == 3

    resp2 = await client.post("/api/v1/ingest/transactions", json=SAMPLE_TRANSACTIONS)
    assert resp2.json()["data"]["count"] == 0
