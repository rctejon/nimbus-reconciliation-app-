from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.database import init_db
from app.routers.ingestion import router as ingestion_router
from app.routers.reconciliation import router as reconciliation_router
from app.routers.reports import router as reports_router


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    yield


app = FastAPI(
    title="Nimbus Settlement Reconciliation Engine",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(ingestion_router)
app.include_router(reconciliation_router)
app.include_router(reports_router)


@app.get("/health")
async def health():
    return {"status": "healthy", "service": "nimbus-reconciliation-engine"}
