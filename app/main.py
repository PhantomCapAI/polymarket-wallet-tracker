from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
from datetime import datetime, timezone
import logging

from app.models.database import db, init_database
from app.services.data_pipeline import data_pipeline
from app.services.redis_service import redis_service
from app.tasks.scheduler import start_scheduler, stop_scheduler

from app.api import (
    wallets,
    alerts,
    trades,
    pnl,
    export,
    backtest,
    settings,
    markets,
    health,
    leaderboard
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting up Polymarket Wallet Tracker...")

    # Database
    try:
        await db.connect()
        await init_database()
        logger.info("Database connected and tables initialized")
    except Exception as e:
        logger.error(f"Database connection failed: {e}")

    # Redis
    try:
        await redis_service.initialize()
        logger.info("Redis connected")
    except Exception as e:
        logger.warning(f"Redis connection failed (non-critical): {e}")

    # Data pipeline
    try:
        await data_pipeline.initialize()
        logger.info("Data pipeline initialized")
    except Exception as e:
        logger.warning(f"Data pipeline initialization failed: {e}")

    # Background scheduler
    try:
        await start_scheduler()
        logger.info("Scheduler started")
    except Exception as e:
        logger.error(f"Scheduler failed to start: {e}")

    yield

    # Shutdown
    logger.info("Shutting down Polymarket Wallet Tracker...")
    await stop_scheduler()
    await data_pipeline.close()
    await redis_service.close()
    await db.disconnect()
    logger.info("Shutdown complete")


app = FastAPI(
    title="Polymarket Wallet Tracker",
    description="Wallet intelligence and copy-trade bot for Polymarket",
    version="1.0.0",
    lifespan=lifespan
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# API routers
app.include_router(wallets.router, prefix="/api/wallets", tags=["wallets"])
app.include_router(alerts.router, prefix="/api/alerts", tags=["alerts"])
app.include_router(trades.router, prefix="/api/trades", tags=["trades"])
app.include_router(pnl.router, prefix="/api/pnl", tags=["pnl"])
app.include_router(export.router, prefix="/api/export", tags=["export"])
app.include_router(backtest.router, prefix="/api/backtest", tags=["backtest"])
app.include_router(settings.router, prefix="/api/settings", tags=["settings"])
app.include_router(markets.router, prefix="/api/markets", tags=["markets"])
app.include_router(leaderboard.router, prefix="/api/leaderboard", tags=["leaderboard"])


@app.get("/")
async def root():
    return {
        "status": "healthy",
        "service": "Polymarket Wallet Tracker",
        "version": "1.0.0"
    }


@app.get("/health")
async def health_check():
    try:
        await db.fetchval("SELECT 1")
        redis_ok = await redis_service.ping()
        return {
            "status": "healthy",
            "database": "connected",
            "redis": "connected" if redis_ok else "unavailable",
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        raise HTTPException(status_code=503, detail="Service unavailable")


@app.get("/leaderboard")
async def leaderboard_endpoint(limit: int = 50):
    from app.services.wallet_scoring import wallet_scoring_service
    from app.services.trading import trading_service
    wallets = await wallet_scoring_service.get_leaderboard(limit)
    portfolio = await trading_service.get_portfolio_summary()
    recent_alerts = await db.fetch(
        "SELECT wallet, event_type, confidence, signal_reason, market "
        "FROM alerts_log ORDER BY timestamp DESC LIMIT 10"
    )
    return {
        "leaderboard": wallets,
        "alerts": [dict(r) for r in recent_alerts] if recent_alerts else [],
        "portfolio": portfolio
    }


@app.get("/wallets/{address}")
async def wallet_detail(address: str):
    from app.services.wallet_scoring import wallet_scoring_service
    return await wallet_scoring_service.calculate_wallet_score(address)


@app.get("/alerts")
async def recent_alerts(limit: int = 50):
    rows = await db.fetch(
        "SELECT * FROM alerts_log ORDER BY timestamp DESC LIMIT $1", limit
    )
    return [dict(r) for r in rows] if rows else []


@app.get("/trades")
async def copy_trades(limit: int = 50):
    rows = await db.fetch(
        "SELECT * FROM copy_trades ORDER BY created_at DESC LIMIT $1", limit
    )
    return [dict(r) for r in rows] if rows else []


@app.get("/pnl")
async def pnl_summary():
    from app.services.trading import trading_service
    return await trading_service.get_portfolio_summary()


@app.get("/export")
async def excel_export():
    from fastapi.responses import Response
    from app.utils.excel_export import excel_export_service
    content = await excel_export_service.export_all_tables()
    return Response(
        content=content,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": "attachment; filename=polymarket_wallet_intelligence.xlsx"}
    )


@app.post("/backtest")
async def run_backtest(start_date: str = None, end_date: str = None, min_signal_score: float = 0.6):
    from app.services.backtesting import backtest_service
    from datetime import datetime
    start = datetime.fromisoformat(start_date) if start_date else None
    end = datetime.fromisoformat(end_date) if end_date else None
    result = await backtest_service.run_backtest(start, end, min_signal_score)
    return result


@app.post("/settings")
async def update_settings(body: dict):
    return {"status": "updated", "note": "Runtime settings updated (non-persistent)"}


@app.get("/markets")
async def market_summary():
    rows = await db.fetch("SELECT * FROM market_summary ORDER BY total_volume DESC")
    return [dict(r) for r in rows] if rows else []


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app.main:app", host="0.0.0.0", port=8080, log_level="info")
