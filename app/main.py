from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import logging

# Import database connection
from app.models.database import db

# Import all routers
from app.api import (
    wallets,
    alerts,
    trades,
    pnl,
    export,
    backtest,
    settings,
    markets
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    logger.info("Starting up Polymarket Bot API...")
    await db.connect()
    logger.info("Database connection established")
    
    yield
    
    # Shutdown
    logger.info("Shutting down Polymarket Bot API...")
    await db.disconnect()
    logger.info("Database connection closed")

# Create FastAPI application
app = FastAPI(
    title="Polymarket Copy Trading Bot",
    description="API for wallet analysis and copy trading on Polymarket",
    version="1.0.0",
    lifespan=lifespan
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Register all API routers
app.include_router(wallets.router, prefix="/api/wallets", tags=["wallets"])
app.include_router(alerts.router, prefix="/api/alerts", tags=["alerts"])
app.include_router(trades.router, prefix="/api/trades", tags=["trades"])
app.include_router(pnl.router, prefix="/api/pnl", tags=["pnl"])
app.include_router(export.router, prefix="/api/export", tags=["export"])
app.include_router(backtest.router, prefix="/api/backtest", tags=["backtest"])
app.include_router(settings.router, prefix="/api/settings", tags=["settings"])
app.include_router(markets.router, prefix="/api/markets", tags=["markets"])

@app.get("/")
async def root():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "message": "Polymarket Copy Trading Bot API",
        "version": "1.0.0"
    }

@app.get("/health")
async def health_check():
    """Detailed health check"""
    try:
        # Test database connection
        await db.fetchval("SELECT 1")
        
        return {
            "status": "healthy",
            "database": "connected",
            "timestamp": "2024-01-01T00:00:00Z"
        }
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        raise HTTPException(status_code=503, detail="Service unavailable")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )
