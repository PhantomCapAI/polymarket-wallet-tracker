from fastapi import APIRouter, Depends
from typing import Dict, Any
import asyncio
from datetime import datetime

from app.models.database import db

router = APIRouter()

@router.get("/")
async def health_check() -> Dict[str, Any]:
    """Basic health check endpoint"""
    try:
        # Test database connection
        await db.fetchval("SELECT 1")
        db_status = "healthy"
    except Exception as e:
        db_status = f"unhealthy: {str(e)}"
    
    return {
        "status": "healthy" if db_status == "healthy" else "unhealthy",
        "timestamp": datetime.now().isoformat(),
        "database": db_status,
        "version": "1.0.0"
    }

@router.get("/detailed")
async def detailed_health() -> Dict[str, Any]:
    """Detailed health check with service statuses"""
    try:
        # Test database connection and get stats
        wallet_count = await db.fetchval("SELECT COUNT(*) FROM wallets_master")
        trade_count = await db.fetchval("SELECT COUNT(*) FROM copy_trades WHERE status = 'open'")
        
        return {
            "status": "healthy",
            "timestamp": datetime.now().isoformat(),
            "database": "healthy",
            "services": {
                "wallet_scoring": "running",
                "trading": "running", 
                "alerting": "running"
            },
            "stats": {
                "total_wallets": int(wallet_count or 0),
                "open_trades": int(trade_count or 0)
            }
        }
    except Exception as e:
        return {
            "status": "unhealthy",
            "timestamp": datetime.now().isoformat(),
            "error": str(e)
        }
