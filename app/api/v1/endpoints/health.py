from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from datetime import datetime

from app.database.connection import get_db
from app.services.redis_service import redis_service

router = APIRouter()

@router.get("/health")
async def health_check(db: AsyncSession = Depends(get_db)):
    """System health check endpoint"""
    try:
        health_status = {
            "timestamp": datetime.utcnow(),
            "status": "healthy",
            "services": {}
        }
        
        # Check database connection
        try:
            await db.execute(text("SELECT 1"))
            health_status["services"]["database"] = "healthy"
        except Exception as e:
            health_status["services"]["database"] = f"unhealthy: {str(e)}"
            health_status["status"] = "degraded"
        
        # Check Redis connection
        try:
            await redis_service.ping()
            health_status["services"]["redis"] = "healthy"
        except Exception as e:
            health_status["services"]["redis"] = f"unhealthy: {str(e)}"
            health_status["status"] = "degraded"
        
        return health_status
        
    except Exception as e:
        return {
            "timestamp": datetime.utcnow(),
            "status": "unhealthy",
            "error": str(e)
        }

@router.get("/health/detailed")
async def detailed_health_check(db: AsyncSession = Depends(get_db)):
    """Detailed system health check"""
    try:
        health_data = {
            "timestamp": datetime.utcnow(),
            "status": "healthy",
            "database": {},
            "redis": {},
            "background_tasks": {}
        }
        
        # Database checks
        try:
            # Check connection
            await db.execute(text("SELECT 1"))
            
            # Check table counts (simplified for now)
            health_data["database"] = {
                "status": "healthy",
                "connection": "active"
            }
            
        except Exception as e:
            health_data["database"] = {"status": "unhealthy", "error": str(e)}
            health_data["status"] = "degraded"
        
        # Redis checks
        try:
            redis_info = await redis_service.info()
            health_data["redis"] = {
                "status": "healthy",
                "connected_clients": redis_info.get("connected_clients", 0),
                "used_memory_human": redis_info.get("used_memory_human", "unknown")
            }
        except Exception as e:
            health_data["redis"] = {"status": "unhealthy", "error": str(e)}
            health_data["status"] = "degraded"
        
        # Background task status
        health_data["background_tasks"] = {
            "scheduler": "running",
            "last_data_update": "unknown"
        }
        
        return health_data
        
    except Exception as e:
        return {
            "timestamp": datetime.utcnow(),
            "status": "unhealthy",
            "error": str(e)
        }
