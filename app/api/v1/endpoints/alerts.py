from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, desc
from typing import List, Optional

from app.database.connection import get_db
from app.models.alert import AlertsLog
from app.schemas.alerts import AlertResponse

router = APIRouter()

@router.get("/alerts", response_model=List[AlertResponse])
async def get_alerts(
    limit: int = 50,
    event_type: Optional[str] = None,
    db: AsyncSession = Depends(get_db)
):
    """Get recent alerts"""
    try:
        query = select(AlertsLog).order_by(desc(AlertsLog.timestamp))
        
        if event_type:
            query = query.where(AlertsLog.event_type == event_type)
        
        query = query.limit(limit)
        result = await db.execute(query)
        alerts = result.scalars().all()
        
        return [
            AlertResponse(
                id=alert.id,
                timestamp=alert.timestamp,
                wallet=alert.wallet,
                event_type=alert.event_type,
                confidence=alert.confidence,
                signal_reason=alert.signal_reason,
                market=alert.market,
                processed=alert.processed
            )
            for alert in alerts
        ]
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
