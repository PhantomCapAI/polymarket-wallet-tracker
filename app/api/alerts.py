from fastapi import APIRouter, HTTPException, Query
from typing import List, Optional
from datetime import datetime, timedelta
import logging

from app.models.database import db

logger = logging.getLogger(__name__)
router = APIRouter()

@router.get("/recent")
async def get_recent_alerts(
    limit: int = Query(50, ge=1, le=200),
    hours: int = Query(24, ge=1, le=168)
):
    """Get recent alerts within specified timeframe"""
    try:
        cutoff_time = datetime.now() - timedelta(hours=hours)
        
        alerts = await db.fetch("""
            SELECT a.*, w.signal_score, w.win_rate
            FROM alerts_log a
            LEFT JOIN wallets_master w ON a.wallet = w.wallet
            WHERE a.timestamp >= $1
            ORDER BY a.timestamp DESC
            LIMIT $2
        """, cutoff_time, limit)
        
        return {
            "alerts": [dict(a) for a in alerts],
            "count": len(alerts),
            "timeframe_hours": hours
        }
        
    except Exception as e:
        logger.error(f"Error fetching recent alerts: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch alerts")

@router.get("/by-confidence/{confidence}")
async def get_alerts_by_confidence(
    confidence: str,
    limit: int = Query(100, ge=1, le=500)
):
    """Get alerts filtered by confidence level"""
    valid_confidence = ["high", "medium", "low"]
    if confidence not in valid_confidence:
        raise HTTPException(
            status_code=400, 
            detail=f"Invalid confidence level. Must be one of: {valid_confidence}"
        )
    
    try:
        alerts = await db.fetch("""
            SELECT a.*, w.signal_score, w.timing_edge
            FROM alerts_log a
            LEFT JOIN wallets_master w ON a.wallet = w.wallet
            WHERE a.confidence = $1
            ORDER BY a.timestamp DESC
            LIMIT $2
        """, confidence, limit)
        
        return {
            "alerts": [dict(a) for a in alerts],
            "count": len(alerts),
            "confidence_filter": confidence
        }
        
    except Exception as e:
        logger.error(f"Error fetching alerts by confidence: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch alerts")

@router.post("/create")
async def create_alert(
    wallet: str,
    event_type: str,
    confidence: str,
    signal_reason: str,
    market: Optional[str] = None
):
    """Create a new alert"""
    valid_events = ["trade_opened", "trade_closed", "large_position", "unusual_activity"]
    valid_confidence = ["high", "medium", "low"]
    
    if event_type not in valid_events:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid event type. Must be one of: {valid_events}"
        )
    
    if confidence not in valid_confidence:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid confidence level. Must be one of: {valid_confidence}"
        )
    
    try:
        alert_id = await db.fetchval("""
            INSERT INTO alerts_log (wallet, event_type, confidence, signal_reason, market, timestamp)
            VALUES ($1, $2, $3, $4, $5, NOW())
            RETURNING id
        """, wallet, event_type, confidence, signal_reason, market)
        
        return {
            "alert_id": str(alert_id),
            "status": "created",
            "wallet": wallet,
            "event_type": event_type,
            "confidence": confidence
        }
        
    except Exception as e:
        logger.error(f"Error creating alert: {e}")
        raise HTTPException(status_code=500, detail="Failed to create alert")

@router.get("/stats")
async def get_alert_stats():
    """Get alert statistics and summary"""
    try:
        # Total alerts by confidence
        confidence_stats = await db.fetch("""
            SELECT confidence, COUNT(*) as count
            FROM alerts_log
            WHERE timestamp >= NOW() - INTERVAL '24 hours'
            GROUP BY confidence
            ORDER BY count DESC
        """)
        
        # Total alerts by event type
        event_stats = await db.fetch("""
            SELECT event_type, COUNT(*) as count
            FROM alerts_log
            WHERE timestamp >= NOW() - INTERVAL '24 hours'
            GROUP BY event_type
            ORDER BY count DESC
        """)
        
        # Top alert generators
        top_wallets = await db.fetch("""
            SELECT a.wallet, COUNT(*) as alert_count, w.signal_score
            FROM alerts_log a
            LEFT JOIN wallets_master w ON a.wallet = w.wallet
            WHERE a.timestamp >= NOW() - INTERVAL '24 hours'
            GROUP BY a.wallet, w.signal_score
            ORDER BY alert_count DESC
            LIMIT 10
        """)
        
        return {
            "confidence_breakdown": [dict(c) for c in confidence_stats],
            "event_type_breakdown": [dict(e) for e in event_stats],
            "top_alert_wallets": [dict(w) for w in top_wallets],
            "period": "24 hours"
        }
        
    except Exception as e:
        logger.error(f"Error fetching alert stats: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch alert statistics")

@router.patch("/{alert_id}/process")
async def mark_alert_processed(alert_id: str):
    """Mark an alert as processed"""
    try:
        result = await db.execute("""
            UPDATE alerts_log 
            SET processed = TRUE 
            WHERE id = $1
        """, alert_id)
        
        if result == "UPDATE 0":
            raise HTTPException(status_code=404, detail="Alert not found")
        
        return {
            "alert_id": alert_id,
            "status": "processed"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error marking alert as processed: {e}")
        raise HTTPException(status_code=500, detail="Failed to update alert")
