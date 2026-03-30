from fastapi import APIRouter, HTTPException, Query
from typing import List, Optional
from datetime import datetime, timedelta
import logging

from app.models.database import db

logger = logging.getLogger(__name__)
router = APIRouter()

@router.get("/recent")
async def get_recent_trades(
    limit: int = Query(100, ge=1, le=500),
    hours: int = Query(24, ge=1, le=168)
):
    """Get recent trades across all tracked wallets"""
    try:
        cutoff_time = datetime.now() - timedelta(hours=hours)
        
        trades = await db.fetch("""
            SELECT t.*, w.signal_score, w.timing_edge
            FROM trades_log t
            LEFT JOIN wallets_master w ON t.wallet = w.wallet
            WHERE t.entry_time >= $1
            ORDER BY t.entry_time DESC
            LIMIT $2
        """, cutoff_time, limit)
        
        return {
            "trades": [dict(t) for t in trades],
            "count": len(trades),
            "timeframe_hours": hours
        }
        
    except Exception as e:
        logger.error(f"Error fetching recent trades: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch recent trades")

@router.get("/wallet/{wallet}")
async def get_wallet_trades(
    wallet: str,
    limit: int = Query(100, ge=1, le=500),
    market: Optional[str] = None
):
    """Get trades for a specific wallet"""
    try:
        if market:
            trades = await db.fetch("""
                SELECT * FROM trades_log 
                WHERE wallet = $1 AND market = $2
                ORDER BY entry_time DESC 
                LIMIT $3
            """, wallet, market, limit)
        else:
            trades = await db.fetch("""
                SELECT * FROM trades_log 
                WHERE wallet = $1
                ORDER BY entry_time DESC 
                LIMIT $2
            """, wallet, limit)
        
        return {
            "wallet": wallet,
            "trades": [dict(t) for t in trades],
            "count": len(trades),
            "market_filter": market
        }
        
    except Exception as e:
        logger.error(f"Error fetching trades for wallet {wallet}: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch wallet trades")

@router.get("/market/{market}")
async def get_market_trades(
    market: str,
    limit: int = Query(100, ge=1, le=500),
    min_signal_score: float = Query(0.0, ge=0, le=1)
):
    """Get trades for a specific market, optionally filtered by signal score"""
    try:
        trades = await db.fetch("""
            SELECT t.*, w.signal_score, w.win_rate
            FROM trades_log t
            LEFT JOIN wallets_master w ON t.wallet = w.wallet
            WHERE t.market = $1 
            AND (w.signal_score IS NULL OR w.signal_score >= $2)
            ORDER BY t.entry_time DESC
            LIMIT $3
        """, market, min_signal_score, limit)
        
        return {
            "market": market,
            "trades": [dict(t) for t in trades],
            "count": len(trades),
            "min_signal_score": min_signal_score
        }
        
    except Exception as e:
        logger.error(f"Error fetching trades for market {market}: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch market trades")

@router.post("/log")
async def log_trade(
    wallet: str,
    market: str,
    direction: str,
    entry_price: float,
    position_size: float,
    exit_price: Optional[float] = None,
    pnl: Optional[float] = None,
    entry_time: Optional[datetime] = None,
    exit_time: Optional[datetime] = None
):
    """Log a new trade"""
    valid_directions = ["long", "short", "buy", "sell"]
    if direction not in valid_directions:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid direction. Must be one of: {valid_directions}"
        )
    
    try:
        trade_id = await db.fetchval("""
            INSERT INTO trades_log 
            (wallet, market, direction, entry_price, position_size, 
             exit_price, pnl, entry_time, exit_time)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            RETURNING id
        """, 
            wallet, market, direction, entry_price, position_size,
            exit_price, pnl, 
            entry_time or datetime.now(),
            exit_time
        )
        
        return {
            "trade_id": str(trade_id),
            "status": "logged",
            "wallet": wallet,
            "market": market,
            "direction": direction
        }
        
    except Exception as e:
        logger.error(f"Error logging trade: {e}")
        raise HTTPException(status_code=500, detail="Failed to log trade")

@router.patch("/{trade_id}/close")
async def close_trade(
    trade_id: str,
    exit_price: float,
    exit_time: Optional[datetime] = None
):
    """Close an existing trade and calculate PnL"""
    try:
        # Get trade details
        trade = await db.fetchrow("""
            SELECT * FROM trades_log WHERE id = $1
        """, trade_id)
        
        if not trade:
            raise HTTPException(status_code=404, detail="Trade not found")
        
        if trade['exit_price'] is not None:
            raise HTTPException(status_code=400, detail="Trade already closed")
        
        # Calculate PnL based on direction
        entry_price = float(trade['entry_price'])
        position_size = float(trade['position_size'])
        
        if trade['direction'].lower() in ['long', 'buy']:
            pnl = (exit_price - entry_price) * position_size
        else:  # short/sell
            pnl = (entry_price - exit_price) * position_size
        
        # Update trade
        await db.execute("""
            UPDATE trades_log 
            SET exit_price = $1, pnl = $2, exit_time = $3
            WHERE id = $4
        """, exit_price, pnl, exit_time or datetime.now(), trade_id)
        
        return {
            "trade_id": trade_id,
            "status": "closed",
            "exit_price": exit_price,
            "pnl": round(pnl, 6)
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error closing trade {trade_id}: {e}")
        raise HTTPException(status_code=500, detail="Failed to close trade")

@router.get("/stats")
async def get_trade_stats():
    """Get trade statistics and summary"""
    try:
        # Total trades and PnL
        overall_stats = await db.fetchrow("""
            SELECT 
                COUNT(*) as total_trades,
                COUNT(CASE WHEN pnl > 0 THEN 1 END) as winning_trades,
                SUM(pnl) as total_pnl,
                AVG(pnl) as avg_pnl,
                COUNT(DISTINCT wallet) as unique_wallets,
                COUNT(DISTINCT market) as unique_markets
            FROM trades_log
            WHERE pnl IS NOT NULL
        """)
        
        # Top markets by volume
        market_stats = await db.fetch("""
            SELECT 
                market,
                COUNT(*) as trade_count,
                SUM(position_size * entry_price) as total_volume,
                SUM(pnl) as total_pnl
            FROM trades_log
            WHERE pnl IS NOT NULL
            GROUP BY market
            ORDER BY total_volume DESC
            LIMIT 10
        """)
        
        # Recent activity (last 24h)
        recent_activity = await db.fetchrow("""
            SELECT 
                COUNT(*) as trades_24h,
                SUM(pnl) as pnl_24h,
                COUNT(DISTINCT wallet) as active_wallets_24h
            FROM trades_log
            WHERE entry_time >= NOW() - INTERVAL '24 hours'
            AND pnl IS NOT NULL
        """)
        
        return {
            "overall": dict(overall_stats) if overall_stats else {},
            "top_markets": [dict(m) for m in market_stats],
            "recent_activity": dict(recent_activity) if recent_activity else {},
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error fetching trade stats: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch trade statistics")
