from fastapi import APIRouter, HTTPException, Query
from typing import List, Optional
from datetime import datetime, timedelta
from decimal import Decimal
import logging

from app.models.database import db

logger = logging.getLogger(__name__)
router = APIRouter()

@router.get("/summary")
async def get_pnl_summary(
    days: int = Query(30, ge=1, le=365),
    min_signal_score: float = Query(0.0, ge=0, le=1)
):
    """Get PnL summary for specified period"""
    try:
        cutoff_date = datetime.now() - timedelta(days=days)
        
        summary = await db.fetchrow("""
            SELECT 
                COUNT(*) as total_trades,
                SUM(CASE WHEN t.pnl > 0 THEN 1 ELSE 0 END) as winning_trades,
                SUM(t.pnl) as total_pnl,
                AVG(t.pnl) as avg_pnl,
                MAX(t.pnl) as max_win,
                MIN(t.pnl) as max_loss,
                SUM(t.position_size * t.entry_price) as total_volume,
                COUNT(DISTINCT t.wallet) as active_wallets
            FROM trades_log t
            LEFT JOIN wallets_master w ON t.wallet = w.wallet
            WHERE t.entry_time >= $1 
            AND t.pnl IS NOT NULL
            AND (w.signal_score IS NULL OR w.signal_score >= $2)
        """, cutoff_date, min_signal_score)
        
        if not summary or summary['total_trades'] == 0:
            return {
                "summary": {
                    "total_trades": 0,
                    "total_pnl": 0,
                    "win_rate": 0,
                    "avg_pnl": 0,
                    "roi": 0
                },
                "period_days": days,
                "min_signal_score": min_signal_score
            }
        
        win_rate = float(summary['winning_trades']) / float(summary['total_trades'])
        roi = float(summary['total_pnl']) / float(summary['total_volume']) if summary['total_volume'] > 0 else 0
        
        return {
            "summary": {
                "total_trades": summary['total_trades'],
                "winning_trades": summary['winning_trades'],
                "total_pnl": round(float(summary['total_pnl'] or 0), 6),
                "avg_pnl": round(float(summary['avg_pnl'] or 0), 6),
                "max_win": round(float(summary['max_win'] or 0), 6),
                "max_loss": round(float(summary['max_loss'] or 0), 6),
                "win_rate": round(win_rate, 3),
                "roi": round(roi, 4),
                "total_volume": round(float(summary['total_volume'] or 0), 6),
                "active_wallets": summary['active_wallets']
            },
            "period_days": days,
            "min_signal_score": min_signal_score
        }
        
    except Exception as e:
        logger.error(f"Error fetching PnL summary: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch PnL summary")

@router.get("/by-wallet")
async def get_pnl_by_wallet(
    limit: int = Query(50, ge=1, le=200),
    days: int = Query(30, ge=1, le=365)
):
    """Get PnL breakdown by wallet"""
    try:
        cutoff_date = datetime.now() - timedelta(days=days)
        
        wallet_pnl = await db.fetch("""
            SELECT 
                t.wallet,
                w.signal_score,
                COUNT(*) as trade_count,
                SUM(t.pnl) as total_pnl,
                AVG(t.pnl) as avg_pnl,
                SUM(CASE WHEN t.pnl > 0 THEN 1 ELSE 0 END) as wins,
                SUM(t.position_size * t.entry_price) as total_volume
            FROM trades_log t
            LEFT JOIN wallets_master w ON t.wallet = w.wallet
            WHERE t.entry_time >= $1 AND t.pnl IS NOT NULL
            GROUP BY t.wallet, w.signal_score
            HAVING COUNT(*) >= 5
            ORDER BY total_pnl DESC
            LIMIT $2
        """, cutoff_date, limit)
        
        result = []
        for row in wallet_pnl:
            win_rate = float(row['wins']) / float(row['trade_count']) if row['trade_count'] > 0 else 0
            roi = float(row['total_pnl'] or 0) / float(row['total_volume'] or 1) if row['total_volume'] > 0 else 0
            
            result.append({
                "wallet": row['wallet'],
                "signal_score": float(row['signal_score'] or 0),
                "trade_count": row['trade_count'],
                "total_pnl": round(float(row['total_pnl'] or 0), 6),
                "avg_pnl": round(float(row['avg_pnl'] or 0), 6),
                "win_rate": round(win_rate, 3),
                "roi": round(roi, 4),
                "total_volume": round(float(row['total_volume'] or 0), 6)
            })
        
        return {
            "wallet_pnl": result,
            "count": len(result),
            "period_days": days,
            "limit": limit
        }
        
    except Exception as e:
        logger.error(f"Error fetching PnL by wallet: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch wallet PnL breakdown")

@router.get("/by-market")
async def get_pnl_by_market(
    limit: int = Query(20, ge=1, le=100),
    days: int = Query(30, ge=1, le=365)
):
    """Get PnL breakdown by market"""
    try:
        cutoff_date = datetime.now() - timedelta(days=days)
        
        market_pnl = await db.fetch("""
            SELECT 
                market,
                COUNT(*) as trade_count,
                SUM(pnl) as total_pnl,
                AVG(pnl) as avg_pnl,
                SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END) as wins,
                SUM(position_size * entry_price) as total_volume,
                COUNT(DISTINCT wallet) as unique_traders
            FROM trades_log
            WHERE entry_time >= $1 AND pnl IS NOT NULL
            GROUP BY market
            HAVING COUNT(*) >= 3
            ORDER BY total_pnl DESC
            LIMIT $2
        """, cutoff_date, limit)
        
        result = []
        for row in market_pnl:
            win_rate = float(row['wins']) / float(row['trade_count']) if row['trade_count'] > 0 else 0
            roi = float(row['total_pnl'] or 0) / float(row['total_volume'] or 1) if row['total_volume'] > 0 else 0
            
            result.append({
                "market": row['market'],
                "trade_count": row['trade_count'],
                "total_pnl": round(float(row['total_pnl'] or 0), 6),
                "avg_pnl": round(float(row['avg_pnl'] or 0), 6),
                "win_rate": round(win_rate, 3),
                "roi": round(roi, 4),
                "total_volume": round(float(row['total_volume'] or 0), 6),
                "unique_traders": row['unique_traders']
            })
        
        return {
            "market_pnl": result,
            "count": len(result),
            "period_days": days,
            "limit": limit
        }
        
    except Exception as e:
        logger.error(f"Error fetching PnL by market: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch market PnL breakdown")

@router.get("/daily")
async def get_daily_pnl(
    days: int = Query(30, ge=1, le=365),
    min_signal_score: float = Query(0.0, ge=0, le=1)
):
    """Get daily PnL aggregation"""
    try:
        cutoff_date = datetime.now() - timedelta(days=days)
        
        daily_pnl = await db.fetch("""
            SELECT 
                DATE(t.entry_time) as trade_date,
                COUNT(*) as trade_count,
                SUM(t.pnl) as daily_pnl,
                SUM(CASE WHEN t.pnl > 0 THEN 1 ELSE 0 END) as wins,
                COUNT(DISTINCT t.wallet) as active_wallets
            FROM trades_log t
            LEFT JOIN wallets_master w ON t.wallet = w.wallet
            WHERE t.entry_time >= $1 
            AND t.pnl IS NOT NULL
            AND (w.signal_score IS NULL OR w.signal_score >= $2)
            GROUP BY DATE(t.entry_time)
            ORDER BY trade_date DESC
        """, cutoff_date, min_signal_score)
        
        result = []
        for row in daily_pnl:
            win_rate = float(row['wins']) / float(row['trade_count']) if row['trade_count'] > 0 else 0
            
            result.append({
                "date": row['trade_date'].isoformat(),
                "trade_count": row['trade_count'],
                "daily_pnl": round(float(row['daily_pnl'] or 0), 6),
                "win_rate": round(win_rate, 3),
                "active_wallets": row['active_wallets']
            })
        
        return {
            "daily_pnl": result,
            "period_days": days,
            "min_signal_score": min_signal_score
        }
        
    except Exception as e:
        logger.error(f"Error fetching daily PnL: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch daily PnL")

@router.get("/leaderboard")
async def get_pnl_leaderboard(
    period: str = Query("30d", regex="^(7d|30d|90d|all)$"),
    metric: str = Query("total_pnl", regex="^(total_pnl|roi|win_rate|avg_pnl)$"),
    limit: int = Query(20, ge=1, le=100)
):
    """Get PnL leaderboard by different metrics"""
    try:
        # Determine date filter
        if period == "7d":
            cutoff_date = datetime.now() - timedelta(days=7)
        elif period == "30d":
            cutoff_date = datetime.now() - timedelta(days=30)
        elif period == "90d":
            cutoff_date = datetime.now() - timedelta(days=90)
        else:  # all
            cutoff_date = datetime.min
        
        # Base query
        base_query = """
            SELECT 
                t.wallet,
                w.signal_score,
                COUNT(*) as trade_count,
                SUM(t.pnl) as total_pnl,
                AVG(t.pnl) as avg_pnl,
                SUM(CASE WHEN t.pnl > 0 THEN 1 ELSE 0 END) as wins,
                SUM(t.position_size * t.entry_price) as total_volume
            FROM trades_log t
            LEFT JOIN wallets_master w ON t.wallet = w.wallet
            WHERE t.entry_time >= $1 AND t.pnl IS NOT NULL
            GROUP BY t.wallet, w.signal_score
            HAVING COUNT(*) >= 5
        """
        
        # Order by chosen metric
        if metric == "total_pnl":
            order_clause = "ORDER BY total_pnl DESC"
        elif metric == "roi":
            order_clause = "ORDER BY (SUM(t.pnl) / SUM(t.position_size * t.entry_price)) DESC"
        elif metric == "win_rate":
            order_clause = "ORDER BY (SUM(CASE WHEN t.pnl > 0 THEN 1 ELSE 0 END)::float / COUNT(*)) DESC"
        else:  # avg_pnl
            order_clause = "ORDER BY avg_pnl DESC"
        
        query = f"{base_query} {order_clause} LIMIT $2"
        
        leaderboard = await db.fetch(query, cutoff_date, limit)
        
        result = []
        for i, row in enumerate(leaderboard):
            win_rate = float(row['wins']) / float(row['trade_count']) if row['trade_count'] > 0 else 0
            roi = float(row['total_pnl'] or 0) / float(row['total_volume'] or 1) if row['total_volume'] > 0 else 0
            
            result.append({
                "rank": i + 1,
                "wallet": row['wallet'],
                "signal_score": float(row['signal_score'] or 0),
                "trade_count": row['trade_count'],
                "total_pnl": round(float(row['total_pnl'] or 0), 6),
                "avg_pnl": round(float(row['avg_pnl'] or 0), 6),
                "win_rate": round(win_rate, 3),
                "roi": round(roi, 4)
            })
        
        return {
            "leaderboard": result,
            "period": period,
            "metric": metric,
            "limit": limit,
            "count": len(result)
        }
        
    except Exception as e:
        logger.error(f"Error fetching PnL leaderboard: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch PnL leaderboard")
