from fastapi import APIRouter, Query
from typing import List, Dict, Any, Optional
from datetime import datetime

from app.models.database import db

router = APIRouter()

@router.get("/")
async def get_leaderboard(
    limit: int = Query(50, ge=1, le=100),
    min_signal_score: Optional[float] = Query(None, ge=0, le=1)
) -> Dict[str, Any]:
    """Get wallet leaderboard ranked by signal score"""
    try:
        query = """
            SELECT wallet, signal_score, realized_pnl, win_rate, 
                   avg_position_size, market_diversity, timing_edge,
                   closing_efficiency, consistency_score, total_trades,
                   active_days, last_trade_at, last_updated
            FROM wallets_master
            WHERE 1=1
        """
        params = []
        
        if min_signal_score is not None:
            query += " AND signal_score >= $" + str(len(params) + 1)
            params.append(min_signal_score)
        
        query += " ORDER BY signal_score DESC LIMIT $" + str(len(params) + 1)
        params.append(limit)
        
        wallets = await db.fetch(query, *params)
        
        return {
            "wallets": [dict(wallet) for wallet in wallets],
            "total_count": len(wallets),
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        return {
            "error": str(e),
            "wallets": [],
            "total_count": 0
        }

@router.get("/top/{count}")
async def get_top_wallets(count: int) -> List[Dict[str, Any]]:
    """Get top N wallets by signal score"""
    try:
        wallets = await db.fetch("""
            SELECT wallet, signal_score, realized_pnl, win_rate
            FROM wallets_master
            ORDER BY signal_score DESC
            LIMIT $1
        """, count)
        
        return [dict(wallet) for wallet in wallets]
        
    except Exception as e:
        return []
