from fastapi import APIRouter, HTTPException, Query
from typing import List, Optional
from decimal import Decimal
import logging

from app.models.database import db
from app.services.wallet_scoring import WalletScoringService

logger = logging.getLogger(__name__)
router = APIRouter()

@router.get("/top")
async def get_top_wallets(
    limit: int = Query(50, ge=1, le=200),
    min_score: float = Query(0.5, ge=0, le=1),
    min_trades: int = Query(10, ge=1)
):
    """Get top performing wallets by signal score"""
    try:
        wallets = await db.fetch("""
            SELECT * FROM wallets_master 
            WHERE signal_score >= $1 AND total_trades >= $2
            ORDER BY signal_score DESC 
            LIMIT $3
        """, min_score, min_trades, limit)
        
        return {
            "wallets": [dict(w) for w in wallets],
            "count": len(wallets),
            "filters": {
                "min_score": min_score,
                "min_trades": min_trades,
                "limit": limit
            }
        }
        
    except Exception as e:
        logger.error(f"Error fetching top wallets: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch top wallets")

@router.get("/{wallet}/details")
async def get_wallet_details(wallet: str):
    """Get detailed information about a specific wallet"""
    try:
        # Get wallet master data
        wallet_data = await db.fetchrow("""
            SELECT * FROM wallets_master WHERE wallet = $1
        """, wallet)
        
        if not wallet_data:
            raise HTTPException(status_code=404, detail="Wallet not found")
        
        # Get recent trades
        recent_trades = await db.fetch("""
            SELECT * FROM trades_log 
            WHERE wallet = $1 
            ORDER BY entry_time DESC 
            LIMIT 50
        """, wallet)
        
        # Get market breakdown
        market_stats = await db.fetch("""
            SELECT 
                market,
                COUNT(*) as trade_count,
                SUM(pnl) as total_pnl,
                AVG(pnl) as avg_pnl
            FROM trades_log 
            WHERE wallet = $1 AND pnl IS NOT NULL
            GROUP BY market
            ORDER BY total_pnl DESC
        """, wallet)
        
        return {
            "wallet_info": dict(wallet_data),
            "recent_trades": [dict(t) for t in recent_trades],
            "market_breakdown": [dict(m) for m in market_stats]
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching wallet details for {wallet}: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch wallet details")

@router.post("/score/{wallet}")
async def score_wallet(wallet: str):
    """Calculate and update score for a specific wallet"""
    try:
        scoring_service = WalletScoringService()
        score_data = await scoring_service.calculate_wallet_score(wallet)
        
        if score_data['total_trades'] == 0:
            raise HTTPException(status_code=404, detail="No trades found for wallet")
        
        # Update database
        await db.execute("""
            INSERT INTO wallets_master 
            (wallet, signal_score, realized_pnl, win_rate, avg_position_size,
             market_diversity, timing_edge, closing_efficiency, consistency_score,
             total_trades, active_days, last_trade_at, last_updated)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, NOW())
            ON CONFLICT (wallet) DO UPDATE SET
                signal_score = EXCLUDED.signal_score,
                realized_pnl = EXCLUDED.realized_pnl,
                win_rate = EXCLUDED.win_rate,
                avg_position_size = EXCLUDED.avg_position_size,
                market_diversity = EXCLUDED.market_diversity,
                timing_edge = EXCLUDED.timing_edge,
                closing_efficiency = EXCLUDED.closing_efficiency,
                consistency_score = EXCLUDED.consistency_score,
                total_trades = EXCLUDED.total_trades,
                active_days = EXCLUDED.active_days,
                last_trade_at = EXCLUDED.last_trade_at,
                last_updated = NOW()
        """, *[score_data[key] for key in [
            'wallet', 'signal_score', 'realized_pnl', 'win_rate', 'avg_position_size',
            'market_diversity', 'timing_edge', 'closing_efficiency', 'consistency_score',
            'total_trades', 'active_days', 'last_trade_at'
        ]])
        
        return score_data
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error scoring wallet {wallet}: {e}")
        raise HTTPException(status_code=500, detail="Failed to score wallet")

@router.post("/score/all")
async def score_all_wallets():
    """Recalculate scores for all wallets"""
    try:
        scoring_service = WalletScoringService()
        scored_count = await scoring_service.score_all_wallets()
        
        return {
            "status": "completed",
            "wallets_scored": scored_count,
            "message": f"Successfully scored {scored_count} wallets"
        }
        
    except Exception as e:
        logger.error(f"Error scoring all wallets: {e}")
        raise HTTPException(status_code=500, detail="Failed to score wallets")

@router.get("/search")
async def search_wallets(
    query: str = Query(..., min_length=3),
    limit: int = Query(20, ge=1, le=100)
):
    """Search wallets by address"""
    try:
        wallets = await db.fetch("""
            SELECT * FROM wallets_master 
            WHERE wallet ILIKE $1
            ORDER BY signal_score DESC 
            LIMIT $2
        """, f"%{query}%", limit)
        
        return {
            "wallets": [dict(w) for w in wallets],
            "count": len(wallets),
            "query": query
        }
        
    except Exception as e:
        logger.error(f"Error searching wallets: {e}")
        raise HTTPException(status_code=500, detail="Failed to search wallets")
