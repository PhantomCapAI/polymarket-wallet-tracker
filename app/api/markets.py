from fastapi import APIRouter, Query, HTTPException
from typing import List, Dict, Any, Optional

from app.models.database import db

router = APIRouter()

@router.get("/")
async def get_markets(
    limit: int = Query(50, ge=1, le=200),
    min_volume: Optional[float] = Query(None, ge=0)
) -> Dict[str, Any]:
    """Get market summary data"""
    try:
        query = "SELECT * FROM market_summary WHERE 1=1"
        params = []
        
        if min_volume is not None:
            query += f" AND total_volume >= ${len(params) + 1}"
            params.append(min_volume)
        
        query += f" ORDER BY total_volume DESC LIMIT ${len(params) + 1}"
        params.append(limit)
        
        markets = await db.fetch(query, *params)
        
        return {
            "markets": [dict(market) for market in markets],
            "total_count": len(markets)
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/{market_id}")
async def get_market_details(market_id: str) -> Dict[str, Any]:
    """Get detailed information about a specific market"""
    try:
        market = await db.fetchrow("""
            SELECT * FROM market_summary WHERE market = $1
        """, market_id)
        
        if not market:
            raise HTTPException(status_code=404, detail="Market not found")
        
        # Get recent trades for this market
        recent_trades = await db.fetch("""
            SELECT t.wallet, t.entry_price, t.position_size, t.entry_time, w.signal_score
            FROM trades_log t
            LEFT JOIN wallets_master w ON t.wallet = w.wallet
            WHERE t.market = $1
            ORDER BY t.entry_time DESC
            LIMIT 10
        """, market_id)
        
        return {
            "market_info": dict(market),
            "recent_trades": [dict(trade) for trade in recent_trades]
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/{market_id}/smart-money")
async def get_market_smart_money(market_id: str) -> Dict[str, Any]:
    """Get smart money activity for a specific market"""
    try:
        smart_money_activity = await db.fetch("""
            SELECT t.wallet, t.entry_price, t.position_size, t.entry_time, 
                   t.pnl, w.signal_score, w.win_rate
            FROM trades_log t
            JOIN wallets_master w ON t.wallet = w.wallet
            WHERE t.market = $1 AND w.signal_score > 0.7
            ORDER BY w.signal_score DESC, t.entry_time DESC
            LIMIT 20
        """, market_id)
        
        return {
            "market": market_id,
            "smart_money_trades": [dict(trade) for trade in smart_money_activity],
            "count": len(smart_money_activity)
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/trending/volume")
async def get_trending_markets() -> List[Dict[str, Any]]:
    """Get markets with highest trading volume"""
    try:
        trending = await db.fetch("""
            SELECT market, total_volume, smart_money_count, trend_bias
            FROM market_summary
            WHERE total_volume > 0
            ORDER BY total_volume DESC
            LIMIT 10
        """)
        
        return [dict(market) for market in trending]
        
    except Exception as e:
        return []
