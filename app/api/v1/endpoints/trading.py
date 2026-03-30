from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, desc
from typing import List

from app.database.connection import get_db
from app.models.copy_trade import CopyTrades
from app.services.trading import trading_service
from app.schemas.trading import CopyTradeResponse, TradingStatsResponse

router = APIRouter()

@router.get("/copy-trades", response_model=List[CopyTradeResponse])
async def get_copy_trades(
    limit: int = 50,
    db: AsyncSession = Depends(get_db)
):
    """Get recent copy trades"""
    try:
        query = select(CopyTrades).order_by(desc(CopyTrades.created_at)).limit(limit)
        result = await db.execute(query)
        trades = result.scalars().all()
        
        return [
            CopyTradeResponse(
                id=trade.id,
                source_wallet=trade.source_wallet,
                market=trade.market,
                direction=trade.direction,
                entry_price=float(trade.entry_price),
                exit_price=float(trade.exit_price) if trade.exit_price else None,
                position_size=float(trade.position_size),
                signal_score=float(trade.signal_score),
                status=trade.status,
                pnl=float(trade.pnl) if trade.pnl else None,
                created_at=trade.created_at,
                closed_at=trade.closed_at
            )
            for trade in trades
        ]
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/trading-stats", response_model=TradingStatsResponse)
async def get_trading_stats(db: AsyncSession = Depends(get_db)):
    """Get overall trading statistics"""
    try:
        # Get all copy trades
        query = select(CopyTrades)
        result = await db.execute(query)
        all_trades = result.scalars().all()
        
        if not all_trades:
            return TradingStatsResponse(
                total_trades=0,
                open_positions=0,
                total_pnl=0.0,
                win_rate=0.0,
                avg_position_size=0.0,
                best_trade=0.0,
                worst_trade=0.0
            )
        
        # Calculate stats
        open_trades = [t for t in all_trades if t.status == 'open']
        closed_trades = [t for t in all_trades if t.status == 'closed' and t.pnl is not None]
        
        total_pnl = sum(float(trade.pnl) for trade in closed_trades)
        winning_trades = [t for t in closed_trades if t.pnl > 0]
        win_rate = len(winning_trades) / len(closed_trades) if closed_trades else 0.0
        
        avg_position_size = sum(float(t.position_size) for t in all_trades) / len(all_trades)
        best_trade = max((float(t.pnl) for t in closed_trades), default=0.0)
        worst_trade = min((float(t.pnl) for t in closed_trades), default=0.0)
        
        return TradingStatsResponse(
            total_trades=len(all_trades),
            open_positions=len(open_trades),
            total_pnl=total_pnl,
            win_rate=win_rate,
            avg_position_size=avg_position_size,
            best_trade=best_trade,
            worst_trade=worst_trade
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
