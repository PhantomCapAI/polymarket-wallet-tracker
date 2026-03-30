from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from datetime import datetime
from typing import Optional

from app.database.connection import get_db
from app.services.backtesting import backtesting_service

router = APIRouter()

@router.post("/backtest")
async def run_backtest(
    start_date: datetime,
    end_date: datetime,
    min_signal_score: Optional[float] = 0.6,
    max_position_size: Optional[float] = 1000.0,
    initial_balance: Optional[float] = 10000.0,
    db: AsyncSession = Depends(get_db)
):
    """Run backtest simulation"""
    try:
        if end_date <= start_date:
            raise HTTPException(status_code=400, detail="End date must be after start date")
        
        if (end_date - start_date).days > 365:
            raise HTTPException(status_code=400, detail="Backtest period cannot exceed 365 days")
        
        parameters = {
            'min_signal_score': min_signal_score,
            'max_position_size': max_position_size,
            'initial_balance': initial_balance
        }
        
        results = await backtesting_service.run_backtest(
            db, start_date, end_date, parameters
        )
        
        return {
            "start_date": results.start_date,
            "end_date": results.end_date,
            "total_trades": results.total_trades,
            "winning_trades": results.winning_trades,
            "losing_trades": results.losing_trades,
            "net_pnl": results.net_pnl,
            "win_rate": results.win_rate,
            "avg_win": results.avg_win,
            "avg_loss": results.avg_loss,
            "max_drawdown": results.max_drawdown,
            "sharpe_ratio": results.sharpe_ratio,
            "sortino_ratio": results.sortino_ratio,
            "max_consecutive_losses": results.max_consecutive_losses,
            "daily_pnl": results.daily_pnl
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
