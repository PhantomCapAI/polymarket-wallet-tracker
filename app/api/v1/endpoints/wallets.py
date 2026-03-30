from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, and_, desc, func
from typing import Optional, List
from datetime import datetime, timedelta

from app.database.connection import get_db
from app.models.wallet import WalletMaster
from app.models.trade import TradesLog
from app.schemas.wallet import WalletResponse, WalletDetailResponse, LeaderboardResponse

router = APIRouter()

@router.get("/leaderboard", response_model=List[LeaderboardResponse])
async def get_leaderboard(
    limit: int = Query(default=50, ge=1, le=100),
    min_trades: int = Query(default=50, ge=1),
    db: AsyncSession = Depends(get_db)
):
    """Get wallet leaderboard ranked by signal score"""
    try:
        query = select(WalletMaster).where(
            and_(
                WalletMaster.total_trades >= min_trades,
                WalletMaster.active_days >= 7
            )
        ).order_by(desc(WalletMaster.signal_score)).limit(limit)
        
        result = await db.execute(query)
        wallets = result.scalars().all()
        
        leaderboard = []
        for i, wallet in enumerate(wallets, 1):
            leaderboard.append(LeaderboardResponse(
                rank=i,
                wallet=wallet.wallet,
                signal_score=float(wallet.signal_score),
                realized_pnl=float(wallet.realized_pnl),
                win_rate=float(wallet.win_rate),
                total_trades=wallet.total_trades,
                timing_edge=wallet.timing_edge,
                consistency_score=float(wallet.consistency_score)
            ))
        
        return leaderboard
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/wallets/{address}", response_model=WalletDetailResponse)
async def get_wallet_details(
    address: str,
    db: AsyncSession = Depends(get_db)
):
    """Get detailed information about a specific wallet"""
    try:
        # Get wallet master data
        wallet_query = select(WalletMaster).where(WalletMaster.wallet == address)
        wallet_result = await db.execute(wallet_query)
        wallet = wallet_result.scalar_one_or_none()
        
        if not wallet:
            raise HTTPException(status_code=404, detail="Wallet not found")
        
        # Get recent trades
        recent_trades_query = select(TradesLog).where(
            TradesLog.wallet == address
        ).order_by(desc(TradesLog.entry_time)).limit(20)
        
        recent_trades_result = await db.execute(recent_trades_query)
        recent_trades = recent_trades_result.scalars().all()
        
        # Calculate additional metrics
        if recent_trades:
            recent_pnl = sum(float(trade.pnl) for trade in recent_trades if trade.pnl)
            recent_wins = sum(1 for trade in recent_trades if trade.pnl and trade.pnl > 0)
            recent_win_rate = recent_wins / len(recent_trades) if recent_trades else 0
        else:
            recent_pnl = 0.0
            recent_win_rate = 0.0
        
        # Get market performance
        market_performance_query = select(
            TradesLog.market,
            func.count(TradesLog.id).label('trade_count'),
            func.avg(TradesLog.pnl).label('avg_pnl'),
            func.sum(TradesLog.pnl).label('total_pnl')
        ).where(
            and_(
                TradesLog.wallet == address,
                TradesLog.pnl.isnot(None)
            )
        ).group_by(TradesLog.market).order_by(desc('total_pnl')).limit(10)
        
        market_performance_result = await db.execute(market_performance_query)
        market_performance = [
            {
                "market": row.market,
                "trade_count": row.trade_count,
                "avg_pnl": float(row.avg_pnl) if row.avg_pnl else 0.0,
                "total_pnl": float(row.total_pnl) if row.total_pnl else 0.0
            }
            for row in market_performance_result.fetchall()
        ]
        
        return WalletDetailResponse(
            wallet=wallet.wallet,
            signal_score=float(wallet.signal_score),
            realized_pnl=float(wallet.realized_pnl),
            win_rate=float(wallet.win_rate),
            avg_position_size=float(wallet.avg_position_size),
            market_diversity=wallet.market_diversity,
            timing_edge=wallet.timing_edge,
            closing_efficiency=float(wallet.closing_efficiency),
            consistency_score=float(wallet.consistency_score),
            total_trades=wallet.total_trades,
            active_days=wallet.active_days,
            last_trade_at=wallet.last_trade_at,
            recent_pnl=recent_pnl,
            recent_win_rate=recent_win_rate,
            market_performance=market_performance,
            recent_trades=[
                {
                    "market": trade.market,
                    "entry_time": trade.entry_time,
                    "exit_time": trade.exit_time,
                    "pnl": float(trade.pnl) if trade.pnl else 0.0,
                    "outcome": trade.outcome
                }
                for trade in recent_trades
            ]
        )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/wallets", response_model=List[WalletResponse])
async def search_wallets(
    search: Optional[str] = Query(None, description="Search by wallet address"),
    min_signal_score: Optional[float] = Query(None, ge=0, le=1),
    min_trades: Optional[int] = Query(None, ge=1),
    timing_edge: Optional[str] = Query(None, pattern="^(early|average|late)$"),
    limit: int = Query(default=20, ge=1, le=100),
    db: AsyncSession = Depends(get_db)
):
    """Search and filter wallets"""
    try:
        query = select(WalletMaster)
        conditions = []
        
        if search:
            conditions.append(WalletMaster.wallet.ilike(f"%{search}%"))
        
        if min_signal_score is not None:
            conditions.append(WalletMaster.signal_score >= min_signal_score)
        
        if min_trades is not None:
            conditions.append(WalletMaster.total_trades >= min_trades)
        
        if timing_edge:
            conditions.append(WalletMaster.timing_edge == timing_edge)
        
        if conditions:
            query = query.where(and_(*conditions))
        
        query = query.order_by(desc(WalletMaster.signal_score)).limit(limit)
        
        result = await db.execute(query)
        wallets = result.scalars().all()
        
        return [
            WalletResponse(
                wallet=wallet.wallet,
                signal_score=float(wallet.signal_score),
                realized_pnl=float(wallet.realized_pnl),
                win_rate=float(wallet.win_rate),
                total_trades=wallet.total_trades,
                timing_edge=wallet.timing_edge,
                last_updated=wallet.last_updated
            )
            for wallet in wallets
        ]
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
