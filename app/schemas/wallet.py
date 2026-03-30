from pydantic import BaseModel
from typing import Optional, List, Dict, Any
from datetime import datetime
from decimal import Decimal

class WalletResponse(BaseModel):
    wallet: str
    signal_score: float
    realized_pnl: float
    win_rate: float
    total_trades: int
    timing_edge: str
    last_updated: datetime

    class Config:
        from_attributes = True

class LeaderboardResponse(BaseModel):
    rank: int
    wallet: str
    signal_score: float
    realized_pnl: float
    win_rate: float
    total_trades: int
    timing_edge: str
    consistency_score: float

    class Config:
        from_attributes = True

class WalletDetailResponse(BaseModel):
    wallet: str
    signal_score: float
    realized_pnl: float
    win_rate: float
    avg_position_size: float
    market_diversity: int
    timing_edge: str
    closing_efficiency: float
    consistency_score: float
    total_trades: int
    active_days: int
    last_trade_at: Optional[datetime]
    recent_pnl: float
    recent_win_rate: float
    market_performance: List[Dict[str, Any]]
    recent_trades: List[Dict[str, Any]]

    class Config:
        from_attributes = True
