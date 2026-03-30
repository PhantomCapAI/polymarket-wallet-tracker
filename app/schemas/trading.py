from pydantic import BaseModel
from typing import Optional
from datetime import datetime

class CopyTradeResponse(BaseModel):
    id: str
    source_wallet: str
    market: str
    direction: str
    entry_price: float
    exit_price: Optional[float]
    position_size: float
    signal_score: float
    status: str
    pnl: Optional[float]
    created_at: datetime
    closed_at: Optional[datetime]

    class Config:
        from_attributes = True

class TradingStatsResponse(BaseModel):
    total_trades: int
    open_positions: int
    total_pnl: float
    win_rate: float
    avg_position_size: float
    best_trade: float
    worst_trade: float

    class Config:
        from_attributes = True
