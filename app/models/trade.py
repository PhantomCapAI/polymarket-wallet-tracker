from pydantic import BaseModel
from typing import Optional
from datetime import datetime
from decimal import Decimal

class Trade(BaseModel):
    id: str
    wallet: str
    market: str
    entry_time: datetime
    exit_time: Optional[datetime]
    entry_price: float
    exit_price: Optional[float]
    peak_price: Optional[float]
    position_size: float
    pnl: float
    outcome: Optional[str]
    created_at: datetime

class TradeAnalysis(BaseModel):
    trade: Trade
    timing_classification: str  # early, mid, late
    closing_efficiency: float
    market_context: dict
    alpha_score: float
