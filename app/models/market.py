from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime

class MarketSummary(BaseModel):
    market: str
    total_volume: float
    avg_win_rate: float
    top_wallet: Optional[str]
    volatility: float
    trend_bias: str  # bullish, bearish, neutral
    smart_money_count: int
    last_updated: datetime

class MarketIntelligence(BaseModel):
    market: str
    summary: MarketSummary
    top_wallets: List[str]
    recent_activity: List[dict]
    convergence_signals: List[dict]
