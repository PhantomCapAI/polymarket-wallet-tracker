from dataclasses import dataclass
from typing import Optional, List
from datetime import datetime

@dataclass
class WalletMetrics:
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
    last_updated: datetime

@dataclass
class WalletFilter:
    min_signal_score: Optional[float] = None
    min_trades: Optional[int] = None
    min_volume: Optional[float] = None
    min_active_days: Optional[int] = None
    timing_edge: Optional[str] = None
    markets: Optional[List[str]] = None
