from dataclasses import dataclass
from typing import Optional
from datetime import datetime
from decimal import Decimal

@dataclass
class CopyTrade:
    id: str
    source_wallet: str
    market: str
    direction: str
    entry_price: float
    position_size: float
    signal_score: float
    status: str
    stop_loss_price: Optional[float] = None
    exit_price: Optional[float] = None
    pnl: Optional[float] = None
    created_at: Optional[datetime] = None
    closed_at: Optional[datetime] = None

@dataclass
class CopyTradeRequest:
    source_wallet: str
    market: str
    direction: str
    signal_score: float
    confidence_level: str
