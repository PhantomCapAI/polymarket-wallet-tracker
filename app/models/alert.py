from dataclasses import dataclass
from typing import Optional
from datetime import datetime

@dataclass
class Alert:
    id: str
    wallet: str
    event_type: str
    confidence: str
    signal_reason: str
    market: str
    timestamp: datetime
    processed: bool = False

@dataclass
class AlertConfig:
    convergence_threshold: int = 3
    conviction_multiplier: float = 3.0
    min_signal_score: float = 0.7
    cooldown_hours: int = 2
