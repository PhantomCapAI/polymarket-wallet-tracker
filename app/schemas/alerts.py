from pydantic import BaseModel
from datetime import datetime

class AlertResponse(BaseModel):
    id: str
    timestamp: datetime
    wallet: str
    event_type: str
    confidence: str
    signal_reason: str
    market: str
    processed: str

    class Config:
        from_attributes = True
