import os
from pydantic_settings import BaseSettings
from typing import Optional

class Settings(BaseSettings):
    # Database Configuration
    DATABASE_URL: str = os.getenv("DATABASE_URL", "postgresql://user:password@localhost:5432/polymarket_bot")
    
    # Redis Configuration
    REDIS_URL: str = os.getenv("REDIS_URL", "redis://localhost:6379")
    REDIS_PASSWORD: Optional[str] = os.getenv("REDIS_PASSWORD")
    REDIS_DB: int = int(os.getenv("REDIS_DB", "0"))
    REDIS_MAX_CONNECTIONS: int = int(os.getenv("REDIS_MAX_CONNECTIONS", "10"))
    
    # API Configuration
    HOST: str = os.getenv("HOST", "0.0.0.0")
    PORT: int = int(os.getenv("PORT", "8000"))
    DEBUG: bool = os.getenv("DEBUG", "false").lower() == "true"
    
    # Polymarket Configuration
    POLYMARKET_API_KEY: str = os.getenv("POLYMARKET_API_KEY", "")
    POLYMARKET_SECRET: str = os.getenv("POLYMARKET_SECRET", "")
    POLYMARKET_PASSPHRASE: str = os.getenv("POLYMARKET_PASSPHRASE", "")
    
    # Telegram Configuration
    TELEGRAM_BOT_TOKEN: str = os.getenv("TELEGRAM_BOT_TOKEN", "")
    TELEGRAM_CHAT_ID: str = os.getenv("TELEGRAM_CHAT_ID", "")
    
    # Scoring Weights (must sum to 1.0)
    WEIGHT_CONSISTENCY: float = 0.20
    WEIGHT_TIMING: float = 0.25
    WEIGHT_CLOSING: float = 0.15
    WEIGHT_PNL: float = 0.25
    WEIGHT_WIN_RATE: float = 0.10
    WEIGHT_DIVERSITY: float = 0.05
    
    # Trading Configuration
    MIN_TRADES: int = 10
    MIN_SIGNAL_SCORE: float = 0.75
    MAX_POSITION_SIZE: float = 1000.0
    STOP_LOSS_PERCENTAGE: float = 0.10
    
    # Alert Thresholds
    HIGH_CONFIDENCE_THRESHOLD: float = 0.85
    MEDIUM_CONFIDENCE_THRESHOLD: float = 0.70
    
    # Copy Trading Settings
    COPY_TRADING_ENABLED: bool = os.getenv("COPY_TRADING_ENABLED", "false").lower() == "true"
    MAX_CONCURRENT_POSITIONS: int = 5
    POSITION_SIZE_MULTIPLIER: float = 0.1
    
    class Config:
        env_file = ".env"
        case_sensitive = True

settings = Settings()
