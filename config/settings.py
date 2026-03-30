from pydantic_settings import BaseSettings
from typing import Optional


class Settings(BaseSettings):
    # Database Configuration
    DATABASE_URL: str = "postgresql://user:password@localhost:5432/polymarket_bot"

    # Redis Configuration
    REDIS_URL: str = "redis://localhost:6379"
    REDIS_PASSWORD: Optional[str] = None
    REDIS_DB: int = 0
    REDIS_MAX_CONNECTIONS: int = 10

    # API Configuration
    HOST: str = "0.0.0.0"
    PORT: int = 8000
    DEBUG: bool = False

    # Polymarket Configuration
    POLYMARKET_API_KEY: str = ""
    POLYMARKET_SECRET: str = ""
    POLYMARKET_PASSPHRASE: str = ""
    POLYMARKET_PRIVATE_KEY: str = ""
    POLYMARKET_FUNDER: str = "0x326939f264b1Daa0De941cD8BeFDa28F42A02d5C"

    # Telegram Configuration
    TELEGRAM_BOT_TOKEN: str = ""
    TELEGRAM_CHAT_ID: str = ""

    # Scoring Weights (must sum to 1.0)
    WEIGHT_CONSISTENCY: float = 0.30
    WEIGHT_TIMING: float = 0.25
    WEIGHT_CLOSING: float = 0.15
    WEIGHT_PNL: float = 0.12
    WEIGHT_WIN_RATE: float = 0.10
    WEIGHT_DIVERSITY: float = 0.08

    # Trading Configuration
    MIN_TRADES: int = 10
    MIN_SIGNAL_SCORE: float = 0.75
    MAX_POSITION_SIZE: float = 1000.0
    STOP_LOSS_PERCENTAGE: float = 0.15
    BANKROLL: float = 10000.0

    # Position Sizing
    HIGH_CONFIDENCE_SIZE: float = 0.05
    MEDIUM_CONFIDENCE_SIZE: float = 0.02
    MAX_BANKROLL_PER_MARKET: float = 0.10
    MAX_TOTAL_EXPOSURE: float = 0.40

    # Risk Management
    DAILY_LOSS_LIMIT: float = 0.10
    CIRCUIT_BREAKER_LOSSES: int = 3
    CIRCUIT_BREAKER_HOURS: int = 6

    # Alert Thresholds
    HIGH_CONFIDENCE_THRESHOLD: float = 0.85
    MEDIUM_CONFIDENCE_THRESHOLD: float = 0.70

    # Copy Trading Settings
    COPY_TRADING_ENABLED: bool = False
    MAX_CONCURRENT_POSITIONS: int = 5
    POSITION_SIZE_MULTIPLIER: float = 0.1

    # Backtest Mode
    BACKTEST_MODE: bool = False

    model_config = {
        "env_file": ".env",
        "case_sensitive": True,
    }


settings = Settings()
