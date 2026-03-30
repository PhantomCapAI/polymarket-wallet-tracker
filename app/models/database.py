import asyncpg
import logging
from typing import Optional
from config.settings import settings

logger = logging.getLogger(__name__)


class Database:
    def __init__(self):
        self.pool: Optional[asyncpg.Pool] = None

    async def connect(self):
        """Establish database connection pool"""
        try:
            self.pool = await asyncpg.create_pool(
                settings.DATABASE_URL,
                min_size=5,
                max_size=20,
                command_timeout=60
            )
            logger.info("Database connection pool created successfully")
        except Exception as e:
            logger.error(f"Failed to create database pool: {e}")
            raise

    async def disconnect(self):
        """Close database connection pool"""
        if self.pool:
            await self.pool.close()
            logger.info("Database connection pool closed")

    async def fetch(self, query: str, *args):
        """Execute a query and return all results"""
        if not self.pool:
            raise RuntimeError("Database not connected")

        async with self.pool.acquire() as conn:
            return await conn.fetch(query, *args)

    async def fetchrow(self, query: str, *args):
        """Execute a query and return one result"""
        if not self.pool:
            raise RuntimeError("Database not connected")

        async with self.pool.acquire() as conn:
            return await conn.fetchrow(query, *args)

    async def fetchval(self, query: str, *args):
        """Execute a query and return a single value"""
        if not self.pool:
            raise RuntimeError("Database not connected")

        async with self.pool.acquire() as conn:
            return await conn.fetchval(query, *args)

    async def execute(self, query: str, *args):
        """Execute a query without returning results"""
        if not self.pool:
            raise RuntimeError("Database not connected")

        async with self.pool.acquire() as conn:
            return await conn.execute(query, *args)

    async def executemany(self, query: str, args_list):
        """Execute a query multiple times with different parameters"""
        if not self.pool:
            raise RuntimeError("Database not connected")

        async with self.pool.acquire() as conn:
            return await conn.executemany(query, args_list)


# Global database instance
db = Database()


async def init_database():
    """Initialize database connection and create tables if needed"""
    await db.connect()
    await _create_tables()
    await _create_indexes()


async def _create_tables():
    """Create database tables if they don't exist"""
    tables = [
        """
        CREATE TABLE IF NOT EXISTS wallets_master (
            wallet VARCHAR(42) PRIMARY KEY,
            signal_score DECIMAL(10,6) DEFAULT 0,
            realized_pnl DECIMAL(20,6) DEFAULT 0,
            win_rate DECIMAL(5,3) DEFAULT 0,
            avg_position_size DECIMAL(20,6) DEFAULT 0,
            market_diversity DECIMAL(5,3) DEFAULT 0,
            timing_edge DECIMAL(5,3) DEFAULT 0,
            closing_efficiency DECIMAL(5,3) DEFAULT 0,
            consistency_score DECIMAL(5,3) DEFAULT 0,
            total_trades INTEGER DEFAULT 0,
            active_days INTEGER DEFAULT 0,
            last_trade_at TIMESTAMPTZ,
            last_updated TIMESTAMPTZ DEFAULT NOW()
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS trades_log (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            wallet VARCHAR(42) NOT NULL,
            market VARCHAR(255),
            direction VARCHAR(10),
            entry_price DECIMAL(20,10),
            position_size DECIMAL(20,6),
            exit_price DECIMAL(20,10),
            peak_price DECIMAL(20,10),
            pnl DECIMAL(20,6),
            outcome VARCHAR(20),
            entry_time TIMESTAMPTZ DEFAULT NOW(),
            exit_time TIMESTAMPTZ,
            created_at TIMESTAMPTZ DEFAULT NOW()
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS alerts_log (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            wallet VARCHAR(42) NOT NULL,
            event_type VARCHAR(50) NOT NULL,
            confidence VARCHAR(20) NOT NULL,
            signal_reason TEXT,
            market VARCHAR(255),
            timestamp TIMESTAMPTZ DEFAULT NOW(),
            processed BOOLEAN DEFAULT FALSE
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS backtest_results (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            days_back INTEGER NOT NULL,
            min_signal_score DECIMAL(5,3) NOT NULL,
            status VARCHAR(20) DEFAULT 'pending',
            total_pnl DECIMAL(20,6),
            win_rate DECIMAL(5,3),
            total_trades INTEGER,
            error TEXT,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            completed_at TIMESTAMPTZ
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS copy_trades (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            source_wallet VARCHAR(42),
            market VARCHAR(255),
            direction VARCHAR(10),
            entry_price DECIMAL(20,10),
            exit_price DECIMAL(20,10),
            position_size DECIMAL(20,6),
            signal_score DECIMAL(10,6),
            status VARCHAR(20) DEFAULT 'open',
            pnl DECIMAL(20,6),
            stop_loss_price DECIMAL(20,10),
            created_at TIMESTAMPTZ DEFAULT NOW(),
            closed_at TIMESTAMPTZ
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS market_summary (
            market VARCHAR(255) PRIMARY KEY,
            total_volume DECIMAL(20,6),
            avg_win_rate DECIMAL(5,3),
            top_wallet VARCHAR(42),
            volatility DECIMAL(10,6),
            trend_bias VARCHAR(20),
            smart_money_count INTEGER DEFAULT 0,
            last_updated TIMESTAMPTZ DEFAULT NOW()
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS fees_collected (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            trade_id UUID REFERENCES copy_trades(id),
            gross_pnl DECIMAL(20,6) NOT NULL,
            fee_pct DECIMAL(5,4) NOT NULL,
            fee_amount DECIMAL(20,6) NOT NULL,
            net_pnl DECIMAL(20,6) NOT NULL,
            treasury_wallet VARCHAR(42) NOT NULL,
            collected_at TIMESTAMPTZ DEFAULT NOW()
        )
        """,
    ]

    for table_sql in tables:
        try:
            await db.execute(table_sql)
            logger.info("Table created/verified successfully")
        except Exception as e:
            logger.error(f"Error creating table: {e}")
            raise


async def _create_indexes():
    """Create indexes for query performance"""
    indexes = [
        "CREATE INDEX IF NOT EXISTS idx_trades_log_wallet ON trades_log (wallet)",
        "CREATE INDEX IF NOT EXISTS idx_trades_log_market ON trades_log (market)",
        "CREATE INDEX IF NOT EXISTS idx_trades_log_entry_time ON trades_log (entry_time)",
        "CREATE INDEX IF NOT EXISTS idx_trades_log_wallet_market ON trades_log (wallet, market)",
        "CREATE INDEX IF NOT EXISTS idx_alerts_log_wallet ON alerts_log (wallet)",
        "CREATE INDEX IF NOT EXISTS idx_alerts_log_timestamp ON alerts_log (timestamp)",
        "CREATE INDEX IF NOT EXISTS idx_alerts_log_processed ON alerts_log (processed) WHERE NOT processed",
        "CREATE INDEX IF NOT EXISTS idx_copy_trades_source ON copy_trades (source_wallet)",
        "CREATE INDEX IF NOT EXISTS idx_copy_trades_status ON copy_trades (status)",
        "CREATE INDEX IF NOT EXISTS idx_copy_trades_market ON copy_trades (market)",
        "CREATE INDEX IF NOT EXISTS idx_wallets_master_score ON wallets_master (signal_score DESC)",
        "CREATE INDEX IF NOT EXISTS idx_backtest_results_status ON backtest_results (status)",
        "CREATE INDEX IF NOT EXISTS idx_fees_collected_at ON fees_collected (collected_at)",
        "CREATE INDEX IF NOT EXISTS idx_fees_trade_id ON fees_collected (trade_id)",
    ]

    for index_sql in indexes:
        try:
            await db.execute(index_sql)
        except Exception as e:
            logger.error(f"Error creating index: {e}")
            raise


async def close_database():
    """Close database connection"""
    await db.disconnect()
