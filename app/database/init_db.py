import asyncio
import logging
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy import text
from app.models.database import Base, AsyncSessionLocal
from app.models.wallet import WalletMaster
from app.models.trade import TradesLog
from app.models.market import MarketSummary
from app.models.alert import AlertsLog
from app.models.copy_trade import CopyTrades
from config.settings import settings

logger = logging.getLogger(__name__)

async def create_tables():
    """Create all database tables"""
    try:
        engine = create_async_engine(
            settings.DATABASE_URL,
            echo=True,
            pool_pre_ping=True
        )
        
        async with engine.begin() as conn:
            # Create all tables
            await conn.run_sync(Base.metadata.create_all)
            
        logger.info("Database tables created successfully")
        return engine
        
    except Exception as e:
        logger.error(f"Error creating database tables: {e}")
        raise

async def init_database():
    """Initialize database with tables and indexes"""
    try:
        engine = await create_tables()
        
        # Create additional indexes for performance
        async with engine.begin() as conn:
            # Wallets indexes
            await conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_wallets_signal_score 
                ON wallets_master (signal_score DESC);
            """))
            
            await conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_wallets_last_updated 
                ON wallets_master (last_updated);
            """))
            
            # Trades indexes
            await conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_trades_wallet 
                ON trades_log (wallet);
            """))
            
            await conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_trades_entry_time 
                ON trades_log (entry_time);
            """))
            
            await conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_trades_market 
                ON trades_log (market);
            """))
            
            # Copy trades indexes
            await conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_copy_trades_source_wallet 
                ON copy_trades (source_wallet);
            """))
            
            await conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_copy_trades_status 
                ON copy_trades (status);
            """))
            
            await conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_copy_trades_created_at 
                ON copy_trades (created_at);
            """))
            
            # Alerts indexes
            await conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_alerts_timestamp 
                ON alerts_log (timestamp);
            """))
            
            await conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_alerts_wallet 
                ON alerts_log (wallet);
            """))
            
            await conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_alerts_processed 
                ON alerts_log (processed);
            """))
            
        logger.info("Database indexes created successfully")
        await engine.dispose()
        
    except Exception as e:
        logger.error(f"Error initializing database: {e}")
        raise

if __name__ == "__main__":
    asyncio.run(init_database())
