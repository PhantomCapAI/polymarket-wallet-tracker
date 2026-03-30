import asyncio
import logging
from datetime import datetime, timedelta
from typing import Optional

from app.models.database import AsyncSessionLocal
from app.services.data_pipeline import data_pipeline
from app.services.wallet_scoring import wallet_scoring_service
from app.services.alerting import alerting_service
from app.services.trading import trading_service

logger = logging.getLogger(__name__)

class TaskScheduler:
    def __init__(self):
        self.running = False
        self.tasks = []
    
    async def start(self):
        """Start the task scheduler"""
        if self.running:
            return
        
        self.running = True
        
        # Schedule tasks
        self.tasks = [
            asyncio.create_task(self._data_update_loop()),
            asyncio.create_task(self._wallet_scoring_loop()),
            asyncio.create_task(self._alert_check_loop()),
            asyncio.create_task(self._stop_loss_check_loop()),
            asyncio.create_task(self._daily_summary_loop())
        ]
        
        logger.info("Task scheduler started")
    
    async def stop(self):
        """Stop the task scheduler"""
        self.running = False
        
        for task in self.tasks:
            task.cancel()
        
        await asyncio.gather(*self.tasks, return_exceptions=True)
        self.tasks = []
        
        logger.info("Task scheduler stopped")
    
    async def _data_update_loop(self):
        """Update data every 5 minutes"""
        while self.running:
            try:
                async with AsyncSessionLocal() as db:
                    await data_pipeline.update_wallet_data(db)
                    await data_pipeline.update_trade_data(db)
                    await data_pipeline.update_market_summaries(db)
                
                logger.info("Data update completed")
                
            except Exception as e:
                logger.error(f"Error in data update loop: {e}")
            
            await asyncio.sleep(300)  # 5 minutes
    
    async def _wallet_scoring_loop(self):
        """Update wallet scores every 10 minutes"""
        while self.running:
            try:
                async with AsyncSessionLocal() as db:
                    await wallet_scoring_service.batch_update_scores(db)
                
                logger.info("Wallet scoring completed")
                
            except Exception as e:
                logger.error(f"Error in wallet scoring loop: {e}")
            
            await asyncio.sleep(600)  # 10 minutes
    
    async def _alert_check_loop(self):
        """Check for alerts every 2 minutes"""
        while self.running:
            try:
                async with AsyncSessionLocal() as db:
                    alerts = await alerting_service.check_alert_conditions(db)
                
                if alerts:
                    logger.info(f"Generated {len(alerts)} alerts")
                
            except Exception as e:
                logger.error(f"Error in alert check loop: {e}")
            
            await asyncio.sleep(120)  # 2 minutes
    
    async def _stop_loss_check_loop(self):
        """Check stop losses every minute"""
        while self.running:
            try:
                async with AsyncSessionLocal() as db:
                    closed_positions = await trading_service.check_stop_losses(db)
                
                if closed_positions:
                    logger.info(f"Closed {len(closed_positions)} positions due to stop loss")
                
            except Exception as e:
                logger.error(f"Error in stop loss check loop: {e}")
            
            await asyncio.sleep(60)  # 1 minute
    
    async def _daily_summary_loop(self):
        """Send daily summary at 9 AM UTC"""
        while self.running:
            try:
                now = datetime.utcnow()
                next_run = now.replace(hour=9, minute=0, second=0, microsecond=0)
                
                if next_run <= now:
                    next_run += timedelta(days=1)
                
                sleep_seconds = (next_run - now).total_seconds()
                await asyncio.sleep(sleep_seconds)
                
                if self.running:
                    async with AsyncSessionLocal() as db:
                        await alerting_service.send_daily_pnl_summary(db)
                    
                    logger.info("Daily summary sent")
                
            except Exception as e:
                logger.error(f"Error in daily summary loop: {e}")

# Global scheduler instance
scheduler = TaskScheduler()

async def start_scheduler():
    """Start the background scheduler"""
    await scheduler.start()

async def stop_scheduler():
    """Stop the background scheduler"""
    await scheduler.stop()
