import logging
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.cron import CronTrigger

from app.models.database import db
from app.services.data_pipeline import data_pipeline
from app.services.wallet_scoring import wallet_scoring_service
from app.services.alerting import alerting_service
from app.services.trading import trading_service

logger = logging.getLogger(__name__)


class TaskScheduler:
    def __init__(self):
        self.scheduler = None  # type: AsyncIOScheduler | None

    def start(self):
        """Create the APScheduler instance, register jobs, and start."""
        self.scheduler = AsyncIOScheduler(timezone="UTC")

        # ── Data ingestion: every 15 minutes ──────────────────────────
        self.scheduler.add_job(
            self._run_data_update,
            trigger=IntervalTrigger(minutes=15),
            id="data_update",
            name="Fetch wallet and trade data from CLOB API",
            replace_existing=True,
            max_instances=1,
        )

        # ── Wallet scoring: every 15 minutes, offset by 5 min ────────
        self.scheduler.add_job(
            self._run_wallet_scoring,
            trigger=CronTrigger(minute="5,20,35,50"),
            id="wallet_scoring",
            name="Re-score all tracked wallets",
            replace_existing=True,
            max_instances=1,
        )

        # ── Market summary: every hour ────────────────────────────────
        self.scheduler.add_job(
            self._run_market_summary,
            trigger=IntervalTrigger(hours=1),
            id="market_summary",
            name="Aggregate market-level statistics",
            replace_existing=True,
            max_instances=1,
        )

        # ── Stop-loss monitor: every 60 seconds ──────────────────────
        self.scheduler.add_job(
            self._run_stop_loss_check,
            trigger=IntervalTrigger(seconds=60),
            id="stop_loss_check",
            name="Check open positions for stop-loss triggers",
            replace_existing=True,
            max_instances=1,
        )

        # ── Alert checks: every 2 minutes ────────────────────────────
        self.scheduler.add_job(
            self._run_alert_check,
            trigger=IntervalTrigger(minutes=2),
            id="alert_check",
            name="Check for convergence, conviction, and timing alerts",
            replace_existing=True,
            max_instances=1,
        )

        # ── Daily summary: midnight UTC ───────────────────────────────
        self.scheduler.add_job(
            self._run_daily_summary,
            trigger=CronTrigger(hour=0, minute=0),
            id="daily_summary",
            name="Send daily P&L summary via Telegram",
            replace_existing=True,
            max_instances=1,
        )

        self.scheduler.start()
        logger.info("TaskScheduler started with %d jobs", len(self.scheduler.get_jobs()))

    def stop(self):
        """Shut down the scheduler gracefully."""
        if self.scheduler and self.scheduler.running:
            self.scheduler.shutdown(wait=False)
            logger.info("TaskScheduler stopped")

    # ------------------------------------------------------------------
    # Job wrappers — each catches its own errors so one failure never
    # takes down the scheduler.
    # ------------------------------------------------------------------

    @staticmethod
    async def _run_data_update():
        logger.info("[job:data_update] starting")
        try:
            await data_pipeline.initialize()
            await data_pipeline.update_wallet_data(db)
            await data_pipeline.update_trade_data(db)
            logger.info("[job:data_update] finished")
        except Exception as exc:
            logger.error("[job:data_update] failed: %s", exc)

    @staticmethod
    async def _run_wallet_scoring():
        logger.info("[job:wallet_scoring] starting")
        try:
            scored = await wallet_scoring_service.score_all_wallets()
            logger.info("[job:wallet_scoring] scored %s wallets", scored)
        except Exception as exc:
            logger.error("[job:wallet_scoring] failed: %s", exc)

    @staticmethod
    async def _run_market_summary():
        logger.info("[job:market_summary] starting")
        try:
            await data_pipeline.initialize()
            await data_pipeline.update_market_summaries(db)
            logger.info("[job:market_summary] finished")
        except Exception as exc:
            logger.error("[job:market_summary] failed: %s", exc)

    @staticmethod
    async def _run_stop_loss_check():
        try:
            await trading_service.monitor_trades()
        except Exception as exc:
            logger.error("[job:stop_loss_check] failed: %s", exc)

    @staticmethod
    async def _run_alert_check():
        logger.info("[job:alert_check] starting")
        try:
            await alerting_service.check_all_alerts()
            logger.info("[job:alert_check] finished")
        except Exception as exc:
            logger.error("[job:alert_check] failed: %s", exc)

    @staticmethod
    async def _run_daily_summary():
        logger.info("[job:daily_summary] starting")
        try:
            await alerting_service.send_daily_summary()
            logger.info("[job:daily_summary] finished")
        except Exception as exc:
            logger.error("[job:daily_summary] failed: %s", exc)


# Global instance
scheduler = TaskScheduler()


async def start_scheduler():
    """Convenience function for lifespan startup."""
    scheduler.start()


async def stop_scheduler():
    """Convenience function for lifespan shutdown."""
    scheduler.stop()
