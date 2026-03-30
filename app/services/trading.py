import logging
import uuid
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta

import aiohttp

from app.models.database import db
from config.settings import settings

logger = logging.getLogger(__name__)

# --------------------------------------------------------------------------- #
# Optional dependency: py_clob_client
# --------------------------------------------------------------------------- #
_CLOB_AVAILABLE = False
try:
    from py_clob_client.client import ClobClient
    from py_clob_client.clob_types import OrderArgs, ApiCreds
    _CLOB_AVAILABLE = True
except ImportError:
    logger.warning(
        "py_clob_client is not installed. Copy-trading execution is disabled; "
        "tracking and scoring will continue to work."
    )

# --------------------------------------------------------------------------- #
# Constants
# --------------------------------------------------------------------------- #
CLOB_HOST = "https://clob.polymarket.com"
CHAIN_ID = 137
SIGNATURE_TYPE = 1

HIGH_CONFIDENCE_THRESHOLD = 0.8
MEDIUM_CONFIDENCE_THRESHOLD = 0.6
HIGH_CONFIDENCE_ALLOC = 0.05   # 5% of bankroll
MEDIUM_CONFIDENCE_ALLOC = 0.02  # 2% of bankroll

MAX_SINGLE_MARKET_EXPOSURE = 0.10   # 10%
MAX_TOTAL_EXPOSURE = 0.40           # 40%
STOP_LOSS_PCT = 0.15                # 15%
DAILY_LOSS_HALT_PCT = 0.10          # 10%
CIRCUIT_BREAKER_LOSSES = 3
CIRCUIT_BREAKER_HOURS = 6
DAILY_HALT_HOURS = 24


class TradingService:
    """Real copy-trade execution against the Polymarket CLOB."""

    def __init__(self):
        self.client: Optional[Any] = None
        self.consecutive_losses: int = 0
        self.halted_until: Optional[datetime] = None

        if _CLOB_AVAILABLE and settings.POLYMARKET_PRIVATE_KEY:
            try:
                self.client = ClobClient(
                    host=CLOB_HOST,
                    key=settings.POLYMARKET_PRIVATE_KEY,
                    chain_id=CHAIN_ID,
                    signature_type=SIGNATURE_TYPE,
                    funder=settings.POLYMARKET_FUNDER,
                )
                self.client.set_api_creds(ApiCreds(
                    api_key=settings.POLYMARKET_API_KEY,
                    api_secret=settings.POLYMARKET_SECRET,
                    api_passphrase=settings.POLYMARKET_PASSPHRASE,
                ))
                logger.info("ClobClient initialized successfully")
            except Exception:
                logger.exception("Failed to initialize ClobClient")
                self.client = None
        else:
            if not _CLOB_AVAILABLE:
                logger.warning("ClobClient unavailable (py_clob_client not installed)")
            else:
                logger.warning("ClobClient unavailable (no POLYMARKET_PRIVATE_KEY)")

    # ------------------------------------------------------------------ #
    # Trade evaluation
    # ------------------------------------------------------------------ #

    async def evaluate_copy_trade(
        self,
        wallet: str,
        market: str,
        direction: str,
        entry_price: float,
        signal_score: float,
    ) -> bool:
        """Decide whether a copy-trade should be executed.

        Returns True when the trade passes all filters.
        """
        if signal_score < MEDIUM_CONFIDENCE_THRESHOLD:
            return False

        if self._is_halted():
            logger.info("Trading halted -- skipping evaluation")
            return False

        bankroll = await self._get_bankroll()

        # Already have a position in this market?
        existing = await db.fetchval("""
            SELECT COUNT(*) FROM copy_trades
            WHERE market = $1 AND status = 'open'
        """, market)
        if existing and int(existing) > 0:
            return False

        # Single-market exposure
        market_exposure = await self._market_exposure(market)
        if market_exposure >= MAX_SINGLE_MARKET_EXPOSURE * bankroll:
            return False

        # Total portfolio exposure
        total_exposure = await self._total_exposure()
        if total_exposure >= MAX_TOTAL_EXPOSURE * bankroll:
            return False

        return True

    # ------------------------------------------------------------------ #
    # Trade execution
    # ------------------------------------------------------------------ #

    async def execute_copy_trade(
        self,
        source_wallet: str,
        market: str,
        token_id: str,
        direction: str,
        price: float,
        signal_score: float,
    ) -> Dict[str, Any]:
        """Place a limit-order copy-trade and log it."""
        bankroll = await self._get_bankroll()

        # Position sizing by confidence tier
        if signal_score >= HIGH_CONFIDENCE_THRESHOLD:
            alloc = HIGH_CONFIDENCE_ALLOC
            confidence = "high"
        else:
            alloc = MEDIUM_CONFIDENCE_ALLOC
            confidence = "medium"

        position_size = bankroll * alloc
        trade_id = str(uuid.uuid4())
        order_id: Optional[str] = None
        executed = False

        # Actual order placement (skip in backtest mode)
        if not settings.BACKTEST_MODE:
            if self.client is None:
                logger.error("Cannot execute copy trade: ClobClient not available")
                return {"success": False, "error": "ClobClient not available"}

            try:
                side = "BUY" if direction.lower() == "buy" else "SELL"
                order_args = OrderArgs(
                    token_id=token_id,
                    price=price,
                    size=position_size / price,  # shares
                    side=side,
                )
                resp = self.client.create_and_post_order(order_args)
                order_id = resp.get("orderID") or resp.get("order_id")
                executed = True
                logger.info("Order placed: %s", order_id)
            except Exception:
                logger.exception("Order placement failed for %s", market)
                return {"success": False, "error": "Order placement failed"}
        else:
            executed = True  # backtest: treat as successful

        # Log to copy_trades
        await db.execute("""
            INSERT INTO copy_trades
                (id, source_wallet, market, direction, entry_price,
                 position_size, signal_score, status, stop_loss_price, created_at)
            VALUES ($1,$2,$3,$4,$5,$6,$7,'open',$8, NOW())
        """,
            trade_id, source_wallet, market, direction, price,
            position_size, signal_score,
            price * (1 - STOP_LOSS_PCT) if direction.lower() == "buy" else price * (1 + STOP_LOSS_PCT),
        )

        # Telegram alert
        await self._send_telegram(
            f"COPY TRADE EXECUTED\n"
            f"Market: {market}\n"
            f"Direction: {direction.upper()}\n"
            f"Price: {price:.4f}\n"
            f"Size: ${position_size:,.2f}\n"
            f"Confidence: {confidence}\n"
            f"Signal: {signal_score:.4f}\n"
            f"Source: {source_wallet[:12]}...\n"
            f"{'BACKTEST' if settings.BACKTEST_MODE else 'LIVE'}"
        )

        return {
            "success": True,
            "trade_id": trade_id,
            "order_id": order_id,
            "position_size": position_size,
            "confidence": confidence,
            "backtest": settings.BACKTEST_MODE,
        }

    # ------------------------------------------------------------------ #
    # Stop-loss / risk management
    # ------------------------------------------------------------------ #

    async def monitor_trades(self) -> None:
        """Alias for scheduler compatibility."""
        await self.check_stop_losses()

    async def check_stop_losses(self) -> None:
        """Iterate open positions and enforce stop-loss + circuit breaker rules."""
        if self._is_halted():
            return

        open_trades = await db.fetch("""
            SELECT id, market, direction, entry_price, position_size,
                   stop_loss_price, signal_score, created_at
            FROM copy_trades
            WHERE status = 'open'
        """)

        if not open_trades:
            return

        bankroll = await self._get_bankroll()

        # Check daily loss
        daily_pnl = await self._daily_realized_pnl()
        if daily_pnl <= -(DAILY_LOSS_HALT_PCT * bankroll):
            logger.warning("Daily loss limit hit (%.2f). Halting for %dh.", daily_pnl, DAILY_HALT_HOURS)
            self.halted_until = datetime.utcnow() + timedelta(hours=DAILY_HALT_HOURS)
            # Close everything
            for trade in open_trades:
                token_id = trade['market']
                current = await self.get_current_price(token_id)
                if current is not None:
                    await self.close_position(str(trade['id']), current)
            await self._send_telegram(
                f"DAILY LOSS LIMIT\nDaily PnL: ${daily_pnl:,.2f}\nAll positions closed. Trading halted {DAILY_HALT_HOURS}h."
            )
            return

        for trade in open_trades:
            token_id = trade['market']
            current_price = await self.get_current_price(token_id)
            if current_price is None:
                continue

            entry_price = float(trade['entry_price'])
            direction = trade['direction']

            if direction.lower() == "buy":
                pnl_pct = (current_price - entry_price) / entry_price
            else:
                pnl_pct = (entry_price - current_price) / entry_price

            if pnl_pct <= -STOP_LOSS_PCT:
                logger.info("Stop-loss triggered for trade %s (%.2f%%)", trade['id'], pnl_pct * 100)
                await self.close_position(str(trade['id']), current_price)

    # ------------------------------------------------------------------ #
    # Position closing
    # ------------------------------------------------------------------ #

    async def close_position(self, trade_id: str, exit_price: float) -> Dict[str, Any]:
        """Close a position: update DB, track consecutive losses, send alert."""
        trade = await db.fetchrow("SELECT * FROM copy_trades WHERE id = $1", trade_id)
        if trade is None:
            return {"success": False, "error": "Trade not found"}

        entry_price = float(trade['entry_price'])
        position_size = float(trade['position_size'])
        direction = trade['direction']

        if direction.lower() == "buy":
            pnl = (exit_price - entry_price) / entry_price * position_size
        else:
            pnl = (entry_price - exit_price) / entry_price * position_size

        await db.execute("""
            UPDATE copy_trades
            SET exit_price = $1, pnl = $2, status = 'closed', closed_at = NOW()
            WHERE id = $3
        """, exit_price, pnl, trade_id)

        # Circuit breaker tracking
        if pnl < 0:
            self.consecutive_losses += 1
            if self.consecutive_losses >= CIRCUIT_BREAKER_LOSSES:
                self.halted_until = datetime.utcnow() + timedelta(hours=CIRCUIT_BREAKER_HOURS)
                logger.warning(
                    "Circuit breaker: %d consecutive losses. Pausing %dh.",
                    self.consecutive_losses, CIRCUIT_BREAKER_HOURS,
                )
                await self._send_telegram(
                    f"CIRCUIT BREAKER\n{self.consecutive_losses} consecutive losses.\n"
                    f"Trading paused for {CIRCUIT_BREAKER_HOURS}h."
                )
        else:
            self.consecutive_losses = 0

        pnl_label = f"+${pnl:,.2f}" if pnl >= 0 else f"-${abs(pnl):,.2f}"
        await self._send_telegram(
            f"POSITION CLOSED\nTrade: {trade_id[:8]}...\nPnL: {pnl_label}\n"
            f"Exit: {exit_price:.4f}"
        )

        return {"success": True, "trade_id": trade_id, "pnl": pnl, "exit_price": exit_price}

    # ------------------------------------------------------------------ #
    # Price fetching (async via aiohttp)
    # ------------------------------------------------------------------ #

    async def get_current_price(self, token_id: str) -> Optional[float]:
        """Fetch current mid-price from Polymarket CLOB REST API."""
        url = f"{CLOB_HOST}/prices"
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, params={"token_ids": token_id}, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    if resp.status != 200:
                        logger.warning("Price API returned %d for %s", resp.status, token_id)
                        return None
                    data = await resp.json()
                    # Response shape: { token_id: price_str }
                    price_str = data.get(token_id)
                    if price_str is not None:
                        return float(price_str)
                    return None
        except Exception:
            logger.exception("Failed to fetch price for %s", token_id)
            return None

    # ------------------------------------------------------------------ #
    # Portfolio summary
    # ------------------------------------------------------------------ #

    async def get_portfolio_summary(self) -> Dict[str, Any]:
        """Aggregate portfolio stats from copy_trades."""
        row = await db.fetchrow("""
            SELECT
                COUNT(*) FILTER (WHERE status = 'open')   AS open_positions,
                COALESCE(SUM(position_size) FILTER (WHERE status = 'open'), 0) AS total_exposure,
                COALESCE(SUM(pnl) FILTER (WHERE status = 'closed'), 0) AS total_pnl,
                COALESCE(SUM(pnl) FILTER (WHERE status = 'closed'
                    AND closed_at >= CURRENT_DATE), 0) AS daily_pnl
            FROM copy_trades
        """)

        bankroll = await self._get_bankroll()

        return {
            "total_value": bankroll + float(row['total_pnl'] or 0),
            "open_positions": int(row['open_positions'] or 0),
            "total_exposure": float(row['total_exposure'] or 0),
            "daily_pnl": float(row['daily_pnl'] or 0),
            "total_pnl": float(row['total_pnl'] or 0),
            "exposure_pct": (float(row['total_exposure'] or 0) / bankroll * 100) if bankroll > 0 else 0,
            "halted": self._is_halted(),
            "halted_until": self.halted_until.isoformat() if self.halted_until else None,
        }

    # ------------------------------------------------------------------ #
    # Private helpers
    # ------------------------------------------------------------------ #

    def _is_halted(self) -> bool:
        if self.halted_until is None:
            return False
        if datetime.utcnow() >= self.halted_until:
            self.halted_until = None
            self.consecutive_losses = 0
            return False
        return True

    async def _get_bankroll(self) -> float:
        return settings.BANKROLL

    async def _market_exposure(self, market: str) -> float:
        val = await db.fetchval("""
            SELECT COALESCE(SUM(position_size), 0)
            FROM copy_trades
            WHERE market = $1 AND status = 'open'
        """, market)
        return float(val or 0)

    async def _total_exposure(self) -> float:
        val = await db.fetchval("""
            SELECT COALESCE(SUM(position_size), 0)
            FROM copy_trades WHERE status = 'open'
        """)
        return float(val or 0)

    async def _daily_realized_pnl(self) -> float:
        val = await db.fetchval("""
            SELECT COALESCE(SUM(pnl), 0)
            FROM copy_trades
            WHERE status = 'closed' AND closed_at >= CURRENT_DATE
        """)
        return float(val or 0)

    async def _send_telegram(self, text: str) -> None:
        """Best-effort Telegram notification."""
        token = settings.TELEGRAM_BOT_TOKEN
        chat_id = settings.TELEGRAM_CHAT_ID
        if not token or not chat_id:
            return
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        try:
            async with aiohttp.ClientSession() as session:
                await session.post(
                    url,
                    json={"chat_id": chat_id, "text": text, "parse_mode": "Markdown"},
                    timeout=aiohttp.ClientTimeout(total=10),
                )
        except Exception:
            logger.debug("Telegram send failed", exc_info=True)


# Global instance
trading_service = TradingService()
