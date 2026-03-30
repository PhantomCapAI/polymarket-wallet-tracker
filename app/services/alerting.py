import logging
from typing import List, Dict, Any, Optional
from datetime import datetime
import uuid

from app.models.database import db
from app.models.alert import AlertConfig
from app.utils.telegram_bot import TelegramBot
from config.settings import settings

logger = logging.getLogger(__name__)


class AlertingService:
    def __init__(self):
        self.telegram = TelegramBot()
        self.config = AlertConfig()

    # ------------------------------------------------------------------
    # Orchestrator
    # ------------------------------------------------------------------

    async def check_all_alerts(self):
        """Run every alert check in sequence."""
        try:
            await self.check_new_positions()
            await self.check_convergence()
            await self.check_conviction_spikes()
            await self.check_timing_anomalies()
        except Exception as e:
            logger.error(f"Error in check_all_alerts: {e}")

    # ------------------------------------------------------------------
    # Detection: new positions by top-10 wallets
    # ------------------------------------------------------------------

    async def check_new_positions(self):
        """Detect when any top-10-scored wallet enters a new position."""
        try:
            rows = await db.fetch("""
                SELECT t.wallet, t.market, t.direction, t.entry_price, t.position_size,
                       w.signal_score
                FROM trades_log t
                JOIN wallets_master w ON t.wallet = w.wallet
                WHERE t.entry_time > NOW() - INTERVAL '15 minutes'
                  AND t.exit_time IS NULL
                  AND w.wallet IN (
                      SELECT wallet FROM wallets_master
                      ORDER BY signal_score DESC
                      LIMIT 10
                  )
                ORDER BY w.signal_score DESC
            """)

            for row in rows:
                already = await db.fetchrow("""
                    SELECT 1 FROM alerts_log
                    WHERE wallet = $1
                      AND event_type = 'new_position'
                      AND market = $2
                      AND timestamp > NOW() - INTERVAL '1 hour'
                """, row['wallet'], row['market'])

                if already:
                    continue

                confidence = 'high' if float(row['signal_score']) >= 0.85 else 'medium'
                reason = (
                    f"Top-10 wallet opened {row['direction'].upper()} "
                    f"${float(row['position_size']):,.2f} @ {float(row['entry_price']):.4f}"
                )

                await self.create_alert(
                    row['wallet'], 'new_position', confidence, reason, row['market']
                )
        except Exception as e:
            logger.error(f"Error in check_new_positions: {e}")

    # ------------------------------------------------------------------
    # Detection: convergence of high-score wallets on same market
    # ------------------------------------------------------------------

    async def check_convergence(self):
        """Detect when multiple wallets with signal_score > 0.7 align on the same market."""
        try:
            rows = await db.fetch("""
                SELECT t.market,
                       COUNT(DISTINCT t.wallet) AS wallet_count,
                       ARRAY_AGG(DISTINCT t.wallet) AS wallets,
                       AVG(w.signal_score) AS avg_score
                FROM trades_log t
                JOIN wallets_master w ON t.wallet = w.wallet
                WHERE w.signal_score > 0.7
                  AND t.entry_time > NOW() - INTERVAL '1 hour'
                  AND t.exit_time IS NULL
                GROUP BY t.market
                HAVING COUNT(DISTINCT t.wallet) >= $1
            """, self.config.convergence_threshold)

            for row in rows:
                already = await db.fetchrow("""
                    SELECT 1 FROM alerts_log
                    WHERE event_type = 'convergence'
                      AND market = $1
                      AND timestamp > NOW() - INTERVAL '4 hours'
                """, row['market'])

                if already:
                    continue

                wallet_count = row['wallet_count']
                avg_score = float(row['avg_score'])
                confidence = 'high' if wallet_count >= 5 else 'medium'
                wallets_list = row['wallets']

                reason = (
                    f"{wallet_count} high-signal wallets converged "
                    f"(avg score {avg_score:.3f})"
                )
                await self.create_alert(
                    f"{wallet_count}_wallets", 'convergence', confidence, reason, row['market']
                )

                # Dedicated convergence Telegram message
                wallet_lines = "\n".join(
                    f"  - `{w[:10]}...`" for w in wallets_list[:5]
                )
                extra = f"\n  - ...and {len(wallets_list) - 5} more" if len(wallets_list) > 5 else ""
                msg = (
                    f"🎯 *CONVERGENCE SIGNAL*\n\n"
                    f"*Market:* {row['market']}\n"
                    f"*Wallets aligned:* {wallet_count}\n"
                    f"*Avg signal score:* {avg_score:.3f}\n"
                    f"*Confidence:* {confidence.upper()}\n\n"
                    f"*Wallets:*\n{wallet_lines}{extra}"
                )
                await self.telegram.send_message(msg)

        except Exception as e:
            logger.error(f"Error in check_convergence: {e}")

    # ------------------------------------------------------------------
    # Detection: conviction spikes (3x+ average position size)
    # ------------------------------------------------------------------

    async def check_conviction_spikes(self):
        """Detect when a wallet's position is 3x+ its historical average."""
        try:
            rows = await db.fetch("""
                SELECT t.wallet, t.market, t.position_size,
                       w.avg_position_size, w.signal_score
                FROM trades_log t
                JOIN wallets_master w ON t.wallet = w.wallet
                WHERE t.entry_time > NOW() - INTERVAL '1 hour'
                  AND t.position_size > w.avg_position_size * $1
                  AND w.avg_position_size > 0
                  AND w.signal_score > $2
            """, self.config.conviction_multiplier, self.config.min_signal_score)

            for row in rows:
                already = await db.fetchrow("""
                    SELECT 1 FROM alerts_log
                    WHERE wallet = $1
                      AND event_type = 'conviction_spike'
                      AND timestamp > NOW() - INTERVAL '2 hours'
                """, row['wallet'])

                if already:
                    continue

                multiplier = float(row['position_size']) / float(row['avg_position_size'])
                confidence = 'high' if multiplier >= 5 else 'medium'
                reason = (
                    f"Position size {multiplier:.1f}x wallet average "
                    f"(${float(row['position_size']):,.2f} vs avg "
                    f"${float(row['avg_position_size']):,.2f})"
                )

                await self.create_alert(
                    row['wallet'], 'conviction_spike', confidence, reason, row['market']
                )
        except Exception as e:
            logger.error(f"Error in check_conviction_spikes: {e}")

    # ------------------------------------------------------------------
    # Detection: timing anomalies
    # ------------------------------------------------------------------

    async def check_timing_anomalies(self):
        """Detect sudden improvements in timing edge for high-score wallets."""
        try:
            rows = await db.fetch("""
                SELECT w.wallet, w.signal_score, w.timing_edge,
                       COUNT(t.id) AS recent_trades
                FROM wallets_master w
                JOIN trades_log t ON w.wallet = t.wallet
                WHERE w.timing_edge > 0.8
                  AND w.signal_score > 0.8
                  AND t.entry_time > NOW() - INTERVAL '24 hours'
                GROUP BY w.wallet, w.signal_score, w.timing_edge
                HAVING COUNT(t.id) >= 3
            """)

            for row in rows:
                already = await db.fetchrow("""
                    SELECT 1 FROM alerts_log
                    WHERE wallet = $1
                      AND event_type = 'timing_anomaly'
                      AND timestamp > NOW() - INTERVAL '6 hours'
                """, row['wallet'])

                if already:
                    continue

                reason = (
                    f"Timing edge {float(row['timing_edge']):.3f} with "
                    f"{row['recent_trades']} trades in 24 h "
                    f"(signal {float(row['signal_score']):.3f})"
                )

                await self.create_alert(
                    row['wallet'], 'timing_anomaly', 'high', reason, 'timing_pattern'
                )
        except Exception as e:
            logger.error(f"Error in check_timing_anomalies: {e}")

    # ------------------------------------------------------------------
    # Core: persist alert + send Telegram
    # ------------------------------------------------------------------

    async def create_alert(
        self,
        wallet: str,
        event_type: str,
        confidence: str,
        signal_reason: str,
        market: str,
    ):
        """Insert into alerts_log and send a Telegram notification."""
        try:
            alert_id = str(uuid.uuid4())
            await db.execute("""
                INSERT INTO alerts_log (id, wallet, event_type, confidence, signal_reason, market)
                VALUES ($1, $2, $3, $4, $5, $6)
            """, alert_id, wallet, event_type, confidence, signal_reason, market)

            emoji_map = {
                'high': '🔥', 'medium': '⚠️', 'low': 'ℹ️',
            }
            event_emoji_map = {
                'new_position': '📈', 'convergence': '🎯',
                'conviction_spike': '💪', 'timing_anomaly': '⚡',
            }
            c_emoji = emoji_map.get(confidence, '🔔')
            e_emoji = event_emoji_map.get(event_type, '🔔')

            msg = (
                f"{c_emoji}{e_emoji} *{event_type.upper().replace('_', ' ')}*\n\n"
                f"*Wallet:* `{wallet[:10]}...`\n"
                f"*Market:* {market}\n"
                f"*Confidence:* {confidence.upper()}\n"
                f"*Reason:* {signal_reason}\n"
                f"*Time:* {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}"
            )
            await self.telegram.send_message(msg)
        except Exception as e:
            logger.error(f"Error creating alert: {e}")

    # ------------------------------------------------------------------
    # Daily summary
    # ------------------------------------------------------------------

    async def send_daily_summary(self):
        """Send end-of-day PnL summary via Telegram."""
        try:
            total_pnl = await db.fetchval("""
                SELECT COALESCE(SUM(pnl), 0) FROM copy_trades WHERE status != 'open'
            """)

            daily_pnl = await db.fetchval("""
                SELECT COALESCE(SUM(pnl), 0) FROM copy_trades
                WHERE status != 'open' AND closed_at::date = CURRENT_DATE
            """)

            daily_trades = await db.fetchval("""
                SELECT COUNT(*) FROM copy_trades
                WHERE created_at::date = CURRENT_DATE
            """)

            open_positions = await db.fetchval("""
                SELECT COUNT(*) FROM copy_trades WHERE status = 'open'
            """)

            portfolio_value = await db.fetchval("""
                SELECT COALESCE(SUM(position_size), 0) FROM copy_trades WHERE status = 'open'
            """)

            best_trade = await db.fetchrow("""
                SELECT market, pnl FROM copy_trades
                WHERE status != 'open' AND closed_at::date = CURRENT_DATE
                ORDER BY pnl DESC LIMIT 1
            """)

            worst_trade = await db.fetchrow("""
                SELECT market, pnl FROM copy_trades
                WHERE status != 'open' AND closed_at::date = CURRENT_DATE
                ORDER BY pnl ASC LIMIT 1
            """)

            pnl_emoji = "✅" if float(daily_pnl or 0) >= 0 else "❌"
            date_str = datetime.utcnow().strftime('%Y-%m-%d')

            msg = (
                f"📊 *DAILY SUMMARY — {date_str}*\n\n"
                f"*Today's P&L:* {pnl_emoji} ${float(daily_pnl or 0):,.2f}\n"
                f"*Total P&L (all time):* ${float(total_pnl or 0):,.2f}\n"
                f"*Trades today:* {int(daily_trades or 0)}\n"
                f"*Open positions:* {int(open_positions or 0)}\n"
                f"*Open exposure:* ${float(portfolio_value or 0):,.2f}\n"
            )

            if best_trade and best_trade['pnl'] is not None:
                msg += (
                    f"\n*Best trade:* {best_trade['market']}"
                    f" → ${float(best_trade['pnl']):,.2f}\n"
                )
            if worst_trade and worst_trade['pnl'] is not None:
                msg += (
                    f"*Worst trade:* {worst_trade['market']}"
                    f" → ${float(worst_trade['pnl']):,.2f}\n"
                )

            # Revenue stats
            daily_fees = await db.fetchval("""
                SELECT COALESCE(SUM(fee_amount), 0) FROM fees_collected
                WHERE collected_at::date = CURRENT_DATE
            """)
            total_fees = await db.fetchval("""
                SELECT COALESCE(SUM(fee_amount), 0) FROM fees_collected
            """)
            if float(total_fees or 0) > 0:
                msg += (
                    f"\n💰 *Revenue*\n"
                    f"*Fees today:* ${float(daily_fees or 0):,.2f}\n"
                    f"*Total fees (all time):* ${float(total_fees or 0):,.2f}\n"
                )

            await self.telegram.send_message(msg)
        except Exception as e:
            logger.error(f"Error sending daily summary: {e}")

    # ------------------------------------------------------------------
    # Copy-trade lifecycle alerts
    # ------------------------------------------------------------------

    async def send_copy_trade_alert(self, trade_info: Dict[str, Any]):
        """Alert when a copy trade is executed."""
        try:
            wallet = trade_info.get('source_wallet', 'unknown')
            market = trade_info.get('market', 'unknown')
            direction = trade_info.get('direction', '?')
            size = float(trade_info.get('position_size', 0))
            score = float(trade_info.get('signal_score', 0))
            confidence = 'HIGH' if score >= settings.HIGH_CONFIDENCE_THRESHOLD else 'MEDIUM'

            msg = (
                f"🤖 *COPY TRADE EXECUTED*\n\n"
                f"*Source wallet:* `{wallet[:10]}...`\n"
                f"*Market:* {market}\n"
                f"*Direction:* {direction.upper()}\n"
                f"*Size:* ${size:,.2f}\n"
                f"*Signal score:* {score:.3f}\n"
                f"*Confidence:* {confidence}\n"
                f"*Time:* {datetime.utcnow().strftime('%H:%M:%S UTC')}"
            )
            await self.telegram.send_message(msg)
        except Exception as e:
            logger.error(f"Error sending copy trade alert: {e}")

    async def send_trade_closed_alert(self, trade_info: Dict[str, Any]):
        """Alert when a copy trade is closed, including P&L."""
        try:
            trade_id = str(trade_info.get('id', ''))
            market = trade_info.get('market', 'unknown')
            pnl = float(trade_info.get('pnl', 0))
            reason = trade_info.get('reason', 'manual')
            pnl_emoji = "✅" if pnl >= 0 else "❌"

            msg = (
                f"{pnl_emoji} *TRADE CLOSED*\n\n"
                f"*Trade:* `{trade_id[:8]}...`\n"
                f"*Market:* {market}\n"
                f"*P&L:* ${pnl:,.2f}\n"
                f"*Reason:* {reason}\n"
                f"*Time:* {datetime.utcnow().strftime('%H:%M:%S UTC')}"
            )
            await self.telegram.send_message(msg)
        except Exception as e:
            logger.error(f"Error sending trade closed alert: {e}")

    # ------------------------------------------------------------------
    # Risk alerts (stop-loss, daily limit, circuit breaker)
    # ------------------------------------------------------------------

    async def send_risk_alert(self, alert_type: str, details: Dict[str, Any]):
        """Send risk-management alert (stop-loss, daily limit, circuit breaker)."""
        try:
            if alert_type == 'stop_loss':
                trade_id = str(details.get('trade_id', ''))
                market = details.get('market', 'unknown')
                pnl = float(details.get('pnl', 0))
                msg = (
                    f"🛑 *STOP-LOSS TRIGGERED*\n\n"
                    f"*Trade:* `{trade_id[:8]}...`\n"
                    f"*Market:* {market}\n"
                    f"*P&L:* ${pnl:,.2f}\n"
                    f"*Time:* {datetime.utcnow().strftime('%H:%M:%S UTC')}"
                )

            elif alert_type == 'daily_limit':
                daily_pnl = float(details.get('daily_pnl', 0))
                limit_pct = settings.DAILY_LOSS_LIMIT * 100
                msg = (
                    f"🛑 *DAILY LOSS LIMIT REACHED*\n\n"
                    f"*Daily P&L:* ${daily_pnl:,.2f}\n"
                    f"*Limit:* -{limit_pct:.0f}% of bankroll\n"
                    f"*Time:* {datetime.utcnow().strftime('%H:%M:%S UTC')}\n\n"
                    f"All positions closed. Trading halted until tomorrow."
                )

            elif alert_type == 'circuit_breaker':
                losses = int(details.get('consecutive_losses', 0))
                hours = settings.CIRCUIT_BREAKER_HOURS
                msg = (
                    f"🚨 *CIRCUIT BREAKER ACTIVATED*\n\n"
                    f"*Consecutive losses:* {losses}\n"
                    f"*Trading halted:* {hours} hours\n"
                    f"*Time:* {datetime.utcnow().strftime('%H:%M:%S UTC')}\n\n"
                    f"All trading suspended for risk management."
                )

            else:
                msg = (
                    f"⚠️ *RISK ALERT — {alert_type.upper()}*\n\n"
                    f"*Details:* {details}\n"
                    f"*Time:* {datetime.utcnow().strftime('%H:%M:%S UTC')}"
                )

            await self.telegram.send_message(msg)
        except Exception as e:
            logger.error(f"Error sending risk alert ({alert_type}): {e}")


# Global singleton
alerting_service = AlertingService()
