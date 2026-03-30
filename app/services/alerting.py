import asyncio
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
import uuid

from app.models.database import db
from app.models.alert import Alert, AlertConfig
from app.utils.telegram_bot import TelegramBot
from config.settings import settings

class AlertingService:
    def __init__(self):
        self.telegram_bot = TelegramBot()
        self.alert_config = AlertConfig()
        
    async def create_alert(self, wallet: str, event_type: str, confidence: str, signal_reason: str, market: str):
        """Create and store an alert"""
        try:
            alert_id = str(uuid.uuid4())
            
            await db.execute("""
                INSERT INTO alerts_log (id, wallet, event_type, confidence, signal_reason, market)
                VALUES ($1, $2, $3, $4, $5, $6)
            """, alert_id, wallet, event_type, confidence, signal_reason, market)
            
            # Send Telegram notification
            await self.send_telegram_alert(wallet, event_type, confidence, signal_reason, market)
            
        except Exception as e:
            print(f"Error creating alert: {e}")
    
    async def send_telegram_alert(self, wallet: str, event_type: str, confidence: str, signal_reason: str, market: str):
        """Send alert via Telegram bot"""
        try:
            message = self.format_alert_message(wallet, event_type, confidence, signal_reason, market)
            await self.telegram_bot.send_message(message)
        except Exception as e:
            print(f"Error sending Telegram alert: {e}")
    
    def format_alert_message(self, wallet: str, event_type: str, confidence: str, signal_reason: str, market: str) -> str:
        """Format alert message for Telegram"""
        emoji_map = {
            'high': '🔥',
            'medium': '⚠️', 
            'low': 'ℹ️',
            'entry': '📈',
            'alignment': '🎯',
            'anomaly': '⚡',
            'conviction': '💪'
        }
        
        confidence_emoji = emoji_map.get(confidence, '')
        event_emoji = emoji_map.get(event_type, '')
        
        message = f"{confidence_emoji} {event_emoji} **{event_type.upper()} SIGNAL**\n\n"
        message += f"**Wallet:** `{wallet[:10]}...`\n"
        message += f"**Market:** {market}\n"
        message += f"**Confidence:** {confidence.upper()}\n"
        message += f"**Reason:** {signal_reason}\n"
        message += f"**Time:** {datetime.now().strftime('%H:%M:%S UTC')}"
        
        return message
    
    async def send_copy_trade_alert(self, trade_id: str, market: str, direction: str, position_size: float, confidence: str, signal_score: float):
        """Send alert when copy trade is executed"""
        try:
            message = f"🤖 **COPY TRADE EXECUTED**\n\n"
            message += f"**Trade ID:** `{trade_id[:8]}...`\n"
            message += f"**Market:** {market}\n"
            message += f"**Direction:** {direction.upper()}\n"
            message += f"**Size:** ${position_size:,.2f}\n"
            message += f"**Confidence:** {confidence.upper()}\n"
            message += f"**Signal Score:** {signal_score:.3f}\n"
            message += f"**Time:** {datetime.now().strftime('%H:%M:%S UTC')}"
            
            await self.telegram_bot.send_message(message)
        except Exception as e:
            print(f"Error sending copy trade alert: {e}")
    
    async def send_trade_closed_alert(self, trade_id: str, pnl: float, reason: str):
        """Send alert when trade is closed"""
        try:
            pnl_emoji = "✅" if pnl > 0 else "❌"
            
            message = f"{pnl_emoji} **TRADE CLOSED**\n\n"
            message += f"**Trade ID:** `{trade_id[:8]}...`\n"
            message += f"**P&L:** ${pnl:,.2f}\n"
            message += f"**Reason:** {reason}\n"
            message += f"**Time:** {datetime.now().strftime('%H:%M:%S UTC')}"
            
            await self.telegram_bot.send_message(message)
        except Exception as e:
            print(f"Error sending trade closed alert: {e}")
    
    async def send_circuit_breaker_alert(self, consecutive_losses: int):
        """Send alert when circuit breaker is triggered"""
        try:
            message = f"🚨 **CIRCUIT BREAKER ACTIVATED**\n\n"
            message += f"**Consecutive Losses:** {consecutive_losses}\n"
            message += f"**Trading Halted:** {settings.CIRCUIT_BREAKER_HOURS} hours\n"
            message += f"**Time:** {datetime.now().strftime('%H:%M:%S UTC')}\n\n"
            message += "All trading has been suspended for risk management."
            
            await self.telegram_bot.send_message(message)
        except Exception as e:
            print(f"Error sending circuit breaker alert: {e}")
    
    async def send_daily_limit_alert(self, daily_pnl: float):
        """Send alert when daily loss limit is hit"""
        try:
            message = f"🛑 **DAILY LOSS LIMIT REACHED**\n\n"
            message += f"**Daily P&L:** ${daily_pnl:,.2f}\n"
            message += f"**Limit:** -{settings.DAILY_LOSS_LIMIT*100}% of bankroll\n"
            message += f"**Time:** {datetime.now().strftime('%H:%M:%S UTC')}\n\n"
            message += "All positions closed. Trading halted until tomorrow."
            
            await self.telegram_bot.send_message(message)
        except Exception as e:
            print(f"Error sending daily limit alert: {e}")
    
    async def send_convergence_alert(self, market: str, wallets: List[str], confidence: str):
        """Send alert when multiple high-signal wallets converge on same market"""
        try:
            message = f"🎯 **CONVERGENCE SIGNAL**\n\n"
            message += f"**Market:** {market}\n"
            message += f"**Wallets Aligned:** {len(wallets)}\n"
            message += f"**Confidence:** {confidence.upper()}\n"
            message += f"**Time:** {datetime.now().strftime('%H:%M:%S UTC')}\n\n"
            message += f"**Wallets:**\n"
            
            for wallet in wallets[:5]:  # Show max 5 wallets
                message += f"• `{wallet[:10]}...`\n"
            
            if len(wallets) > 5:
                message += f"• ...and {len(wallets) - 5} more"
            
            await self.telegram_bot.send_message(message)
        except Exception as e:
            print(f"Error sending convergence alert: {e}")
    
    async def send_daily_summary(self):
        """Send daily P&L summary at midnight UTC"""
        try:
            # Get daily stats
            daily_trades = await db.fetchval("""
                SELECT COUNT(*) FROM copy_trades 
                WHERE created_at::date = CURRENT_DATE
            """)
            
            daily_pnl = await db.fetchval("""
                SELECT COALESCE(SUM(pnl), 0) FROM copy_trades 
                WHERE status != 'open' AND closed_at::date = CURRENT_DATE
            """)
            
            open_positions = await db.fetchval("""
                SELECT COUNT(*) FROM copy_trades WHERE status = 'open'
            """)
            
            total_pnl = await db.fetchval("""
                SELECT COALESCE(SUM(pnl), 0) FROM copy_trades WHERE status != 'open'
            """)
            
            # Get top performing wallet of the day
            top_wallet = await db.fetchrow("""
                SELECT w.wallet, w.signal_score 
                FROM wallets_master w
                ORDER BY w.signal_score DESC
                LIMIT 1
            """)
            
            message = f"📊 **DAILY SUMMARY - {datetime.now().strftime('%Y-%m-%d')}**\n\n"
            message += f"**Today's Trades:** {int(daily_trades or 0)}\n"
            message += f"**Daily P&L:** ${float(daily_pnl or 0):,.2f}\n"
            message += f"**Open Positions:** {int(open_positions or 0)}\n"
            message += f"**Total P&L:** ${float(total_pnl or 0):,.2f}\n"
            
            if top_wallet:
                message += f"\n**Top Wallet:** `{top_wallet['wallet'][:10]}...`\n"
                message += f"**Signal Score:** {float(top_wallet['signal_score']):.3f}"
            
            await self.telegram_bot.send_message(message)
            
        except Exception as e:
            print(f"Error sending daily summary: {e}")
    
    async def check_convergence_signals(self):
        """Check for convergence signals - multiple high-score wallets on same market"""
        try:
            # Get recent trades by high-signal wallets
            convergence_data = await db.fetch("""
                SELECT t.market, COUNT(DISTINCT t.wallet) as wallet_count, 
                       ARRAY_AGG(DISTINCT t.wallet) as wallets
                FROM trades_log t
                JOIN wallets_master w ON t.wallet = w.wallet
                WHERE w.signal_score > 0.7 
                AND t.entry_time > NOW() - INTERVAL '1 hour'
                GROUP BY t.market
                HAVING COUNT(DISTINCT t.wallet) >= $1
            """, self.alert_config.convergence_threshold)
            
            for convergence in convergence_data:
                market = convergence['market']
                wallets = convergence['wallets']
                wallet_count = convergence['wallet_count']
                
                # Check if we already alerted on this convergence
                recent_alert = await db.fetchrow("""
                    SELECT * FROM alerts_log 
                    WHERE event_type = 'alignment' 
                    AND market = $1 
                    AND timestamp > NOW() - INTERVAL '4 hours'
                """, market)
                
                if not recent_alert:
                    confidence = 'high' if wallet_count >= 5 else 'medium'
                    
                    await self.create_alert(
                        f"{wallet_count}_wallets",
                        'alignment',
                        confidence,
                        f"{wallet_count} high-signal wallets converged on market",
                        market
                    )
                    
                    await self.send_convergence_alert(market, wallets, confidence)
            
        except Exception as e:
            print(f"Error checking convergence signals: {e}")
    
    async def check_unusual_conviction(self):
        """Check for unusual conviction trades (3x+ normal position size)"""
        try:
            # Get wallets with recent large trades
            conviction_trades = await db.fetch("""
                SELECT t.wallet, t.market, t.position_size, w.avg_position_size, w.signal_score
                FROM trades_log t
                JOIN wallets_master w ON t.wallet = w.wallet
                WHERE t.entry_time > NOW() - INTERVAL '1 hour'
                AND t.position_size > w.avg_position_size * $1
                AND w.signal_score > $2
                AND w.avg_position_size > 0
            """, self.alert_config.conviction_multiplier, self.alert_config.min_signal_score)
            
            for trade in conviction_trades:
                # Check if we already alerted on this wallet recently
                recent_alert = await db.fetchrow("""
                    SELECT * FROM alerts_log 
                    WHERE wallet = $1 
                    AND event_type = 'conviction' 
                    AND timestamp > NOW() - INTERVAL '2 hours'
                """, trade['wallet'])
                
                if not recent_alert:
                    size_multiplier = trade['position_size'] / trade['avg_position_size']
                    confidence = 'high' if size_multiplier >= 5 else 'medium'
                    
                    await self.create_alert(
                        trade['wallet'],
                        'conviction',
                        confidence,
                        f"Unusual conviction trade: {size_multiplier:.1f}x normal position size",
                        trade['market']
                    )
            
        except Exception as e:
            print(f"Error checking unusual conviction: {e}")
    
    async def check_timing_anomalies(self):
        """Check for unusual timing patterns that might indicate alpha"""
        try:
            # Get wallets with consistently early entries
            timing_anomalies = await db.fetch("""
                SELECT w.wallet, w.signal_score, COUNT(t.id) as recent_trades
                FROM wallets_master w
                JOIN trades_log t ON w.wallet = t.wallet
                WHERE w.timing_edge = 'early'
                AND w.signal_score > 0.8
                AND t.entry_time > NOW() - INTERVAL '24 hours'
                GROUP BY w.wallet, w.signal_score
                HAVING COUNT(t.id) >= 3
            """)
            
            for anomaly in timing_anomalies:
                # Check if we already alerted
                recent_alert = await db.fetchrow("""
                    SELECT * FROM alerts_log 
                    WHERE wallet = $1 
                    AND event_type = 'anomaly' 
                    AND timestamp > NOW() - INTERVAL '6 hours'
                """, anomaly['wallet'])
                
                if not recent_alert:
                    await self.create_alert(
                        anomaly['wallet'],
                        'anomaly',
                        'high',
                        f"Consistently early entries: {anomaly['recent_trades']} trades in 24h",
                        'timing_pattern'
                    )
            
        except Exception as e:
            print(f"Error checking timing anomalies: {e}")
