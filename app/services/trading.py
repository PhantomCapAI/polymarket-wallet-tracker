import asyncio
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
import uuid
from decimal import Decimal
import json

from app.models.database import db
from app.models.copy_trade import CopyTrade, CopyTradeRequest
from config.settings import settings

class TradingService:
    def __init__(self):
        self.client = None
        self.initialize_client()
        self.active_positions = {}
        self.daily_pnl = 0.0
        self.daily_loss_start = None
        self.circuit_breaker_active = False
        self.circuit_breaker_until = None
        self.consecutive_losses = 0
        
    def initialize_client(self):
        """Initialize Polymarket CLOB client"""
        if not settings.BACKTEST_MODE:
            try:
                # In production, would use py_clob_client
                from py_clob_client.client import ClobClient
                from py_clob_client.constants import POLYGON
                
                self.client = ClobClient(
                    host="https://clob.polymarket.com",
                    key=settings.POLYMARKET_API_KEY,
                    secret=settings.POLYMARKET_SECRET,
                    passphrase=settings.POLYMARKET_PASSPHRASE,
                    chain_id=POLYGON,
                )
            except ImportError:
                print("py_clob_client not installed, using mock client for development")
                self.client = MockClobClient()
            except Exception as e:
                print(f"Failed to initialize CLOB client: {e}")
                self.client = MockClobClient()
    
    async def monitor_trades(self):
        """Monitor open copy trades for stop-loss and risk management - runs every 60 seconds"""
        try:
            # Check circuit breaker
            if self.circuit_breaker_until and datetime.now() < self.circuit_breaker_until:
                return
            else:
                self.circuit_breaker_active = False
                self.circuit_breaker_until = None
            
            # Reset daily P&L at midnight
            now = datetime.now()
            if self.daily_loss_start and now.date() > self.daily_loss_start.date():
                self.daily_pnl = 0.0
                self.daily_loss_start = None
            
            # Get all open positions
            open_trades = await db.fetch("""
                SELECT * FROM copy_trades 
                WHERE status = 'open'
            """)
            
            current_bankroll = await self.get_current_bankroll()
            
            for trade in open_trades:
                await self.check_stop_loss(trade, current_bankroll)
                await self.check_risk_limits(trade, current_bankroll)
            
            # Check daily loss limit
            if self.daily_pnl <= -settings.DAILY_LOSS_LIMIT * current_bankroll:
                await self.trigger_daily_loss_halt()
                
        except Exception as e:
            print(f"Error monitoring trades: {e}")
    
    async def evaluate_copy_trade(self, wallet_data: Dict[str, Any]):
        """Evaluate whether to copy trade a high-signal wallet"""
        try:
            if settings.BACKTEST_MODE:
                return
            
            if self.circuit_breaker_active:
                return
            
            wallet_address = wallet_data['wallet']
            signal_score = wallet_data['signal_score']
            
            # Check confidence thresholds
            if signal_score < settings.MEDIUM_CONFIDENCE_THRESHOLD:
                return
            
            # Get recent positions for this wallet (simplified - would need real position tracking)
            recent_activity = await self.get_wallet_recent_activity(wallet_address)
            
            for activity in recent_activity:
                if await self.should_copy_trade(activity, signal_score):
                    await self.execute_copy_trade(activity, signal_score)
                    
        except Exception as e:
            print(f"Error evaluating copy trade for wallet {wallet_data['wallet']}: {e}")
    
    async def get_wallet_recent_activity(self, wallet_address: str) -> List[Dict[str, Any]]:
        """Get recent activity for a wallet (placeholder - would integrate with real API)"""
        # This would integrate with Polymarket API to get real positions
        # For now, return simulated activity
        return [
            {
                'wallet': wallet_address,
                'market': f'sample_market_{wallet_address[:8]}',
                'direction': 'buy',
                'price': 0.65,
                'size': 100.0,
                'timestamp': datetime.now()
            }
        ]
    
    async def should_copy_trade(self, activity: Dict[str, Any], signal_score: float) -> bool:
        """Determine if we should copy this specific trade"""
        try:
            market = activity['market']
            
            # Check if we already have a position in this market
            existing_position = await db.fetchrow("""
                SELECT * FROM copy_trades 
                WHERE market = $1 AND status = 'open'
            """, market)
            
            if existing_position:
                return False
            
            # Check market exposure limits
            current_bankroll = await self.get_current_bankroll()
            market_exposure = await self.get_market_exposure(market)
            
            if market_exposure >= settings.MAX_BANKROLL_PER_MARKET * current_bankroll:
                return False
            
            # Check total exposure
            total_exposure = await self.get_total_exposure()
            if total_exposure >= settings.MAX_TOTAL_EXPOSURE * current_bankroll:
                return False
            
            return True
            
        except Exception as e:
            print(f"Error checking copy trade conditions: {e}")
            return False
    
    async def execute_copy_trade(self, activity: Dict[str, Any], signal_score: float):
        """Execute a copy trade with proper position sizing"""
        try:
            if settings.BACKTEST_MODE:
                return
            
            market = activity['market']
            direction = activity['direction']
            price = activity['price']
            
            # Calculate position size based on confidence
            current_bankroll = await self.get_current_bankroll()
            
            if signal_score >= settings.HIGH_CONFIDENCE_THRESHOLD:
                position_size = current_bankroll * settings.HIGH_CONFIDENCE_SIZE
                confidence = 'high'
            else:
                position_size = current_bankroll * settings.MEDIUM_CONFIDENCE_SIZE
                confidence = 'medium'
            
            # Calculate stop-loss price
            stop_loss_price = price * (1 - settings.STOP_LOSS_PERCENT) if direction == 'buy' else price * (1 + settings.STOP_LOSS_PERCENT)
            
            # Execute limit order through CLOB client
            if self.client:
                try:
                    # Place limit order
                    order_result = await self.place_limit_order(market, direction, price, position_size)
                    
                    if order_result and order_result.get('success'):
                        # Record copy trade
                        trade_id = str(uuid.uuid4())
                        
                        await db.execute("""
                            INSERT INTO copy_trades 
                            (id, source_wallet, market, direction, entry_price, position_size, 
                             signal_score, status, stop_loss_price, created_at)
                            VALUES ($1, $2, $3, $4, $5, $6, $7, 'open', $8, NOW())
                        """,
                            trade_id,
                            activity.get('wallet', 'unknown'),
                            market,
                            direction,
                            price,
                            position_size,
                            signal_score,
                            stop_loss_price
                        )
                        
                        # Send alert
                        from app.services.alerting import AlertingService
                        alerting_service = AlertingService()
                        await alerting_service.send_copy_trade_alert(
                            trade_id, market, direction, position_size, confidence, signal_score
                        )
                        
                except Exception as e:
                    print(f"Failed to execute copy trade: {e}")
            
        except Exception as e:
            print(f"Error executing copy trade: {e}")
    
    async def place_limit_order(self, market: str, direction: str, price: float, size: float) -> Dict[str, Any]:
        """Place a limit order via CLOB client"""
        if not self.client:
            return {'success': False, 'error': 'No client available'}
        
        try:
            if isinstance(self.client, MockClobClient):
                return await self.client.create_order(market, direction, price, size)
            else:
                # Real py_clob_client implementation
                from py_clob_client.order_builder.constants import BUY, SELL
                from py_clob_client.utilities import build_market_order, build_limit_order
                
                side = BUY if direction == 'buy' else SELL
                order_args = build_limit_order(
                    token_id=market,
                    price=price,
                    size=size,
                    side=side
                )
                
                resp = self.client.create_order(order_args)
                return {'success': resp.get('success', False), 'order_id': resp.get('orderID')}
                
        except Exception as e:
            print(f"Error placing limit order: {e}")
            return {'success': False, 'error': str(e)}
    
    async def check_stop_loss(self, trade: Dict[str, Any], current_bankroll: float):
        """Check if trade should be stopped out"""
        try:
            trade_id = trade['id']
            market = trade['market']
            entry_price = float(trade['entry_price'])
            stop_loss_price = float(trade['stop_loss_price'])
            direction = trade['direction']
            
            # Get current market price (simplified - would use real API)
            current_price = await self.get_current_price(market)
            
            if current_price is None:
                return
            
            should_stop = False
            if direction == 'buy' and current_price <= stop_loss_price:
                should_stop = True
            elif direction == 'sell' and current_price >= stop_loss_price:
                should_stop = True
            
            if should_stop:
                await self.close_position(trade_id, current_price, 'stopped')
                
        except Exception as e:
            print(f"Error checking stop loss for trade {trade['id']}: {e}")
    
    async def check_risk_limits(self, trade: Dict[str, Any], current_bankroll: float):
        """Check portfolio-level risk limits"""
        try:
            # Check if position size is still within limits
            position_value = float(trade['position_size'])
            
            if position_value > settings.MAX_BANKROLL_PER_MARKET * current_bankroll * 1.5:  # 50% buffer
                await self.close_position(trade['id'], None, 'risk_limit')
                
        except Exception as e:
            print(f"Error checking risk limits: {e}")
    
    async def close_position(self, trade_id: str, exit_price: Optional[float], reason: str):
        """Close a copy trade position"""
        try:
            trade = await db.fetchrow("SELECT * FROM copy_trades WHERE id = $1", trade_id)
            if not trade:
                return
            
            entry_price = float(trade['entry_price'])
            position_size = float(trade['position_size'])
            direction = trade['direction']
            
            if exit_price is None:
                exit_price = await self.get_current_price(trade['market'])
                if exit_price is None:
                    exit_price = entry_price  # Fallback
            
            # Calculate P&L
            if direction == 'buy':
                pnl = (exit_price - entry_price) * (position_size / entry_price)
            else:
                pnl = (entry_price - exit_price) * (position_size / entry_price)
            
            # Update trade record
            await db.execute("""
                UPDATE copy_trades 
                SET exit_price = $1, pnl = $2, status = $3, closed_at = NOW()
                WHERE id = $4
            """, exit_price, pnl, reason, trade_id)
            
            # Update daily P&L
            self.daily_pnl += pnl
            if pnl < 0:
                self.consecutive_losses += 1
                if self.daily_loss_start is None:
                    self.daily_loss_start = datetime.now()
            else:
                self.consecutive_losses = 0
            
            # Check circuit breaker
            if self.consecutive_losses >= settings.CIRCUIT_BREAKER_LOSSES:
                await self.trigger_circuit_breaker()
            
            # Send alert
            from app.services.alerting import AlertingService
            alerting_service = AlertingService()
            await alerting_service.send_trade_closed_alert(trade_id, pnl, reason)
            
        except Exception as e:
            print(f"Error closing position {trade_id}: {e}")
    
    async def trigger_circuit_breaker(self):
        """Activate circuit breaker to halt trading"""
        self.circuit_breaker_active = True
        self.circuit_breaker_until = datetime.now() + timedelta(hours=settings.CIRCUIT_BREAKER_HOURS)
        
        # Send alert
        from app.services.alerting import AlertingService
        alerting_service = AlertingService()
        await alerting_service.send_circuit_breaker_alert(self.consecutive_losses)
    
    async def trigger_daily_loss_halt(self):
        """Halt trading due to daily loss limit"""
        self.daily_loss_start = datetime.now()
        
        # Close all open positions
        open_trades = await db.fetch("SELECT * FROM copy_trades WHERE status = 'open'")
        for trade in open_trades:
            await self.close_position(trade['id'], None, 'daily_limit')
        
        # Send alert
        from app.services.alerting import AlertingService
        alerting_service = AlertingService()
        await alerting_service.send_daily_limit_alert(self.daily_pnl)
    
    async def get_current_bankroll(self) -> float:
        """Get current bankroll value"""
        # This would integrate with actual wallet/account balance
        # For now, return a placeholder value
        return 10000.0
    
    async def get_market_exposure(self, market: str) -> float:
        """Get current exposure in a specific market"""
        result = await db.fetchval("""
            SELECT COALESCE(SUM(position_size), 0) 
            FROM copy_trades 
            WHERE market = $1 AND status = 'open'
        """, market)
        return float(result or 0)
    
    async def get_total_exposure(self) -> float:
        """Get total portfolio exposure"""
        result = await db.fetchval("""
            SELECT COALESCE(SUM(position_size), 0) 
            FROM copy_trades 
            WHERE status = 'open'
        """)
        return float(result or 0)
    
    async def get_current_price(self, market: str) -> Optional[float]:
        """Get current market price"""
        try:
            # This would integrate with Polymarket API
            # For now, simulate price movement based on market hash for consistency
            import hashlib
            market_hash = hashlib.md5(market.encode()).hexdigest()
            hash_int = int(market_hash[:8], 16)
            base_price = 0.3 + (hash_int % 4000) / 10000.0  # Range 0.3-0.7
            
            # Add some time-based variation
            time_factor = (datetime.now().timestamp() % 3600) / 3600  # 0-1 over each hour
            variation = (time_factor - 0.5) * 0.1  # ±0.05 variation
            
            return max(0.05, min(0.95, base_price + variation))
        except Exception as e:
            print(f"Error getting current price for {market}: {e}")
            return None
    
    async def get_portfolio_summary(self) -> Dict[str, Any]:
        """Get portfolio performance summary"""
        try:
            # Get open positions
            open_positions = await db.fetchval("""
                SELECT COUNT(*) FROM copy_trades WHERE status = 'open'
            """)
            
            # Get total PnL
            total_pnl = await db.fetchval("""
                SELECT COALESCE(SUM(pnl), 0) FROM copy_trades WHERE status != 'open'
            """)
            
            # Get daily PnL
            daily_pnl = await db.fetchval("""
                SELECT COALESCE(SUM(pnl), 0) FROM copy_trades 
                WHERE status != 'open' AND created_at::date = CURRENT_DATE
            """)
            
            current_bankroll = await self.get_current_bankroll()
            total_exposure = await self.get_total_exposure()
            
            return {
                'total_value': current_bankroll,
                'open_positions': int(open_positions or 0),
                'total_pnl': float(total_pnl or 0),
                'daily_pnl': float(daily_pnl or 0),
                'total_exposure': total_exposure,
                'exposure_percent': (total_exposure / current_bankroll) * 100 if current_bankroll > 0 else 0
            }
            
        except Exception as e:
            print(f"Error getting portfolio summary: {e}")
            return {
                'total_value': 0,
                'open_positions': 0,
                'total_pnl': 0,
                'daily_pnl': 0,
                'total_exposure': 0,
                'exposure_percent': 0
            }


class MockClobClient:
    """Mock CLOB client for development/testing"""
    
    async def create_order(self, market: str, direction: str, price: float, size: float) -> Dict[str, Any]:
        """Simulate order creation"""
        # Simulate random success/failure
        import hashlib
        order_hash = hashlib.md5(f"{market}{direction}{price}{size}".encode()).hexdigest()
        success_rate = int(order_hash[:2], 16) / 255  # 0-1 based on hash
        
        if success_rate > 0.1:  # 90% success rate
            return {
                'success': True,
                'order_id': f"mock_order_{order_hash[:8]}",
                'message': 'Order placed successfully'
            }
        else:
            return {
                'success': False,
                'error': 'Insufficient liquidity',
                'message': 'Mock order failed'
            }
