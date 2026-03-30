import asyncio
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, and_
from dataclasses import dataclass

from app.models.wallet import WalletMaster
from app.models.trade import TradesLog
from app.services.wallet_scoring import wallet_scoring_service
import logging

logger = logging.getLogger(__name__)

@dataclass
class BacktestTrade:
    """Represents a trade in the backtest"""
    timestamp: datetime
    wallet: str
    market: str
    direction: str
    entry_price: float
    exit_price: float
    position_size: float
    signal_score: float
    pnl: float
    fees: float = 0.0

@dataclass
class BacktestResults:
    """Results from a backtest run"""
    start_date: datetime
    end_date: datetime
    total_trades: int
    winning_trades: int
    losing_trades: int
    total_pnl: float
    total_fees: float
    net_pnl: float
    win_rate: float
    avg_win: float
    avg_loss: float
    max_drawdown: float
    sharpe_ratio: float
    sortino_ratio: float
    max_consecutive_losses: int
    trades: List[BacktestTrade]
    daily_pnl: Dict[str, float]

class BacktestingService:
    def __init__(self):
        self.transaction_cost = 0.001  # 0.1% transaction cost
        self.initial_balance = 10000.0
        self.max_position_size = 1000.0
        self.min_signal_score = 0.6
    
    async def run_backtest(
        self,
        db: AsyncSession,
        start_date: datetime,
        end_date: datetime,
        parameters: Optional[Dict[str, Any]] = None
    ) -> BacktestResults:
        """Run complete backtest simulation"""
        try:
            logger.info(f"Starting backtest from {start_date} to {end_date}")
            
            # Override default parameters if provided
            if parameters:
                self.min_signal_score = parameters.get('min_signal_score', self.min_signal_score)
                self.max_position_size = parameters.get('max_position_size', self.max_position_size)
                self.initial_balance = parameters.get('initial_balance', self.initial_balance)
            
            # Get historical wallet data for the period
            wallet_data = await self._get_historical_wallet_data(db, start_date, end_date)
            
            # Get all trades in the period
            all_trades = await self._get_historical_trades(db, start_date, end_date)
            
            # Simulate copy trading strategy
            simulated_trades = await self._simulate_copy_trading(
                wallet_data, all_trades, start_date, end_date
            )
            
            # Calculate performance metrics
            results = self._calculate_performance_metrics(
                simulated_trades, start_date, end_date
            )
            
            logger.info(f"Backtest completed: {results.total_trades} trades, "
                       f"Net P&L: ${results.net_pnl:.2f}, Win Rate: {results.win_rate:.2%}")
            
            return results
            
        except Exception as e:
            logger.error(f"Error running backtest: {e}")
            raise
    
    async def _get_historical_wallet_data(
        self,
        db: AsyncSession,
        start_date: datetime,
        end_date: datetime
    ) -> Dict[str, List[Dict[str, Any]]]:
        """Get historical wallet performance data"""
        try:
            # In a full implementation, this would reconstruct historical
            # wallet scores at different points in time
            
            # For this simulation, we'll use current wallet data
            # and simulate how scores might have evolved
            
            wallets_query = select(WalletMaster).where(
                and_(
                    WalletMaster.total_trades >= 50,
                    WalletMaster.active_days >= 30,
                    WalletMaster.last_trade_at <= end_date
                )
            )
            
            wallets_result = await db.execute(wallets_query)
            wallets = wallets_result.scalars().all()
            
            wallet_data = {}
            
            for wallet in wallets:
                # Simulate historical score progression
                wallet_data[wallet.wallet] = self._simulate_score_history(
                    wallet, start_date, end_date
                )
            
            return wallet_data
            
        except Exception as e:
            logger.error(f"Error getting historical wallet data: {e}")
            return {}
    
    def _simulate_score_history(
        self,
        wallet: WalletMaster,
        start_date: datetime,
        end_date: datetime
    ) -> List[Dict[str, Any]]:
        """Simulate how a wallet's score evolved over time"""
        history = []
        current_date = start_date
        base_score = float(wallet.signal_score)
        
        while current_date <= end_date:
            # Add some realistic variation to the score
            score_variation = np.random.normal(0, 0.1)  # Small random variation
            simulated_score = max(0, min(1, base_score + score_variation))
            
            history.append({
                'date': current_date,
                'signal_score': simulated_score,
                'win_rate': float(wallet.win_rate),
                'realized_pnl': float(wallet.realized_pnl),
                'consistency_score': float(wallet.consistency_score)
            })
            
            current_date += timedelta(days=1)
        
        return history
    
    async def _get_historical_trades(
        self,
        db: AsyncSession,
        start_date: datetime,
        end_date: datetime
    ) -> List[TradesLog]:
        """Get all trades in the historical period"""
        try:
            trades_query = select(TradesLog).where(
                and_(
                    TradesLog.entry_time >= start_date,
                    TradesLog.entry_time <= end_date,
                    TradesLog.exit_time.isnot(None),
                    TradesLog.pnl.isnot(None)
                )
            ).order_by(TradesLog.entry_time)
            
            trades_result = await db.execute(trades_query)
            return trades_result.scalars().all()
            
        except Exception as e:
            logger.error(f"Error getting historical trades: {e}")
            return []
    
    async def _simulate_copy_trading(
        self,
        wallet_data: Dict[str, List[Dict[str, Any]]],
        all_trades: List[TradesLog],
        start_date: datetime,
        end_date: datetime
    ) -> List[BacktestTrade]:
        """Simulate the copy trading strategy"""
        simulated_trades = []
        current_balance = self.initial_balance
        open_positions = {}
        
        # Sort trades chronologically
        sorted_trades = sorted(all_trades, key=lambda x: x.entry_time)
        
        for trade in sorted_trades:
            try:
                # Get wallet signal score at the time of trade
                wallet_score = self._get_wallet_score_at_time(
                    wallet_data, trade.wallet, trade.entry_time
                )
                
                if wallet_score < self.min_signal_score:
                    continue
                
                # Check if we should copy this trade
                if await self._should_copy_trade(
                    trade, wallet_score, current_balance, open_positions
                ):
                    # Calculate position size based on signal score
                    position_size = self._calculate_backtest_position_size(
                        wallet_score, current_balance
                    )
                    
                    if position_size > 0:
                        # Create simulated trade
                        entry_price = float(trade.entry_price)
                        exit_price = float(trade.exit_price) if trade.exit_price else entry_price
                        
                        # Calculate P&L
                        if trade.pnl and trade.pnl != 0:
                            # Scale P&L by our position size vs original
                            original_size = abs(float(trade.position_size))
                            size_ratio = position_size / original_size if original_size > 0 else 1
                            scaled_pnl = float(trade.pnl) * size_ratio
                        else:
                            scaled_pnl = 0
                        
                        # Apply transaction costs
                        fees = position_size * self.transaction_cost
                        net_pnl = scaled_pnl - fees
                        
                        simulated_trade = BacktestTrade(
                            timestamp=trade.entry_time,
                            wallet=trade.wallet,
                            market=trade.market,
                            direction='buy',  # Simplified for backtest
                            entry_price=entry_price,
                            exit_price=exit_price,
                            position_size=position_size,
                            signal_score=wallet_score,
                            pnl=net_pnl,
                            fees=fees
                        )
                        
                        simulated_trades.append(simulated_trade)
                        current_balance += net_pnl
                        
                        # Track position
                        position_key = f"{trade.market}_{trade.wallet}"
                        open_positions[position_key] = trade.exit_time
            
            except Exception as e:
                logger.error(f"Error simulating trade {trade.id}: {e}")
                continue
        
        return simulated_trades
    
    def _get_wallet_score_at_time(
        self,
        wallet_data: Dict[str, List[Dict[str, Any]]],
        wallet: str,
        timestamp: datetime
    ) -> float:
        """Get wallet signal score at specific timestamp"""
        if wallet not in wallet_data:
            return 0.0
        
        wallet_history = wallet_data[wallet]
        
        # Find the score closest to the timestamp
        for i, data_point in enumerate(wallet_history):
            if data_point['date'] >= timestamp.date():
                return data_point['signal_score']
        
        # If no exact match, return the last available score
        return wallet_history[-1]['signal_score'] if wallet_history else 0.0
    
    async def _should_copy_trade(
        self,
        trade: TradesLog,
        wallet_score: float,
        current_balance: float,
        open_positions: Dict[str, datetime]
    ) -> bool:
        """Determine if we should copy a specific trade"""
        # Check signal score threshold
        if wallet_score < self.min_signal_score:
            return False
        
        # Check if we have enough balance
        required_size = self._calculate_backtest_position_size(wallet_score, current_balance)
        if required_size > current_balance * 0.1:  # Max 10% per trade
            return False
        
        # Check if we already have position in this market
        position_key = f"{trade.market}_{trade.wallet}"
        if position_key in open_positions:
            return False
        
        # Check maximum concurrent positions
        active_positions = sum(
            1 for exit_time in open_positions.values()
            if exit_time > trade.entry_time
        )
        if active_positions >= 10:  # Max 10 concurrent positions
            return False
        
        return True
    
    def _calculate_backtest_position_size(
        self,
        signal_score: float,
        current_balance: float
    ) -> float:
        """Calculate position size for backtest"""
        if signal_score >= 0.8:
            return min(current_balance * 0.05, self.max_position_size)  # 5% for high confidence
        elif signal_score >= 0.6:
            return min(current_balance * 0.02, self.max_position_size)  # 2% for medium confidence
        else:
            return 0.0
    
    def _calculate_performance_metrics(
        self,
        trades: List[BacktestTrade],
        start_date: datetime,
        end_date: datetime
    ) -> BacktestResults:
        """Calculate comprehensive performance metrics"""
        if not trades:
            return BacktestResults(
                start_date=start_date,
                end_date=end_date,
                total_trades=0,
                winning_trades=0,
                losing_trades=0,
                total_pnl=0.0,
                total_fees=0.0,
                net_pnl=0.0,
                win_rate=0.0,
                avg_win=0.0,
                avg_loss=0.0,
                max_drawdown=0.0,
                sharpe_ratio=0.0,
                sortino_ratio=0.0,
                max_consecutive_losses=0,
                trades=trades,
                daily_pnl={}
            )
        
        # Basic metrics
        total_trades = len(trades)
        winning_trades = sum(1 for t in trades if t.pnl > 0)
        losing_trades = sum(1 for t in trades if t.pnl < 0)
        total_pnl = sum(t.pnl for t in trades)
        total_fees = sum(t.fees for t in trades)
        net_pnl = total_pnl
        win_rate = winning_trades / total_trades if total_trades > 0 else 0
        
        # Win/Loss averages
        winning_pnls = [t.pnl for t in trades if t.pnl > 0]
        losing_pnls = [t.pnl for t in trades if t.pnl < 0]
        avg_win = np.mean(winning_pnls) if winning_pnls else 0
        avg_loss = np.mean(losing_pnls) if losing_pnls else 0
        
        # Drawdown calculation
        max_drawdown = self._calculate_max_drawdown(trades)
        
        # Risk-adjusted returns
        daily_pnl = self._calculate_daily_pnl(trades, start_date, end_date)
        sharpe_ratio = self._calculate_sharpe_ratio(daily_pnl)
        sortino_ratio = self._calculate_sortino_ratio(daily_pnl)
        
        # Consecutive losses
        max_consecutive_losses = self._calculate_max_consecutive_losses(trades)
        
        return BacktestResults(
            start_date=start_date,
            end_date=end_date,
            total_trades=total_trades,
            winning_trades=winning_trades,
            losing_trades=losing_trades,
            total_pnl=total_pnl,
            total_fees=total_fees,
            net_pnl=net_pnl,
            win_rate=win_rate,
            avg_win=avg_win,
            avg_loss=avg_loss,
            max_drawdown=max_drawdown,
            sharpe_ratio=sharpe_ratio,
            sortino_ratio=sortino_ratio,
            max_consecutive_losses=max_consecutive_losses,
            trades=trades,
            daily_pnl=daily_pnl
        )
    
    def _calculate_max_drawdown(self, trades: List[BacktestTrade]) -> float:
        """Calculate maximum drawdown"""
        if not trades:
            return 0.0
        
        balance = self.initial_balance
        peak_balance = balance
        max_drawdown = 0.0
        
        for trade in sorted(trades, key=lambda x: x.timestamp):
            balance += trade.pnl
            if balance > peak_balance:
                peak_balance = balance
            
            current_drawdown = (peak_balance - balance) / peak_balance if peak_balance > 0 else 0
            max_drawdown = max(max_drawdown, current_drawdown)
        
        return max_drawdown
    
    def _calculate_daily_pnl(
        self,
        trades: List[BacktestTrade],
        start_date: datetime,
        end_date: datetime
    ) -> Dict[str, float]:
        """Calculate daily P&L"""
        daily_pnl = {}
        current_date = start_date.date()
        end_date = end_date.date()
        
        while current_date <= end_date:
            daily_pnl[current_date.isoformat()] = 0.0
            current_date += timedelta(days=1)
        
        for trade in trades:
            trade_date = trade.timestamp.date().isoformat()
            if trade_date in daily_pnl:
                daily_pnl[trade_date] += trade.pnl
        
        return daily_pnl
    
    def _calculate_sharpe_ratio(self, daily_pnl: Dict[str, float]) -> float:
        """Calculate Sharpe ratio"""
        if not daily_pnl:
            return 0.0
        
        returns = list(daily_pnl.values())
        if not returns or len(returns) < 2:
            return 0.0
        
        mean_return = np.mean(returns)
        std_return = np.std(returns)
        
        if std_return == 0:
            return 0.0
        
        # Annualized Sharpe ratio (assuming 252 trading days)
        return (mean_return / std_return) * np.sqrt(252)
    
    def _calculate_sortino_ratio(self, daily_pnl: Dict[str, float]) -> float:
        """Calculate Sortino ratio"""
        if not daily_pnl:
            return 0.0
        
        returns = list(daily_pnl.values())
        if not returns or len(returns) < 2:
            return 0.0
        
        mean_return = np.mean(returns)
        negative_returns = [r for r in returns if r < 0]
        
        if not negative_returns:
            return float('inf') if mean_return > 0 else 0.0
        
        downside_deviation = np.std(negative_returns)
        
        if downside_deviation == 0:
            return 0.0
        
        # Annualized Sortino ratio
        return (mean_return / downside_deviation) * np.sqrt(252)
    
    def _calculate_max_consecutive_losses(self, trades: List[BacktestTrade]) -> int:
        """Calculate maximum consecutive losses"""
        if not trades:
            return 0
        
        max_consecutive = 0
        current_consecutive = 0
        
        for trade in sorted(trades, key=lambda x: x.timestamp):
            if trade.pnl < 0:
                current_consecutive += 1
                max_consecutive = max(max_consecutive, current_consecutive)
            else:
                current_consecutive = 0
        
        return max_consecutive

# Global backtesting service instance
backtesting_service = BacktestingService()
