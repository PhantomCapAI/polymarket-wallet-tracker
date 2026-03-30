import logging
import math
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, field

from app.models.database import db
from config.settings import settings

logger = logging.getLogger(__name__)


@dataclass
class BacktestTrade:
    """A single simulated trade."""
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
class BacktestResult:
    """Aggregated results from a backtest run."""
    start_date: datetime
    end_date: datetime
    total_trades: int
    winning_trades: int
    losing_trades: int
    total_pnl: float
    net_pnl: float
    total_fees: float
    win_rate: float
    avg_win: float
    avg_loss: float
    max_drawdown: float
    sharpe_ratio: float
    sortino_ratio: float
    max_consecutive_losses: int
    trades: List[BacktestTrade] = field(default_factory=list)
    daily_pnl: Dict[str, float] = field(default_factory=dict)


class BacktestService:
    TRANSACTION_COST = 0.001  # 0.1 %

    async def run_backtest(
        self,
        start_date: datetime,
        end_date: datetime,
        min_signal_score: float = 0.6,
    ) -> BacktestResult:
        """
        Full backtest pipeline:
        1. Pull historical trades from trades_log in [start_date, end_date]
        2. Reconstruct wallet scores at each point in time
        3. Simulate copy trades using signal scoring + risk management
        4. Calculate performance metrics
        5. Persist results to backtest_results
        """
        backtest_id = None
        try:
            # Reserve a row so the API can poll status
            backtest_id = await db.fetchval("""
                INSERT INTO backtest_results
                    (days_back, min_signal_score, status, created_at)
                VALUES ($1, $2, 'running', NOW())
                RETURNING id
            """, (end_date - start_date).days, min_signal_score)

            # 1. historical trades (only completed ones with exit data)
            historical = await db.fetch("""
                SELECT t.id, t.wallet, t.market, t.direction,
                       t.entry_price, t.exit_price, t.position_size,
                       t.pnl, t.entry_time, t.exit_time
                FROM trades_log t
                WHERE t.entry_time >= $1
                  AND t.entry_time <= $2
                  AND t.exit_time IS NOT NULL
                  AND t.pnl IS NOT NULL
                ORDER BY t.entry_time
            """, start_date, end_date)

            # 2. wallet scores snapshot (use current scores as baseline)
            wallet_scores = await self._load_wallet_scores()

            # 3. simulate copy trades
            simulated: List[BacktestTrade] = []
            balance = settings.BANKROLL
            peak_balance = balance
            open_markets: Dict[str, datetime] = {}  # market -> expected exit
            consecutive_losses = 0
            max_consecutive_losses = 0
            circuit_breaker_until: Optional[datetime] = None
            daily_pnl_tracker: Dict[str, float] = {}

            for row in historical:
                wallet = row['wallet']
                score = wallet_scores.get(wallet, 0.0)
                if score < min_signal_score:
                    continue

                entry_time: datetime = row['entry_time']
                exit_time: datetime = row['exit_time']

                # Circuit breaker check
                if circuit_breaker_until and entry_time < circuit_breaker_until:
                    continue

                # Daily loss limit check
                day_key = entry_time.strftime('%Y-%m-%d')
                day_pnl = daily_pnl_tracker.get(day_key, 0.0)
                if day_pnl <= -(settings.DAILY_LOSS_LIMIT * balance):
                    continue

                # Max concurrent positions
                open_markets = {
                    m: et for m, et in open_markets.items() if et > entry_time
                }
                if len(open_markets) >= settings.MAX_CONCURRENT_POSITIONS:
                    continue

                # No duplicate market
                if row['market'] in open_markets:
                    continue

                # Position sizing (mirrors live logic)
                if score >= settings.HIGH_CONFIDENCE_THRESHOLD:
                    raw_size = balance * settings.HIGH_CONFIDENCE_SIZE
                elif score >= settings.MEDIUM_CONFIDENCE_THRESHOLD:
                    raw_size = balance * settings.MEDIUM_CONFIDENCE_SIZE
                else:
                    continue

                position_size = min(raw_size, settings.MAX_POSITION_SIZE)
                if position_size <= 0 or position_size > balance * settings.MAX_BANKROLL_PER_MARKET:
                    continue

                entry_price = float(row['entry_price'])
                exit_price = float(row['exit_price'])
                original_size = float(row['position_size'])

                # Scale P&L proportionally
                if original_size > 0:
                    ratio = position_size / original_size
                    raw_pnl = float(row['pnl']) * ratio
                else:
                    raw_pnl = 0.0

                # Stop-loss simulation
                stop_threshold = position_size * settings.STOP_LOSS_PERCENTAGE
                if raw_pnl < -stop_threshold:
                    raw_pnl = -stop_threshold

                fees = position_size * self.TRANSACTION_COST
                net_pnl = raw_pnl - fees

                trade = BacktestTrade(
                    timestamp=entry_time,
                    wallet=wallet,
                    market=row['market'],
                    direction=row['direction'] or 'buy',
                    entry_price=entry_price,
                    exit_price=exit_price,
                    position_size=position_size,
                    signal_score=score,
                    pnl=net_pnl,
                    fees=fees,
                )
                simulated.append(trade)

                balance += net_pnl
                daily_pnl_tracker[day_key] = daily_pnl_tracker.get(day_key, 0.0) + net_pnl
                open_markets[row['market']] = exit_time

                # Consecutive loss / circuit breaker
                if net_pnl < 0:
                    consecutive_losses += 1
                    max_consecutive_losses = max(max_consecutive_losses, consecutive_losses)
                    if consecutive_losses >= settings.CIRCUIT_BREAKER_LOSSES:
                        circuit_breaker_until = entry_time + timedelta(
                            hours=settings.CIRCUIT_BREAKER_HOURS
                        )
                        consecutive_losses = 0
                else:
                    consecutive_losses = 0

            # 4. build daily P&L dict for all calendar days
            daily_pnl = self._build_daily_pnl(simulated, start_date, end_date)
            daily_returns = list(daily_pnl.values())

            # 5. metrics
            total_trades = len(simulated)
            winning = [t for t in simulated if t.pnl > 0]
            losing = [t for t in simulated if t.pnl < 0]
            total_pnl = sum(t.pnl for t in simulated)
            total_fees = sum(t.fees for t in simulated)
            win_rate = len(winning) / total_trades if total_trades else 0.0
            avg_win = (sum(t.pnl for t in winning) / len(winning)) if winning else 0.0
            avg_loss = (sum(t.pnl for t in losing) / len(losing)) if losing else 0.0
            max_dd = await self._calculate_max_drawdown(
                [t.pnl for t in simulated]
            )
            sharpe = await self._calculate_sharpe_ratio(daily_returns)
            sortino = await self._calculate_sortino_ratio(daily_returns)

            result = BacktestResult(
                start_date=start_date,
                end_date=end_date,
                total_trades=total_trades,
                winning_trades=len(winning),
                losing_trades=len(losing),
                total_pnl=total_pnl,
                net_pnl=total_pnl,
                total_fees=total_fees,
                win_rate=win_rate,
                avg_win=avg_win,
                avg_loss=avg_loss,
                max_drawdown=max_dd,
                sharpe_ratio=sharpe,
                sortino_ratio=sortino,
                max_consecutive_losses=max_consecutive_losses,
                trades=simulated,
                daily_pnl=daily_pnl,
            )

            # 6. persist
            await db.execute("""
                UPDATE backtest_results
                SET status = 'completed',
                    total_pnl = $1,
                    win_rate = $2,
                    total_trades = $3,
                    completed_at = NOW()
                WHERE id = $4
            """, result.total_pnl, result.win_rate, result.total_trades, backtest_id)

            logger.info(
                f"Backtest done: {total_trades} trades, "
                f"PnL=${total_pnl:,.2f}, WR={win_rate:.2%}, "
                f"DD={max_dd:.2%}, Sharpe={sharpe:.2f}"
            )
            return result

        except Exception as e:
            logger.error(f"Backtest error: {e}")
            if backtest_id:
                await db.execute("""
                    UPDATE backtest_results
                    SET status = 'failed', error = $1, completed_at = NOW()
                    WHERE id = $2
                """, str(e), backtest_id)
            raise

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    async def _load_wallet_scores(self) -> Dict[str, float]:
        """Load current wallet signal scores as the baseline for replay."""
        rows = await db.fetch("SELECT wallet, signal_score FROM wallets_master")
        return {r['wallet']: float(r['signal_score']) for r in rows}

    def _build_daily_pnl(
        self,
        trades: List[BacktestTrade],
        start_date: datetime,
        end_date: datetime,
    ) -> Dict[str, float]:
        daily: Dict[str, float] = {}
        d = start_date.date() if isinstance(start_date, datetime) else start_date
        end = end_date.date() if isinstance(end_date, datetime) else end_date
        while d <= end:
            daily[d.isoformat()] = 0.0
            d += timedelta(days=1)
        for t in trades:
            key = t.timestamp.date().isoformat()
            if key in daily:
                daily[key] += t.pnl
        return daily

    async def _calculate_max_drawdown(self, pnl_series: List[float]) -> float:
        """Peak-to-trough drawdown as a fraction of peak balance."""
        if not pnl_series:
            return 0.0
        balance = settings.BANKROLL
        peak = balance
        max_dd = 0.0
        for pnl in pnl_series:
            balance += pnl
            if balance > peak:
                peak = balance
            dd = (peak - balance) / peak if peak > 0 else 0.0
            if dd > max_dd:
                max_dd = dd
        return max_dd

    async def _calculate_sharpe_ratio(
        self, daily_returns: List[float], risk_free_rate: float = 0.04
    ) -> float:
        """Annualised Sharpe ratio (252 trading days)."""
        if len(daily_returns) < 2:
            return 0.0
        n = len(daily_returns)
        mean_r = sum(daily_returns) / n
        daily_rf = risk_free_rate / 252
        excess = [r - daily_rf for r in daily_returns]
        mean_excess = sum(excess) / n
        variance = sum((x - mean_excess) ** 2 for x in excess) / (n - 1)
        std = math.sqrt(variance) if variance > 0 else 0.0
        if std == 0:
            return 0.0
        return (mean_excess / std) * math.sqrt(252)

    async def _calculate_sortino_ratio(self, daily_returns: List[float]) -> float:
        """Annualised Sortino ratio (downside deviation only)."""
        if len(daily_returns) < 2:
            return 0.0
        n = len(daily_returns)
        mean_r = sum(daily_returns) / n
        neg = [r for r in daily_returns if r < 0]
        if not neg:
            return float('inf') if mean_r > 0 else 0.0
        downside_var = sum(r ** 2 for r in neg) / len(neg)
        downside_std = math.sqrt(downside_var) if downside_var > 0 else 0.0
        if downside_std == 0:
            return 0.0
        return (mean_r / downside_std) * math.sqrt(252)


# Global instance
backtest_service = BacktestService()
