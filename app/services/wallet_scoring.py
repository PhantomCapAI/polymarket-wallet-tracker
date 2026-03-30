import asyncio
import logging
from typing import Dict, List, Optional
from datetime import datetime, timedelta
from decimal import Decimal
import pandas as pd
import numpy as np

from app.models.database import db
from config.settings import settings

logger = logging.getLogger(__name__)

class WalletScoringService:
    """Service for calculating wallet trading signal scores"""
    
    def __init__(self):
        self.weights = {
            'consistency': settings.WEIGHT_CONSISTENCY,
            'timing': settings.WEIGHT_TIMING,
            'closing': settings.WEIGHT_CLOSING,
            'pnl': settings.WEIGHT_PNL,
            'win_rate': settings.WEIGHT_WIN_RATE,
            'diversity': settings.WEIGHT_DIVERSITY
        }
    
    async def calculate_wallet_score(self, wallet: str) -> Dict:
        """Calculate comprehensive signal score for a wallet"""
        try:
            # Get all trades for this wallet
            trades = await db.fetch("""
                SELECT * FROM trades_log 
                WHERE wallet = $1 
                ORDER BY entry_time
            """, wallet)
            
            if not trades:
                return {
                    'wallet': wallet,
                    'signal_score': 0.0,
                    'total_trades': 0,
                    'realized_pnl': 0.0,
                    'win_rate': 0.0,
                    'avg_position_size': 0.0,
                    'market_diversity': 0.0,
                    'timing_edge': 0.0,
                    'closing_efficiency': 0.0,
                    'consistency_score': 0.0,
                    'active_days': 0,
                    'last_trade_at': None
                }
            
            # Convert to DataFrame for easier analysis
            df = pd.DataFrame([dict(trade) for trade in trades])
            
            # Calculate individual components
            pnl_score = self._calculate_pnl_score(df)
            win_rate_score = self._calculate_win_rate_score(df)
            consistency_score = self._calculate_consistency_score(df)
            timing_score = self._calculate_timing_score(df)
            closing_score = self._calculate_closing_efficiency(df)
            diversity_score = self._calculate_market_diversity(df)
            
            # Calculate weighted final score
            signal_score = (
                self.weights['pnl'] * pnl_score +
                self.weights['win_rate'] * win_rate_score +
                self.weights['consistency'] * consistency_score +
                self.weights['timing'] * timing_score +
                self.weights['closing'] * closing_score +
                self.weights['diversity'] * diversity_score
            )
            
            # Calculate summary statistics
            realized_pnl = float(df[df['pnl'].notna()]['pnl'].sum())
            win_rate = len(df[(df['pnl'] > 0) & df['pnl'].notna()]) / len(df[df['pnl'].notna()]) if len(df[df['pnl'].notna()]) > 0 else 0
            avg_position_size = float(df['position_size'].mean())
            active_days = (df['entry_time'].max() - df['entry_time'].min()).days if len(df) > 1 else 1
            last_trade_at = df['entry_time'].max()
            
            return {
                'wallet': wallet,
                'signal_score': round(signal_score, 6),
                'realized_pnl': round(realized_pnl, 6),
                'win_rate': round(win_rate, 3),
                'avg_position_size': round(avg_position_size, 6),
                'market_diversity': round(diversity_score, 3),
                'timing_edge': round(timing_score, 3),
                'closing_efficiency': round(closing_score, 3),
                'consistency_score': round(consistency_score, 3),
                'total_trades': len(df),
                'active_days': max(active_days, 1),
                'last_trade_at': last_trade_at
            }
            
        except Exception as e:
            logger.error(f"Error calculating score for wallet {wallet}: {e}")
            raise
    
    def _calculate_pnl_score(self, df: pd.DataFrame) -> float:
        """Calculate normalized PnL performance score (0-1)"""
        if df.empty or df['pnl'].isna().all():
            return 0.0
        
        # Filter for completed trades with PnL
        completed = df[df['pnl'].notna()]
        if completed.empty:
            return 0.0
        
        # Calculate risk-adjusted returns
        total_pnl = completed['pnl'].sum()
        total_volume = (completed['position_size'] * completed['entry_price']).sum()
        
        if total_volume == 0:
            return 0.0
        
        roi = total_pnl / total_volume
        
        # Normalize ROI to 0-1 scale (sigmoid-like function)
        # Excellent performance (>50% ROI) gets close to 1.0
        pnl_score = 2 / (1 + np.exp(-roi * 10)) - 1
        return max(0.0, min(1.0, pnl_score))
    
    def _calculate_win_rate_score(self, df: pd.DataFrame) -> float:
        """Calculate win rate score (0-1)"""
        if df.empty or df['pnl'].isna().all():
            return 0.0
        
        completed = df[df['pnl'].notna()]
        if completed.empty:
            return 0.0
        
        win_rate = len(completed[completed['pnl'] > 0]) / len(completed)
        
        # Apply curve to reward high win rates more
        # 60%+ win rate gets exponentially higher scores
        if win_rate >= 0.6:
            return 0.6 + 0.4 * ((win_rate - 0.6) / 0.4) ** 0.5
        else:
            return win_rate
    
    def _calculate_consistency_score(self, df: pd.DataFrame) -> float:
        """Calculate trading consistency score based on PnL variance (0-1)"""
        if df.empty or df['pnl'].isna().all() or len(df[df['pnl'].notna()]) < 3:
            return 0.0
        
        completed = df[df['pnl'].notna()]
        pnl_series = completed['pnl']
        
        # Calculate coefficient of variation (lower is better)
        if pnl_series.mean() == 0:
            return 0.0
        
        cv = abs(pnl_series.std() / pnl_series.mean())
        
        # Convert to 0-1 score (lower CV = higher consistency)
        consistency = 1 / (1 + cv)
        return min(1.0, max(0.0, consistency))
    
    def _calculate_timing_score(self, df: pd.DataFrame) -> float:
        """Calculate market timing score based on entry efficiency (0-1)"""
        if df.empty or len(df) < 5:
            return 0.0
        
        # For timing, we'll look at how quickly positions are entered relative to price movements
        # This is a simplified calculation - in practice would need more market data
        
        # Check for quick decision making (faster entries might indicate better timing)
        df_sorted = df.sort_values('entry_time')
        if len(df_sorted) < 2:
            return 0.5  # Default neutral score
        
        # Calculate time gaps between trades
        time_diffs = df_sorted['entry_time'].diff().dt.total_seconds().dropna()
        
        # Reward consistent, frequent trading (but not too frequent)
        avg_gap_hours = time_diffs.mean() / 3600
        
        # Optimal range: 6-48 hours between trades
        if 6 <= avg_gap_hours <= 48:
            timing_score = 0.9
        elif avg_gap_hours < 6:
            timing_score = 0.6  # Too frequent
        elif avg_gap_hours > 168:  # > 1 week
            timing_score = 0.3  # Too infrequent
        else:
            timing_score = 0.7
        
        return timing_score
    
    def _calculate_closing_efficiency(self, df: pd.DataFrame) -> float:
        """Calculate how efficiently positions are closed (0-1)"""
        if df.empty:
            return 0.0
        
        # Check what percentage of trades have been closed
        total_trades = len(df)
        closed_trades = len(df[df['exit_price'].notna()])
        
        if total_trades == 0:
            return 0.0
        
        close_rate = closed_trades / total_trades
        
        # For closed trades with PnL, check if exits were profitable
        profitable_closes = len(df[(df['pnl'] > 0) & df['pnl'].notna()])
        
        if closed_trades > 0:
            profitable_rate = profitable_closes / closed_trades
            # Combine close rate and profitability
            efficiency = (close_rate * 0.6) + (profitable_rate * 0.4)
        else:
            efficiency = 0.0
        
        return min(1.0, efficiency)
    
    def _calculate_market_diversity(self, df: pd.DataFrame) -> float:
        """Calculate market diversification score (0-1)"""
        if df.empty or df['market'].isna().all():
            return 0.0
        
        # Count unique markets traded
        unique_markets = df['market'].dropna().nunique()
        total_trades = len(df)
        
        if total_trades == 0:
            return 0.0
        
        # Reward trading across multiple markets, but with diminishing returns
        if unique_markets == 1:
            return 0.3
        elif unique_markets <= 3:
            return 0.6
        elif unique_markets <= 5:
            return 0.8
        else:
            return 1.0
    
    async def score_all_wallets(self) -> int:
        """Score all wallets that have trades"""
        try:
            # Get all unique wallets with trades
            wallets = await db.fetch("""
                SELECT DISTINCT wallet 
                FROM trades_log 
                WHERE wallet IS NOT NULL
            """)
            
            scored_count = 0
            
            for wallet_row in wallets:
                wallet = wallet_row['wallet']
                try:
                    score_data = await self.calculate_wallet_score(wallet)
                    
                    # Only update if wallet has minimum trades
                    if score_data['total_trades'] >= settings.MIN_TRADES:
                        await db.execute("""
                            INSERT INTO wallets_master 
                            (wallet, signal_score, realized_pnl, win_rate, avg_position_size,
                             market_diversity, timing_edge, closing_efficiency, consistency_score,
                             total_trades, active_days, last_trade_at, last_updated)
                            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, NOW())
                            ON CONFLICT (wallet) DO UPDATE SET
                                signal_score = EXCLUDED.signal_score,
                                realized_pnl = EXCLUDED.realized_pnl,
                                win_rate = EXCLUDED.win_rate,
                                avg_position_size = EXCLUDED.avg_position_size,
                                market_diversity = EXCLUDED.market_diversity,
                                timing_edge = EXCLUDED.timing_edge,
                                closing_efficiency = EXCLUDED.closing_efficiency,
                                consistency_score = EXCLUDED.consistency_score,
                                total_trades = EXCLUDED.total_trades,
                                active_days = EXCLUDED.active_days,
                                last_trade_at = EXCLUDED.last_trade_at,
                                last_updated = NOW()
                        """, 
                            score_data['wallet'],
                            score_data['signal_score'],
                            score_data['realized_pnl'],
                            score_data['win_rate'],
                            score_data['avg_position_size'],
                            score_data['market_diversity'],
                            score_data['timing_edge'],
                            score_data['closing_efficiency'],
                            score_data['consistency_score'],
                            score_data['total_trades'],
                            score_data['active_days'],
                            score_data['last_trade_at']
                        )
                        
                        scored_count += 1
                        
                        if scored_count % 100 == 0:
                            logger.info(f"Scored {scored_count} wallets...")
                
                except Exception as e:
                    logger.error(f"Error scoring wallet {wallet}: {e}")
                    continue
            
            logger.info(f"Successfully scored {scored_count} wallets")
            return scored_count
            
        except Exception as e:
            logger.error(f"Error in score_all_wallets: {e}")
            raise
