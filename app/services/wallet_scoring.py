import logging
import math
from typing import Dict, List, Optional
from datetime import datetime, timedelta

import numpy as np

from app.models.database import db

logger = logging.getLogger(__name__)

# Weights for signal score composition
WEIGHTS = {
    'consistency': 0.30,
    'timing': 0.25,
    'closing': 0.15,
    'pnl': 0.12,
    'win_rate': 0.10,
    'diversity': 0.08,
}

# Wallet eligibility thresholds
MIN_TRADES = 50
MIN_ACTIVE_DAYS = 30
MIN_DISTINCT_MARKETS = 3


class WalletScoringService:
    """Scores wallets based on real trade data from trades_log.

    All raw metrics are z-score normalized across the full wallet population
    before the weighted signal score is computed.
    """

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def calculate_wallet_score(self, wallet_address: str) -> Dict:
        """Calculate all 7 metrics for a single wallet and return a dict.

        Returns raw (un-normalized) metric values plus a signal_score of 0.0
        when the wallet is scored in isolation. Population-level z-score
        normalization only happens inside ``score_all_wallets``.
        """
        metrics = await self._compute_raw_metrics(wallet_address)
        if metrics is None:
            return self._empty_result(wallet_address)

        # When scoring a single wallet in isolation we cannot z-score
        # normalize (no population). Return raw metrics with signal_score=0.
        return {
            'wallet': wallet_address,
            'signal_score': 0.0,
            'realized_pnl': metrics['realized_pnl'],
            'win_rate': metrics['win_rate'],
            'avg_position_size': metrics['avg_position_size'],
            'market_diversity': metrics['market_diversity_raw'],
            'timing_edge': metrics['timing_edge_raw'],
            'closing_efficiency': metrics['closing_efficiency_raw'],
            'consistency_score': metrics['consistency_raw'],
            'total_trades': metrics['total_trades'],
            'active_days': metrics['active_days'],
            'distinct_markets': metrics['distinct_markets'],
            'last_trade_at': metrics['last_trade_at'],
        }

    async def score_all_wallets(self) -> List[Dict]:
        """Score every eligible wallet with z-score normalization.

        Returns a list of scored wallet dicts ordered by signal_score desc.
        Also persists results to wallets_master.
        """
        eligible = await self._get_eligible_wallets()
        if not eligible:
            logger.info("No eligible wallets found for scoring")
            return []

        # Phase 1 -- compute raw metrics for every eligible wallet
        raw_rows: List[Dict] = []
        for row in eligible:
            wallet = row['wallet']
            try:
                metrics = await self._compute_raw_metrics(wallet)
                if metrics is not None:
                    raw_rows.append(metrics)
            except Exception:
                logger.exception("Failed to compute metrics for %s", wallet)

        if not raw_rows:
            return []

        # Phase 2 -- z-score normalize the 6 component metrics
        metric_keys = [
            'consistency_raw', 'timing_edge_raw', 'closing_efficiency_raw',
            'pnl_score_raw', 'win_rate_raw', 'diversity_score_raw',
        ]
        weight_keys = [
            'consistency', 'timing', 'closing', 'pnl', 'win_rate', 'diversity',
        ]

        # Build arrays
        arrays = {}
        for key in metric_keys:
            arrays[key] = np.array([r[key] for r in raw_rows], dtype=np.float64)

        means = {k: np.mean(arrays[k]) for k in metric_keys}
        stds = {k: np.std(arrays[k]) for k in metric_keys}

        results: List[Dict] = []
        for idx, raw in enumerate(raw_rows):
            z_scores = {}
            for mk, wk in zip(metric_keys, weight_keys):
                std = stds[mk]
                if std > 0:
                    z_scores[wk] = (raw[mk] - means[mk]) / std
                else:
                    z_scores[wk] = 0.0

            signal_score = sum(WEIGHTS[wk] * z_scores[wk] for wk in weight_keys)

            results.append({
                'wallet': raw['wallet'],
                'signal_score': round(float(signal_score), 6),
                'realized_pnl': raw['realized_pnl'],
                'win_rate': round(raw['win_rate'], 4),
                'avg_position_size': round(raw['avg_position_size'], 6),
                'market_diversity': round(raw['market_diversity_raw'], 4),
                'timing_edge': round(raw['timing_edge_raw'], 4),
                'closing_efficiency': round(raw['closing_efficiency_raw'], 4),
                'consistency_score': round(raw['consistency_raw'], 4),
                'total_trades': raw['total_trades'],
                'active_days': raw['active_days'],
                'distinct_markets': raw['distinct_markets'],
                'last_trade_at': raw['last_trade_at'],
            })

        results.sort(key=lambda r: r['signal_score'], reverse=True)

        # Phase 3 -- persist to wallets_master
        for r in results:
            try:
                await db.execute("""
                    INSERT INTO wallets_master
                        (wallet, signal_score, realized_pnl, win_rate,
                         avg_position_size, market_diversity, timing_edge,
                         closing_efficiency, consistency_score,
                         total_trades, active_days, last_trade_at, last_updated)
                    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12, NOW())
                    ON CONFLICT (wallet) DO UPDATE SET
                        signal_score       = EXCLUDED.signal_score,
                        realized_pnl       = EXCLUDED.realized_pnl,
                        win_rate           = EXCLUDED.win_rate,
                        avg_position_size  = EXCLUDED.avg_position_size,
                        market_diversity   = EXCLUDED.market_diversity,
                        timing_edge        = EXCLUDED.timing_edge,
                        closing_efficiency = EXCLUDED.closing_efficiency,
                        consistency_score  = EXCLUDED.consistency_score,
                        total_trades       = EXCLUDED.total_trades,
                        active_days        = EXCLUDED.active_days,
                        last_trade_at      = EXCLUDED.last_trade_at,
                        last_updated       = NOW()
                """,
                    r['wallet'], r['signal_score'], r['realized_pnl'],
                    r['win_rate'], r['avg_position_size'], r['market_diversity'],
                    r['timing_edge'], r['closing_efficiency'], r['consistency_score'],
                    r['total_trades'], r['active_days'], r['last_trade_at'],
                )
            except Exception:
                logger.exception("Failed to persist score for %s", r['wallet'])

        logger.info("Scored %d eligible wallets", len(results))
        return results

    async def get_leaderboard(self, limit: int = 50) -> List[Dict]:
        """Return the top wallets by signal_score from wallets_master."""
        rows = await db.fetch("""
            SELECT wallet, signal_score, realized_pnl, win_rate,
                   avg_position_size, market_diversity, timing_edge,
                   closing_efficiency, consistency_score,
                   total_trades, active_days, last_trade_at, last_updated
            FROM wallets_master
            ORDER BY signal_score DESC
            LIMIT $1
        """, limit)

        return [dict(r) for r in rows]

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    async def _get_eligible_wallets(self) -> list:
        """Return wallets meeting minimum activity thresholds."""
        return await db.fetch("""
            SELECT wallet
            FROM trades_log
            WHERE wallet IS NOT NULL
            GROUP BY wallet
            HAVING COUNT(*) >= $1
               AND (MAX(entry_time) - MIN(entry_time)) >= make_interval(days => $2)
               AND COUNT(DISTINCT market) >= $3
        """, MIN_TRADES, MIN_ACTIVE_DAYS, MIN_DISTINCT_MARKETS)

    async def _compute_raw_metrics(self, wallet: str) -> Optional[Dict]:
        """Compute all 7 raw metrics for *wallet* from trades_log.

        Returns None when the wallet has no trades.
        """
        # ---- summary stats in a single query ----
        summary = await db.fetchrow("""
            SELECT
                COUNT(*)                                          AS total_trades,
                COUNT(DISTINCT market)                            AS distinct_markets,
                AVG(position_size)                                AS avg_position_size,
                MAX(entry_time)                                   AS last_trade_at,
                EXTRACT(DAY FROM MAX(entry_time) - MIN(entry_time))::int AS active_days
            FROM trades_log
            WHERE wallet = $1
        """, wallet)

        if summary is None or summary['total_trades'] == 0:
            return None

        total_trades = summary['total_trades']
        distinct_markets = summary['distinct_markets']
        avg_position_size = float(summary['avg_position_size'] or 0)
        active_days = max(int(summary['active_days'] or 0), 1)
        last_trade_at = summary['last_trade_at']

        # ---- 1. Realized PnL ----
        realized_row = await db.fetchrow("""
            SELECT COALESCE(SUM(pnl), 0) AS realized_pnl
            FROM trades_log
            WHERE wallet = $1 AND exit_price IS NOT NULL
        """, wallet)
        realized_pnl = float(realized_row['realized_pnl'])

        # Normalized PnL score: sigmoid of ROI
        volume_row = await db.fetchrow("""
            SELECT COALESCE(SUM(position_size * entry_price), 0) AS total_volume
            FROM trades_log
            WHERE wallet = $1 AND exit_price IS NOT NULL
        """, wallet)
        total_volume = float(volume_row['total_volume'])
        if total_volume > 0:
            roi = realized_pnl / total_volume
            pnl_score_raw = 2.0 / (1.0 + math.exp(-roi * 10.0)) - 1.0
        else:
            pnl_score_raw = 0.0

        # ---- 2. Win Rate (weighted by position_size) ----
        wr_row = await db.fetchrow("""
            SELECT
                COALESCE(SUM(CASE WHEN pnl > 0 THEN position_size ELSE 0 END), 0) AS win_volume,
                COALESCE(SUM(position_size), 0) AS total_volume
            FROM trades_log
            WHERE wallet = $1 AND pnl IS NOT NULL
        """, wallet)
        win_volume = float(wr_row['win_volume'])
        wr_total = float(wr_row['total_volume'])
        win_rate = win_volume / wr_total if wr_total > 0 else 0.0
        win_rate_raw = win_rate  # already 0-1

        # ---- 3. Avg Position Size (already computed above) ----

        # ---- 4. Market Diversity (log scale diminishing returns) ----
        # log2(n+1) / log2(max_expected+1), capped at 1.0
        diversity_raw = min(1.0, math.log2(distinct_markets + 1) / math.log2(21))

        # ---- 5. Timing Edge ----
        timing_rows = await db.fetch("""
            SELECT entry_price, peak_price
            FROM trades_log
            WHERE wallet = $1
              AND entry_price IS NOT NULL
              AND peak_price IS NOT NULL
              AND peak_price > entry_price
        """, wallet)

        if timing_rows:
            scores = []
            for r in timing_rows:
                entry = float(r['entry_price'])
                peak = float(r['peak_price'])
                price_range = peak - entry
                if price_range <= 0:
                    continue
                # Where in the eventual range did the trader enter?
                # Lower ratio = earlier entry (closer to bottom)
                ratio = (entry - entry) / price_range  # always 0 at entry
                # We need a reference low. Since entry IS our low reference,
                # use peak_price as the 24h high. The metric is:
                # position_in_range = entry / peak  (0 = free, 1 = at peak)
                position_in_range = entry / peak
                if position_in_range <= 0.25:
                    scores.append(1.0)   # Early
                elif position_in_range <= 0.60:
                    scores.append(0.5)   # Mid
                else:
                    scores.append(0.1)   # Late
            timing_raw = float(np.mean(scores)) if scores else 0.5
        else:
            timing_raw = 0.5

        # ---- 6. Closing Efficiency ----
        closing_rows = await db.fetch("""
            SELECT exit_price, peak_price, entry_price
            FROM trades_log
            WHERE wallet = $1
              AND exit_price IS NOT NULL
              AND peak_price IS NOT NULL
              AND peak_price > entry_price
        """, wallet)

        if closing_rows:
            efficiencies = []
            for r in closing_rows:
                exit_p = float(r['exit_price'])
                peak_p = float(r['peak_price'])
                if peak_p > 0:
                    efficiencies.append(min(1.0, exit_p / peak_p))
            closing_raw = float(np.mean(efficiencies)) if efficiencies else 0.0
        else:
            closing_raw = 0.0

        # ---- 7. Consistency Score ----
        consistency_raw = await self._compute_consistency(wallet)

        return {
            'wallet': wallet,
            'total_trades': total_trades,
            'distinct_markets': distinct_markets,
            'avg_position_size': avg_position_size,
            'active_days': active_days,
            'last_trade_at': last_trade_at,
            'realized_pnl': realized_pnl,
            'pnl_score_raw': pnl_score_raw,
            'win_rate': win_rate,
            'win_rate_raw': win_rate_raw,
            'market_diversity_raw': diversity_raw,
            'diversity_score_raw': diversity_raw,
            'timing_edge_raw': timing_raw,
            'closing_efficiency_raw': closing_raw,
            'consistency_raw': consistency_raw,
        }

    async def _compute_consistency(self, wallet: str) -> float:
        """Consistency = inverse of PnL coefficient-of-variation over
        rolling 7-day and 30-day windows.

        Lower variance relative to mean => higher score.
        Penalizes erratic returns; rewards stable profitability.
        """
        # Fetch daily PnL series
        daily_pnl_rows = await db.fetch("""
            SELECT entry_time::date AS trade_date,
                   SUM(pnl) AS daily_pnl
            FROM trades_log
            WHERE wallet = $1 AND pnl IS NOT NULL
            GROUP BY entry_time::date
            ORDER BY trade_date
        """, wallet)

        if len(daily_pnl_rows) < 3:
            return 0.0

        daily_pnls = np.array([float(r['daily_pnl']) for r in daily_pnl_rows], dtype=np.float64)

        def rolling_cv(arr: np.ndarray, window: int) -> float:
            """Mean absolute coefficient of variation over rolling windows."""
            if len(arr) < window:
                return float('inf')
            cvs = []
            for i in range(len(arr) - window + 1):
                chunk = arr[i:i + window]
                mean = np.mean(chunk)
                std = np.std(chunk)
                if abs(mean) > 1e-9:
                    cvs.append(abs(std / mean))
                else:
                    cvs.append(abs(std) * 10)  # penalize zero-mean high-variance
            return float(np.mean(cvs)) if cvs else float('inf')

        cv_7 = rolling_cv(daily_pnls, 7)
        cv_30 = rolling_cv(daily_pnls, 30)

        # Blend: 60% weight on 30-day stability, 40% on 7-day
        blended_cv = 0.6 * cv_30 + 0.4 * cv_7 if cv_30 != float('inf') else cv_7

        # Convert CV to 0-1 score: consistency = 1 / (1 + cv)
        if blended_cv == float('inf'):
            return 0.0

        consistency = 1.0 / (1.0 + blended_cv)
        return max(0.0, min(1.0, consistency))

    @staticmethod
    def _empty_result(wallet: str) -> Dict:
        return {
            'wallet': wallet,
            'signal_score': 0.0,
            'realized_pnl': 0.0,
            'win_rate': 0.0,
            'avg_position_size': 0.0,
            'market_diversity': 0.0,
            'timing_edge': 0.0,
            'closing_efficiency': 0.0,
            'consistency_score': 0.0,
            'total_trades': 0,
            'active_days': 0,
            'distinct_markets': 0,
            'last_trade_at': None,
        }


# Global singleton
wallet_scoring_service = WalletScoringService()
