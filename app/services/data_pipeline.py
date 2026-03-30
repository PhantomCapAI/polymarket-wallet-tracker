import asyncio
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import aiohttp

from app.models.database import db

logger = logging.getLogger(__name__)

# Minimum number of trades a wallet must have across sampled markets
# before we insert it into wallets_master for tracking.
MIN_WALLET_TRADES_THRESHOLD = 3


class DataPipeline:
    """Fetches data from the Polymarket CLOB API and persists it locally."""

    def __init__(self):
        self.base_url = "https://clob.polymarket.com"
        self.session: Optional[aiohttp.ClientSession] = None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def initialize(self):
        """Create a reusable aiohttp session."""
        if self.session is None or self.session.closed:
            timeout = aiohttp.ClientTimeout(total=30)
            self.session = aiohttp.ClientSession(timeout=timeout)
            logger.info("DataPipeline aiohttp session created")

    async def close(self):
        """Close the aiohttp session."""
        if self.session and not self.session.closed:
            await self.session.close()
            logger.info("DataPipeline aiohttp session closed")

    # ------------------------------------------------------------------
    # Raw API helpers
    # ------------------------------------------------------------------

    async def _get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Any:
        """Issue a GET request with basic error handling and rate-limit delay."""
        await self.initialize()
        url = f"{self.base_url}{path}"
        try:
            async with self.session.get(url, params=params) as resp:
                if resp.status == 429:
                    retry_after = int(resp.headers.get("Retry-After", "5"))
                    logger.warning("Rate-limited by CLOB API, sleeping %ss", retry_after)
                    await asyncio.sleep(retry_after)
                    return await self._get(path, params)
                resp.raise_for_status()
                data = await resp.json()
                # Rate-limit: ~10 req/s
                await asyncio.sleep(0.1)
                return data
        except aiohttp.ClientError as exc:
            logger.error("HTTP error fetching %s: %s", url, exc)
            return None
        except Exception as exc:
            logger.error("Unexpected error fetching %s: %s", url, exc)
            return None

    # ------------------------------------------------------------------
    # CLOB API wrappers
    # ------------------------------------------------------------------

    async def fetch_markets(self, next_cursor: str = "") -> Optional[Dict[str, Any]]:
        """GET /markets — returns a page of active markets.

        The API returns ``{"data": [...], "next_cursor": "..."}``.
        Pass the returned ``next_cursor`` to paginate.
        """
        params: Dict[str, Any] = {}
        if next_cursor:
            params["next_cursor"] = next_cursor
        return await self._get("/markets", params=params)

    async def fetch_trades(self, token_id: str, limit: int = 100) -> Optional[List[Dict[str, Any]]]:
        """GET /trades — recent trades for a given token_id."""
        data = await self._get("/trades", params={"market": token_id, "limit": str(limit)})
        if data is None:
            return None
        # The endpoint may return a list directly or wrapped in a key.
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            return data.get("data", data.get("trades", []))
        return []

    async def fetch_price(self, token_id: str) -> Optional[float]:
        """GET /prices — current mid-market price for a token_id."""
        data = await self._get("/prices", params={"token_ids": token_id})
        if data is None:
            return None
        # Response is typically a dict mapping token_id -> price string
        if isinstance(data, dict):
            price_val = data.get(token_id)
            if price_val is not None:
                try:
                    return float(price_val)
                except (ValueError, TypeError):
                    pass
        return None

    # ------------------------------------------------------------------
    # Pipeline stages
    # ------------------------------------------------------------------

    async def update_wallet_data(self, database=None):
        """Discover wallets by scanning recent trades across active markets.

        1. Fetch a batch of active markets.
        2. For each market / token, pull recent trades.
        3. Tally unique wallet addresses.
        4. Upsert wallets that meet the minimum-trade threshold.
        """
        _db = database or db
        logger.info("update_wallet_data: starting")

        wallet_trade_counts: Dict[str, int] = {}
        wallet_last_seen: Dict[str, datetime] = {}

        try:
            # Fetch up to 3 pages of markets (to stay within rate limits)
            next_cursor = ""
            markets_processed = 0

            for _page in range(3):
                markets_resp = await self.fetch_markets(next_cursor=next_cursor)
                if not markets_resp or not isinstance(markets_resp, dict):
                    break

                market_list = markets_resp.get("data", [])
                if not market_list:
                    break

                for market in market_list:
                    # Each market may expose multiple tokens (YES / NO outcomes)
                    tokens = market.get("tokens", [])
                    condition_id = market.get("condition_id", "")

                    for token_info in tokens:
                        token_id = token_info.get("token_id", "")
                        if not token_id:
                            continue

                        trades = await self.fetch_trades(token_id, limit=100)
                        if not trades:
                            continue

                        for trade in trades:
                            for addr_key in ("maker_address", "taker_address"):
                                addr = trade.get(addr_key)
                                if addr:
                                    addr = addr.lower()
                                    wallet_trade_counts[addr] = wallet_trade_counts.get(addr, 0) + 1
                                    ts = self._parse_timestamp(trade.get("timestamp"))
                                    if ts and (addr not in wallet_last_seen or ts > wallet_last_seen[addr]):
                                        wallet_last_seen[addr] = ts

                        markets_processed += 1

                next_cursor = markets_resp.get("next_cursor", "")
                if not next_cursor or next_cursor == "LTE=":
                    break

            # Upsert qualifying wallets
            upserted = 0
            for wallet_addr, count in wallet_trade_counts.items():
                if count < MIN_WALLET_TRADES_THRESHOLD:
                    continue

                last_trade = wallet_last_seen.get(wallet_addr)
                try:
                    await _db.execute(
                        """
                        INSERT INTO wallets_master (wallet, total_trades, last_trade_at, last_updated)
                        VALUES ($1, $2, $3, NOW())
                        ON CONFLICT (wallet) DO UPDATE SET
                            total_trades = wallets_master.total_trades + EXCLUDED.total_trades,
                            last_trade_at = GREATEST(wallets_master.last_trade_at, EXCLUDED.last_trade_at),
                            last_updated = NOW()
                        """,
                        wallet_addr,
                        count,
                        last_trade,
                    )
                    upserted += 1
                except Exception as exc:
                    logger.error("Error upserting wallet %s: %s", wallet_addr, exc)

            logger.info(
                "update_wallet_data: processed %d markets, discovered %d wallets, upserted %d",
                markets_processed,
                len(wallet_trade_counts),
                upserted,
            )

        except Exception as exc:
            logger.error("update_wallet_data failed: %s", exc)

    # ------------------------------------------------------------------

    async def update_trade_data(self, database=None):
        """Fetch recent trades for every tracked wallet and persist them.

        1. Read all tracked wallets from wallets_master.
        2. Re-scan market trades to find trades by those wallets.
        3. Deduplicate and insert new trades into trades_log.
        4. Attempt to update exit prices and PnL for open trades.
        """
        _db = database or db
        logger.info("update_trade_data: starting")

        try:
            wallets = await _db.fetch("SELECT wallet FROM wallets_master")
            if not wallets:
                logger.info("update_trade_data: no tracked wallets yet")
                return

            tracked = {row["wallet"] for row in wallets}

            # Collect trades from recent market activity
            next_cursor = ""
            new_trades_inserted = 0

            for _page in range(2):
                markets_resp = await self.fetch_markets(next_cursor=next_cursor)
                if not markets_resp or not isinstance(markets_resp, dict):
                    break

                market_list = markets_resp.get("data", [])
                if not market_list:
                    break

                for market in market_list:
                    market_slug = market.get("question", market.get("condition_id", "unknown"))
                    tokens = market.get("tokens", [])

                    for token_info in tokens:
                        token_id = token_info.get("token_id", "")
                        if not token_id:
                            continue

                        trades = await self.fetch_trades(token_id, limit=100)
                        if not trades:
                            continue

                        for trade in trades:
                            maker = (trade.get("maker_address") or "").lower()
                            taker = (trade.get("taker_address") or "").lower()
                            price = self._safe_float(trade.get("price"))
                            size = self._safe_float(trade.get("size"))
                            side = trade.get("side", "buy")
                            ts = self._parse_timestamp(trade.get("timestamp"))
                            trade_id = trade.get("id", "")

                            for addr in (maker, taker):
                                if addr not in tracked:
                                    continue

                                # Deduplicate: check if this exact trade is already logged
                                if trade_id:
                                    existing = await _db.fetchval(
                                        """
                                        SELECT 1 FROM trades_log
                                        WHERE wallet = $1 AND market = $2 AND entry_price = $3
                                          AND position_size = $4 AND entry_time = $5
                                        LIMIT 1
                                        """,
                                        addr,
                                        market_slug,
                                        price,
                                        size,
                                        ts,
                                    )
                                    if existing:
                                        continue

                                try:
                                    await _db.execute(
                                        """
                                        INSERT INTO trades_log
                                            (wallet, market, direction, entry_price, position_size, entry_time)
                                        VALUES ($1, $2, $3, $4, $5, $6)
                                        """,
                                        addr,
                                        market_slug,
                                        side,
                                        price,
                                        size,
                                        ts,
                                    )
                                    new_trades_inserted += 1
                                except Exception as exc:
                                    logger.error("Error inserting trade for %s: %s", addr, exc)

                next_cursor = markets_resp.get("next_cursor", "")
                if not next_cursor or next_cursor == "LTE=":
                    break

            # ---- Update open trades with exit data ----
            await self._update_open_trades(_db)

            logger.info("update_trade_data: inserted %d new trades", new_trades_inserted)

        except Exception as exc:
            logger.error("update_trade_data failed: %s", exc)

    async def _update_open_trades(self, _db):
        """Try to fill exit_price and PnL for trades that are still open."""
        try:
            open_trades = await _db.fetch(
                """
                SELECT id, market, direction, entry_price, position_size
                FROM trades_log
                WHERE exit_price IS NULL AND entry_price IS NOT NULL
                ORDER BY entry_time DESC
                LIMIT 500
                """
            )

            if not open_trades:
                return

            # Build a cache of market -> current price to avoid redundant lookups.
            price_cache: Dict[str, Optional[float]] = {}

            for trade in open_trades:
                market = trade["market"]
                entry_price = self._safe_float(trade["entry_price"])
                if entry_price is None or entry_price == 0:
                    continue

                # We don't have a direct token_id stored, so we skip price
                # lookup unless we extend the schema. For now, mark as-is.
                # In a future iteration we'd store token_id alongside market.

            # Nothing actionable without token_id mapping; this is a placeholder
            # for when the schema stores token_id in trades_log.
        except Exception as exc:
            logger.error("_update_open_trades failed: %s", exc)

    # ------------------------------------------------------------------

    async def update_market_summaries(self, database=None):
        """Aggregate trade data per market and upsert into market_summary.

        Creates the market_summary table if it doesn't exist yet.
        """
        _db = database or db
        logger.info("update_market_summaries: starting")

        try:
            # Ensure the table exists
            await _db.execute(
                """
                CREATE TABLE IF NOT EXISTS market_summary (
                    market VARCHAR(255) PRIMARY KEY,
                    total_volume DECIMAL(20,6) DEFAULT 0,
                    avg_win_rate DECIMAL(5,3) DEFAULT 0,
                    top_wallet VARCHAR(42),
                    volatility DECIMAL(10,6) DEFAULT 0,
                    smart_money_count INTEGER DEFAULT 0,
                    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            )

            # Aggregate from trades_log
            market_stats = await _db.fetch(
                """
                SELECT
                    t.market,
                    COALESCE(SUM(t.position_size * t.entry_price), 0) AS total_volume,
                    CASE
                        WHEN COUNT(CASE WHEN t.pnl IS NOT NULL THEN 1 END) > 0
                        THEN COUNT(CASE WHEN t.pnl > 0 THEN 1 END)::decimal
                             / COUNT(CASE WHEN t.pnl IS NOT NULL THEN 1 END)
                        ELSE 0
                    END AS avg_win_rate,
                    COALESCE(STDDEV(t.entry_price), 0) AS volatility
                FROM trades_log t
                WHERE t.market IS NOT NULL
                GROUP BY t.market
                """
            )

            for row in market_stats:
                market_name = row["market"]

                # Find top wallet by PnL for this market
                top_wallet_row = await _db.fetchrow(
                    """
                    SELECT wallet, COALESCE(SUM(pnl), 0) AS total_pnl
                    FROM trades_log
                    WHERE market = $1 AND pnl IS NOT NULL
                    GROUP BY wallet
                    ORDER BY total_pnl DESC
                    LIMIT 1
                    """,
                    market_name,
                )
                top_wallet = top_wallet_row["wallet"] if top_wallet_row else None

                # Count smart-money wallets (signal_score > 0.7) active in this market
                smart_money = await _db.fetchval(
                    """
                    SELECT COUNT(DISTINCT t.wallet)
                    FROM trades_log t
                    JOIN wallets_master w ON t.wallet = w.wallet
                    WHERE t.market = $1 AND w.signal_score > 0.7
                    """,
                    market_name,
                )

                await _db.execute(
                    """
                    INSERT INTO market_summary
                        (market, total_volume, avg_win_rate, top_wallet, volatility, smart_money_count, last_updated)
                    VALUES ($1, $2, $3, $4, $5, $6, NOW())
                    ON CONFLICT (market) DO UPDATE SET
                        total_volume = EXCLUDED.total_volume,
                        avg_win_rate = EXCLUDED.avg_win_rate,
                        top_wallet = EXCLUDED.top_wallet,
                        volatility = EXCLUDED.volatility,
                        smart_money_count = EXCLUDED.smart_money_count,
                        last_updated = NOW()
                    """,
                    market_name,
                    float(row["total_volume"]),
                    float(row["avg_win_rate"]),
                    top_wallet,
                    float(row["volatility"]),
                    int(smart_money or 0),
                )

            logger.info("update_market_summaries: processed %d markets", len(market_stats))

        except Exception as exc:
            logger.error("update_market_summaries failed: %s", exc)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _parse_timestamp(value) -> Optional[datetime]:
        """Best-effort parse of a timestamp from the CLOB API."""
        if value is None:
            return None
        if isinstance(value, datetime):
            return value
        if isinstance(value, (int, float)):
            try:
                return datetime.fromtimestamp(value, tz=timezone.utc)
            except (OSError, ValueError):
                return None
        if isinstance(value, str):
            for fmt in (
                "%Y-%m-%dT%H:%M:%S.%fZ",
                "%Y-%m-%dT%H:%M:%SZ",
                "%Y-%m-%dT%H:%M:%S",
                "%Y-%m-%d %H:%M:%S",
            ):
                try:
                    return datetime.strptime(value, fmt).replace(tzinfo=timezone.utc)
                except ValueError:
                    continue
            # Maybe it's a unix timestamp string
            try:
                return datetime.fromtimestamp(float(value), tz=timezone.utc)
            except (ValueError, OSError):
                pass
        return None

    @staticmethod
    def _safe_float(value) -> Optional[float]:
        if value is None:
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None


# Global singleton
data_pipeline = DataPipeline()
