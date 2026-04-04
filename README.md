# Polymarket Wallet Tracker

Wallet intelligence and copy-trade platform for Polymarket. Scores wallets on 7 metrics, tracks trades, generates alerts, and executes copy trades with performance fees.

## Tech Stack
- Python 3.12, FastAPI, asyncpg
- Neon Postgres (8 tables)
- Redis (caching, optional)
- py-clob-client for Polymarket CLOB
- Docker + Zeabur

## API Endpoints
- `GET /health` — Service health
- `GET /api/wallets/top` — Top wallets by signal score
- `GET /api/wallets/{addr}/details` — Wallet detail with trades
- `GET /api/trades/recent` — Recent trades
- `GET /api/leaderboard/` — Full leaderboard
- `GET /api/pnl` — Portfolio PnL
- `POST /api/wallets/score/all` — Trigger full scoring run
- `GET /api/export` — Excel export

## Env Vars
- `DATABASE_URL` — Neon Postgres
- `REDIS_URL` — Upstash Redis (optional)
- `POLYMARKET_API_KEY`, `POLYMARKET_SECRET`, `POLYMARKET_PASSPHRASE`, `POLYMARKET_PRIVATE_KEY`
- `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID`

## Deployment
```bash
docker build -t wallet-tracker .
docker run -p 8080:8080 --env-file .env wallet-tracker
```
Deployed on Zeabur (project: phantom-pipeline).

