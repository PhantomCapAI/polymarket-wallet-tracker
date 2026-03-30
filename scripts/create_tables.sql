-- Wallet master data with calculated scores
CREATE TABLE IF NOT EXISTS wallets_master (
    wallet VARCHAR(42) PRIMARY KEY,
    signal_score DECIMAL(4,3) NOT NULL DEFAULT 0,
    realized_pnl DECIMAL(15,6) DEFAULT 0,
    win_rate DECIMAL(4,3) DEFAULT 0,
    avg_position_size DECIMAL(15,6) DEFAULT 0,
    market_diversity INTEGER DEFAULT 0,
    timing_edge VARCHAR(20) DEFAULT 'none',
    closing_efficiency DECIMAL(4,3) DEFAULT 0,
    consistency_score DECIMAL(4,3) DEFAULT 0,
    total_trades INTEGER DEFAULT 0,
    active_days INTEGER DEFAULT 0,
    last_trade_at TIMESTAMP,
    last_updated TIMESTAMP DEFAULT NOW()
);

-- Individual trade records
CREATE TABLE IF NOT EXISTS trades_log (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    wallet VARCHAR(42) NOT NULL,
    market VARCHAR(100) NOT NULL,
    direction VARCHAR(10) NOT NULL,
    entry_price DECIMAL(10,8) NOT NULL,
    position_size DECIMAL(15,6) NOT NULL,
    exit_price DECIMAL(10,8),
    pnl DECIMAL(15,6),
    entry_time TIMESTAMP NOT NULL,
    exit_time TIMESTAMP,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Market summary and trends
CREATE TABLE IF NOT EXISTS market_summary (
    market VARCHAR(100) PRIMARY KEY,
    total_volume DECIMAL(20,6) DEFAULT 0,
    trade_count INTEGER DEFAULT 0,
    smart_money_count INTEGER DEFAULT 0,
    trend_bias VARCHAR(20) DEFAULT 'neutral',
    avg_entry_price DECIMAL(10,8),
    price_momentum DECIMAL(5,3) DEFAULT 0,
    last_updated TIMESTAMP DEFAULT NOW()
);

-- Alert system logs
CREATE TABLE IF NOT EXISTS alerts_log (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    wallet VARCHAR(42) NOT NULL,
    event_type VARCHAR(50) NOT NULL,
    confidence VARCHAR(20) NOT NULL,
    signal_reason TEXT,
    market VARCHAR(100),
    timestamp TIMESTAMP DEFAULT NOW(),
    processed BOOLEAN DEFAULT FALSE
);

-- Copy trading execution log
CREATE TABLE IF NOT EXISTS copy_trades (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source_wallet VARCHAR(42) NOT NULL,
    market VARCHAR(100) NOT NULL,
    direction VARCHAR(10) NOT NULL,
    entry_price DECIMAL(10,8) NOT NULL,
    position_size DECIMAL(15,6) NOT NULL,
    signal_score DECIMAL(4,3) NOT NULL,
    status VARCHAR(20) DEFAULT 'open',
    stop_loss_price DECIMAL(10,8),
    exit_price DECIMAL(10,8),
    pnl DECIMAL(15,6),
    created_at TIMESTAMP DEFAULT NOW(),
    closed_at TIMESTAMP
);

-- Backtest results storage
CREATE TABLE IF NOT EXISTS backtest_results (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    days_back INTEGER NOT NULL,
    min_signal_score DECIMAL(4,3) NOT NULL,
    status VARCHAR(20) DEFAULT 'pending',
    total_pnl DECIMAL(15,6),
    win_rate DECIMAL(4,3),
    total_trades INTEGER,
    error TEXT,
    created_at TIMESTAMP DEFAULT NOW(),
    completed_at TIMESTAMP
);

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_wallets_signal_score ON wallets_master(signal_score DESC);
CREATE INDEX IF NOT EXISTS idx_trades_wallet ON trades_log(wallet);
CREATE INDEX IF NOT EXISTS idx_trades_market ON trades_log(market);
CREATE INDEX IF NOT EXISTS idx_trades_entry_time ON trades_log(entry_time DESC);
CREATE INDEX IF NOT EXISTS idx_alerts_timestamp ON alerts_log(timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_copy_trades_status ON copy_trades(status);
CREATE INDEX IF NOT EXISTS idx_copy_trades_created ON copy_trades(created_at DESC);
