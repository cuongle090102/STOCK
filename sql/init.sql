-- Initialize Vietnamese Algorithmic Trading Database
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Create schemas
CREATE SCHEMA IF NOT EXISTS market_data;
CREATE SCHEMA IF NOT EXISTS strategies;
CREATE SCHEMA IF NOT EXISTS risk_management;
CREATE SCHEMA IF NOT EXISTS monitoring;

-- Market data tables
CREATE TABLE market_data.symbols (
    symbol VARCHAR(10) PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    exchange VARCHAR(10) NOT NULL,
    sector VARCHAR(50),
    industry VARCHAR(100),
    market_cap_category VARCHAR(10),
    is_vn30 BOOLEAN DEFAULT FALSE,
    foreign_ownership_limit DECIMAL(5,2) DEFAULT 30.0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE market_data.daily_prices (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    symbol VARCHAR(10) NOT NULL REFERENCES market_data.symbols(symbol),
    date DATE NOT NULL,
    open_price DECIMAL(15,2),
    high_price DECIMAL(15,2),
    low_price DECIMAL(15,2),
    close_price DECIMAL(15,2) NOT NULL,
    volume BIGINT DEFAULT 0,
    value DECIMAL(20,2) DEFAULT 0,
    vwap DECIMAL(15,2),
    trades_count INTEGER,
    data_source VARCHAR(20),
    data_quality_score DECIMAL(3,2) DEFAULT 1.0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(symbol, date)
);

CREATE TABLE market_data.intraday_prices (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    symbol VARCHAR(10) NOT NULL REFERENCES market_data.symbols(symbol),
    timestamp TIMESTAMP NOT NULL,
    price DECIMAL(15,2) NOT NULL,
    volume INTEGER DEFAULT 0,
    bid_price DECIMAL(15,2),
    ask_price DECIMAL(15,2),
    bid_volume INTEGER,
    ask_volume INTEGER,
    data_source VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Trading strategies tables
CREATE TABLE strategies.strategy_definitions (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name VARCHAR(100) NOT NULL UNIQUE,
    description TEXT,
    strategy_type VARCHAR(50) NOT NULL,
    parameters JSONB,
    is_active BOOLEAN DEFAULT TRUE,
    risk_limit DECIMAL(10,4) DEFAULT 0.02,
    created_by VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE strategies.trading_signals (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    strategy_id UUID NOT NULL REFERENCES strategies.strategy_definitions(id),
    symbol VARCHAR(10) NOT NULL REFERENCES market_data.symbols(symbol),
    timestamp TIMESTAMP NOT NULL,
    signal_type VARCHAR(10) NOT NULL CHECK (signal_type IN ('BUY', 'SELL', 'HOLD')),
    confidence DECIMAL(3,2) NOT NULL CHECK (confidence >= 0 AND confidence <= 1),
    strength DECIMAL(5,2),
    target_price DECIMAL(15,2),
    stop_loss DECIMAL(15,2),
    expected_return DECIMAL(5,4),
    expected_risk DECIMAL(5,4),
    parameters JSONB,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Risk management tables
CREATE TABLE risk_management.position_limits (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    symbol VARCHAR(10) REFERENCES market_data.symbols(symbol),
    strategy_id UUID REFERENCES strategies.strategy_definitions(id),
    position_type VARCHAR(20) NOT NULL CHECK (position_type IN ('SYMBOL', 'SECTOR', 'PORTFOLIO')),
    limit_type VARCHAR(20) NOT NULL CHECK (limit_type IN ('MAX_POSITION', 'MAX_LOSS', 'MAX_EXPOSURE')),
    limit_value DECIMAL(15,2) NOT NULL,
    limit_percentage DECIMAL(5,2),
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE risk_management.risk_events (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    event_type VARCHAR(50) NOT NULL,
    severity VARCHAR(10) NOT NULL CHECK (severity IN ('LOW', 'MEDIUM', 'HIGH', 'CRITICAL')),
    symbol VARCHAR(10) REFERENCES market_data.symbols(symbol),
    strategy_id UUID REFERENCES strategies.strategy_definitions(id),
    description TEXT NOT NULL,
    risk_metric VARCHAR(50),
    current_value DECIMAL(15,4),
    threshold_value DECIMAL(15,4),
    breach_magnitude DECIMAL(15,4),
    action_required BOOLEAN DEFAULT FALSE,
    action_taken TEXT,
    is_resolved BOOLEAN DEFAULT FALSE,
    resolved_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Monitoring tables
CREATE TABLE monitoring.system_health (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    component VARCHAR(50) NOT NULL,
    status VARCHAR(20) NOT NULL CHECK (status IN ('HEALTHY', 'WARNING', 'CRITICAL', 'DOWN')),
    metrics JSONB,
    error_message TEXT,
    last_check TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert VN30 symbols
INSERT INTO market_data.symbols (symbol, name, exchange, sector, is_vn30, market_cap_category) VALUES
('VIC', 'Vingroup Joint Stock Company', 'HOSE', 'Real Estate', true, 'LARGE'),
('VNM', 'Vietnam Dairy Products Joint Stock Company', 'HOSE', 'Consumer Goods', true, 'LARGE'),
('HPG', 'Hoa Phat Group Joint Stock Company', 'HOSE', 'Materials', true, 'LARGE'),
('VCB', 'Joint Stock Commercial Bank for Foreign Trade of Vietnam', 'HOSE', 'Banking', true, 'LARGE'),
('FPT', 'FPT Corporation', 'HOSE', 'Technology', true, 'LARGE'),
('GAS', 'PetroVietnam Gas Joint Stock Corporation', 'HOSE', 'Energy', true, 'LARGE'),
('BID', 'Bank for Investment and Development of Vietnam', 'HOSE', 'Banking', true, 'LARGE'),
('CTG', 'Vietnam Joint Stock Commercial Bank for Industry and Trade', 'HOSE', 'Banking', true, 'LARGE'),
('MSN', 'Masan Group Corporation', 'HOSE', 'Consumer Goods', true, 'LARGE'),
('PLX', 'Petrolimex', 'HOSE', 'Energy', true, 'LARGE'),
('VHM', 'Vinhomes Joint Stock Company', 'HOSE', 'Real Estate', true, 'LARGE'),
('TCB', 'Vietnam Technological and Commercial Joint Stock Bank', 'HOSE', 'Banking', true, 'LARGE'),
('MWG', 'Mobile World Investment Corporation', 'HOSE', 'Retail', true, 'LARGE'),
('VRE', 'Vincom Real Estate Investment Company Limited', 'HOSE', 'Real Estate', true, 'LARGE'),
('SAB', 'Sabeco', 'HOSE', 'Consumer Goods', true, 'LARGE'),
('NVL', 'Novaland Group', 'HOSE', 'Real Estate', true, 'LARGE'),
('POW', 'PetroVietnam Power Corporation', 'HOSE', 'Utilities', true, 'LARGE'),
('KDH', 'Khang Dien House Trading and Investment Joint Stock Company', 'HOSE', 'Real Estate', true, 'LARGE'),
('TPB', 'Tien Phong Bank', 'HOSE', 'Banking', true, 'LARGE'),
('SSI', 'Saigon Securities Incorporation', 'HOSE', 'Financial Services', true, 'LARGE'),
('VPB', 'Vietnam Prosperity Joint Stock Commercial Bank', 'HOSE', 'Banking', true, 'LARGE'),
('PDR', 'Phat Dat Real Estate Development Corp', 'HOSE', 'Real Estate', true, 'LARGE'),
('STB', 'Saigon Thuong Tin Commercial Joint Stock Bank', 'HOSE', 'Banking', true, 'LARGE'),
('HDB', 'Ho Chi Minh City Development Joint Stock Commercial Bank', 'HOSE', 'Banking', true, 'LARGE'),
('MBB', 'Military Commercial Joint Stock Bank', 'HOSE', 'Banking', true, 'LARGE'),
('ACB', 'Asia Commercial Joint Stock Bank', 'HOSE', 'Banking', true, 'LARGE'),
('VJC', 'VietJet Aviation Joint Stock Company', 'HOSE', 'Transportation', true, 'LARGE'),
('VND', 'Vina Capital Vietnam Opportunity Fund Limited', 'HNX', 'Investment', true, 'LARGE'),
('GEX', 'GELEX Group', 'UPCOM', 'Pharmaceuticals', true, 'LARGE'),
('DGC', 'Hoa An Joint Stock Company', 'HOSE', 'Chemicals', true, 'LARGE');

-- Create indexes for better performance
CREATE INDEX idx_daily_prices_symbol_date ON market_data.daily_prices(symbol, date);
CREATE INDEX idx_daily_prices_date ON market_data.daily_prices(date);
CREATE INDEX idx_daily_prices_source ON market_data.daily_prices(data_source);
CREATE INDEX idx_intraday_prices_symbol_timestamp ON market_data.intraday_prices(symbol, timestamp);
CREATE INDEX idx_intraday_prices_timestamp ON market_data.intraday_prices(timestamp);
CREATE INDEX idx_trading_signals_strategy_symbol ON strategies.trading_signals(strategy_id, symbol);
CREATE INDEX idx_trading_signals_timestamp ON strategies.trading_signals(timestamp);
CREATE INDEX idx_risk_events_timestamp ON risk_management.risk_events(created_at);
CREATE INDEX idx_risk_events_severity ON risk_management.risk_events(severity);
CREATE INDEX idx_system_health_component ON monitoring.system_health(component);
CREATE INDEX idx_system_health_status ON monitoring.system_health(status);

-- Insert some sample strategy definitions
INSERT INTO strategies.strategy_definitions (name, description, strategy_type, parameters) VALUES
('VN30 Mean Reversion', 'Mean reversion strategy for VN30 stocks', 'MEAN_REVERSION', '{"lookback_period": 20, "z_score_threshold": 2.0, "exit_z_score": 0.5}'),
('Momentum Breakout', 'Momentum strategy for high-volume breakouts', 'MOMENTUM', '{"sma_period": 50, "volume_multiplier": 2.0, "breakout_threshold": 0.05}'),
('VN Index Pairs Trading', 'Statistical arbitrage between correlated VN stocks', 'PAIRS_TRADING', '{"correlation_threshold": 0.8, "spread_z_score": 2.5, "holding_period": 5}');

-- Insert sample risk limits
INSERT INTO risk_management.position_limits (position_type, limit_type, limit_value, limit_percentage) VALUES
('PORTFOLIO', 'MAX_LOSS', 100000000, 5.0),
('PORTFOLIO', 'MAX_EXPOSURE', 2000000000, 80.0),
('SYMBOL', 'MAX_POSITION', 200000000, 10.0),
('SECTOR', 'MAX_EXPOSURE', 500000000, 25.0);

-- Create views for commonly used data
CREATE VIEW market_data.vn30_daily_summary AS
SELECT 
    s.symbol,
    s.name,
    s.sector,
    dp.date,
    dp.close_price,
    dp.volume,
    dp.value,
    COALESCE(LAG(dp.close_price) OVER (PARTITION BY s.symbol ORDER BY dp.date), dp.close_price) as prev_close,
    CASE 
        WHEN LAG(dp.close_price) OVER (PARTITION BY s.symbol ORDER BY dp.date) IS NOT NULL 
        THEN ROUND((dp.close_price - LAG(dp.close_price) OVER (PARTITION BY s.symbol ORDER BY dp.date)) / LAG(dp.close_price) OVER (PARTITION BY s.symbol ORDER BY dp.date) * 100, 2)
        ELSE 0 
    END as price_change_pct
FROM market_data.symbols s
JOIN market_data.daily_prices dp ON s.symbol = dp.symbol
WHERE s.is_vn30 = true
ORDER BY dp.date DESC, s.symbol;

-- Create function to update timestamps
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Create triggers for automatic timestamp updates
CREATE TRIGGER update_symbols_updated_at BEFORE UPDATE ON market_data.symbols FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
CREATE TRIGGER update_daily_prices_updated_at BEFORE UPDATE ON market_data.daily_prices FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
CREATE TRIGGER update_strategy_definitions_updated_at BEFORE UPDATE ON strategies.strategy_definitions FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
CREATE TRIGGER update_position_limits_updated_at BEFORE UPDATE ON risk_management.position_limits FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();