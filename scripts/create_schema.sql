-- Market data (EOD)
CREATE TABLE IF NOT EXISTS market_data_eod (
    id BIGSERIAL PRIMARY KEY,
    ticker VARCHAR(10) NOT NULL,
    date DATE NOT NULL,
    open DECIMAL(12,4),
    high DECIMAL(12,4),
    low DECIMAL(12,4),
    close DECIMAL(12,4),
    volume BIGINT,
    adjusted_close DECIMAL(12,4),
    provider VARCHAR(50) NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(ticker, date, provider)
);

CREATE INDEX idx_market_data_ticker ON market_data_eod(ticker);
CREATE INDEX idx_market_data_date ON market_data_eod(date DESC);
CREATE INDEX idx_market_data_ticker_date ON market_data_eod(ticker, date DESC);

-- Corporate actions
CREATE TABLE IF NOT EXISTS corporate_actions (
    id SERIAL PRIMARY KEY,
    ticker VARCHAR(10) NOT NULL,
    action_type VARCHAR(20) NOT NULL,
    ex_date DATE NOT NULL,
    payment_date DATE,
    ratio DECIMAL(10,6),
    amount DECIMAL(10,4),
    provider VARCHAR(50),
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_corp_actions_ticker ON corporate_actions(ticker);
CREATE INDEX idx_corp_actions_date ON corporate_actions(ex_date DESC);

-- Validation results
CREATE TABLE IF NOT EXISTS validation_results (
    id SERIAL PRIMARY KEY,
    validation_date DATE NOT NULL,
    ticker VARCHAR(10) NOT NULL,
    validation_type VARCHAR(50) NOT NULL,
    severity VARCHAR(20) NOT NULL,
    provider_a VARCHAR(50),
    provider_b VARCHAR(50),
    expected_value DECIMAL(12,4),
    actual_value DECIMAL(12,4),
    details JSONB,
    resolved BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_validation_date ON validation_results(validation_date DESC);
CREATE INDEX idx_validation_ticker ON validation_results(ticker);
CREATE INDEX idx_validation_severity ON validation_results(severity);

-- S&P 500 constituents
CREATE TABLE IF NOT EXISTS sp500_constituents (
    ticker VARCHAR(10) PRIMARY KEY,
    company_name VARCHAR(255),
    sector VARCHAR(100),
    industry VARCHAR(100),
    added_date DATE,
    is_active BOOLEAN DEFAULT TRUE
);

-- Provider metadata
CREATE TABLE IF NOT EXISTS provider_metadata (
    provider_name VARCHAR(50) PRIMARY KEY,
    is_active BOOLEAN DEFAULT TRUE,
    traffic_percentage INT DEFAULT 0,
    last_update TIMESTAMPTZ,
    config JSONB
);

INSERT INTO provider_metadata (provider_name, is_active, traffic_percentage) 
VALUES 
    ('massive', TRUE, 100),
    ('bloomberg', FALSE, 0)
ON CONFLICT (provider_name) DO NOTHING;

-- Index values
CREATE TABLE IF NOT EXISTS index_values (
    id SERIAL PRIMARY KEY,
    index_ticker VARCHAR(20) NOT NULL,
    date DATE NOT NULL,
    value DECIMAL(12,4) NOT NULL,
    return_pct DECIMAL(8,4),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(index_ticker, date)
);

CREATE INDEX idx_index_values_ticker ON index_values(index_ticker);
CREATE INDEX idx_index_values_date ON index_values(date DESC);

-- Index constituents
CREATE TABLE IF NOT EXISTS index_constituents (
    id SERIAL PRIMARY KEY,
    index_ticker VARCHAR(20) NOT NULL,
    ticker VARCHAR(10) NOT NULL,
    weight DECIMAL(8,4),
    effective_date DATE NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_index_constituents_index ON index_constituents(index_ticker, effective_date DESC);

SELECT 'Schema created successfully!' as status;
