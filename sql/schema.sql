-- SQL Schema for Multi-Cloud Financial ETL Pipeline
-- Database should be 'etl_demo_db'

-- Table storing daily ticker prices
CREATE TABLE IF NOT EXISTS daily_prices_fact (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL,
    exchange VARCHAR(50),
    currency VARCHAR(10),
    trade_date DATE NOT NULL,
    open_price NUMERIC(15, 6),
    high_price NUMERIC(15, 6),
    low_price NUMERIC(15, 6),
    close_price NUMERIC(15, 6),
    volume BIGINT,
    rolling_7d_avg_close NUMERIC(15, 6),
    rolling_30d_avg_close NUMERIC(15, 6),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT unique_symbol_date UNIQUE(symbol, trade_date)
);

-- Audit table for ETL job execution logs
CREATE TABLE IF NOT EXISTS etl_audit_log (
    log_id SERIAL PRIMARY KEY,
    execution_id VARCHAR(50) NOT NULL,
    job_name VARCHAR(100) NOT NULL,
    status VARCHAR(20) NOT NULL, -- e.g. 'STARTED', 'SUCCESS', 'FAILED'
    records_processed INTEGER DEFAULT 0,
    error_message TEXT,
    start_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    end_time TIMESTAMP,
    duration_seconds INTEGER
);
