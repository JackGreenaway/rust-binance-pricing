SET
    TIME ZONE 'UTC';



CREATE TABLE IF NOT EXISTS
    market_trade (
        ts timestamptz NOT NULL,
        symbol VARCHAR,
        price NUMERIC(30, 10),
        quantity NUMERIC(30, 10),
        num_trades SMALLINT,
        maker BOOLEAN
    );



CREATE INDEX IF NOT EXISTS idx_market_trade_ts ON market_trade (ts DESC);



CREATE TABLE IF NOT EXISTS
    depth_updates (
        depth_update_id bigserial PRIMARY KEY,
        event_time timestamptz,
        transaction_time timestamptz,
        symbol VARCHAR,
        first_update_id BIGINT,
        last_update_id BIGINT,
        previous_update_id BIGINT
    );



CREATE INDEX IF NOT EXISTS idx_depth_update_ts ON depth_updates (transaction_time DESC);



CREATE TABLE IF NOT EXISTS
    bid_depth (
        depth_update_id BIGINT REFERENCES depth_updates (depth_update_id) ON DELETE CASCADE,
        level_id SMALLINT,
        price NUMERIC(30, 10) NOT NULL,
        quantity NUMERIC(30, 10) NOT NULL
    );



CREATE TABLE IF NOT EXISTS
    ask_depth (
        depth_update_id BIGINT REFERENCES depth_updates (depth_update_id) ON DELETE CASCADE,
        level_id SMALLINT,
        price NUMERIC(30, 10) NOT NULL,
        quantity NUMERIC(30, 10) NOT NULL
    );