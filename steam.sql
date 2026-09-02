CREATE TABLE IF NOT EXISTS steam_review(
    reviewid VARCHAR PRIMARY KEY,
    appid BIGINT,
    review VARCHAR,
    language VARCHAR,
    timestamp_created TIMESTAMPTZ,
    refunded BOOLEAN,
    extracted_at VARCHAR,
    game_name VARCHAR,
    publisher VARCHAR
)
CREATE TABLE IF NOT EXISTS last_ingested(
    appid BIGINT PRIMARY KEY,
    timestamp_created TIMESTAMPTZ,
    reviewid VARCHAR
)