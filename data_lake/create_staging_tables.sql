-- Create staging tables

-- Warehouse Temperature Sensor Data
CREATE TABLE IF NOT EXISTS warehouse_temp_sensor (
    timestamp TIMESTAMP,
    temperature DECIMAL(5,2),
    humidity DECIMAL(5,2),
    location VARCHAR(50),
    sensor_id VARCHAR(20)
);

-- Market Share Data
CREATE TABLE IF NOT EXISTS market_share_report (
    product_category VARCHAR(50),
    market_share DECIMAL(5,2),
    quarter VARCHAR(10),
    year INTEGER,
    region VARCHAR(50)
);

-- Tweet Data
CREATE TABLE IF NOT EXISTS tweets (
    tweet_id SERIAL PRIMARY KEY,
    text TEXT,
    created_at TIMESTAMP,
    location VARCHAR(100),
    sentiment VARCHAR(20),
    product_category VARCHAR(100)
);
