-- Create tables from Kafka topics (compacted topics)

-- Users table (latest state per user)
CREATE TABLE users (
    user_id VARCHAR PRIMARY KEY,
    name VARCHAR,
    email VARCHAR,
    tier VARCHAR,
    created_at BIGINT
) WITH (
    KAFKA_TOPIC='users',
    VALUE_FORMAT='JSON',
    PARTITIONS=1,
    REPLICAS=1
);

-- Products table
CREATE TABLE products (
    product_id VARCHAR PRIMARY KEY,
    name VARCHAR,
    category VARCHAR,
    price DOUBLE,
    stock INT
) WITH (
    KAFKA_TOPIC='products',
    VALUE_FORMAT='JSON',
    PARTITIONS=1,
    REPLICAS=1
);

-- User activity counts (materialized view)
CREATE TABLE user_activity_counts AS
    SELECT 
        user_id,
        COUNT(*) as total_views
    FROM page_views
    GROUP BY user_id
    EMIT CHANGES;

-- Show tables
SHOW TABLES;
