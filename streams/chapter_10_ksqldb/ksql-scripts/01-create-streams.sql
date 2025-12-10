-- Create streams from Kafka topics

-- Page views stream
CREATE STREAM page_views (
    view_id VARCHAR KEY,
    user_id VARCHAR,
    page_url VARCHAR,
    view_time BIGINT
) WITH (
    KAFKA_TOPIC='page-views',
    VALUE_FORMAT='JSON',
    PARTITIONS=1,
    REPLICAS=1
);

-- Click events stream
CREATE STREAM click_events (
    click_id VARCHAR KEY,
    user_id VARCHAR,
    element_id VARCHAR,
    click_time BIGINT
) WITH (
    KAFKA_TOPIC='click-events',
    VALUE_FORMAT='JSON',
    PARTITIONS=1,
    REPLICAS=1
);

-- Order events stream
CREATE STREAM orders (
    order_id VARCHAR KEY,
    user_id VARCHAR,
    product_id VARCHAR,
    amount DOUBLE,
    order_time BIGINT
) WITH (
    KAFKA_TOPIC='orders',
    VALUE_FORMAT='JSON',
    TIMESTAMP='order_time',
    PARTITIONS=1,
    REPLICAS=1
);

-- Show streams
SHOW STREAMS;
