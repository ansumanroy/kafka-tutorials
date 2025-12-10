-- Example KSQL queries

-- 1. Filter page views for specific URL
CREATE STREAM homepage_views AS
    SELECT *
    FROM page_views
    WHERE page_url = '/home'
    EMIT CHANGES;

-- 2. Aggregate: Count clicks per element (tumbling window)
CREATE TABLE click_counts_1min AS
    SELECT 
        element_id,
        COUNT(*) as click_count,
        WINDOWSTART as window_start,
        WINDOWEND as window_end
    FROM click_events
    WINDOW TUMBLING (SIZE 1 MINUTE)
    GROUP BY element_id
    EMIT CHANGES;

-- 3. Filter orders above threshold
CREATE STREAM high_value_orders AS
    SELECT *
    FROM orders
    WHERE amount > 100.0
    EMIT CHANGES;

-- 4. Calculate running totals
CREATE TABLE order_totals_by_user AS
    SELECT 
        user_id,
        SUM(amount) as total_spent,
        COUNT(*) as order_count
    FROM orders
    GROUP BY user_id
    EMIT CHANGES;

-- 5. Session window: User sessions (5 min inactivity)
CREATE TABLE user_sessions AS
    SELECT 
        user_id,
        COUNT(*) as events_in_session,
        MIN(view_time) as session_start,
        MAX(view_time) as session_end
    FROM page_views
    WINDOW SESSION (5 MINUTES)
    GROUP BY user_id
    EMIT CHANGES;

-- Query examples (run interactively)
-- SELECT * FROM page_views EMIT CHANGES;
-- SELECT * FROM user_activity_counts WHERE user_id = 'user123';
-- SELECT * FROM click_counts_1min EMIT CHANGES;
