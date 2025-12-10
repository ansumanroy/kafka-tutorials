-- KSQL joins

-- 1. Stream-Table join: Enrich orders with user info
CREATE STREAM enriched_orders AS
    SELECT 
        o.order_id,
        o.user_id,
        o.product_id,
        o.amount,
        u.name as user_name,
        u.email as user_email,
        u.tier as user_tier
    FROM orders o
    LEFT JOIN users u ON o.user_id = u.user_id
    EMIT CHANGES;

-- 2. Stream-Table join: Enrich orders with product info
CREATE STREAM orders_with_product_details AS
    SELECT 
        o.order_id,
        o.user_id,
        o.amount,
        p.name as product_name,
        p.category as product_category,
        p.price as product_price
    FROM orders o
    LEFT JOIN products p ON o.product_id = p.product_id
    EMIT CHANGES;

-- 3. Stream-Stream join: Match clicks with page views (within 5 min)
CREATE STREAM clicks_with_context AS
    SELECT 
        c.click_id,
        c.user_id,
        c.element_id,
        c.click_time,
        p.page_url,
        p.view_time
    FROM click_events c
    INNER JOIN page_views p 
        WITHIN 5 MINUTES
        ON c.user_id = p.user_id
    EMIT CHANGES;

-- 4. Table-Table join: Users with their activity
CREATE TABLE users_with_activity AS
    SELECT 
        u.user_id,
        u.name,
        u.email,
        COALESCE(a.total_views, 0) as total_views
    FROM users u
    LEFT JOIN user_activity_counts a ON u.user_id = a.user_id;

-- Query enriched data
-- SELECT * FROM enriched_orders WHERE amount > 50 EMIT CHANGES;
-- SELECT * FROM clicks_with_context EMIT CHANGES;
