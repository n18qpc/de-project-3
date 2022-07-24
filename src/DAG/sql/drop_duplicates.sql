-- 1. staging.customer_research
DELETE FROM staging.customer_research
WHERE id IN (
    SELECT id
    FROM (
        SELECT 
            id,
            ROW_NUMBER() OVER (partition BY date_time, category_id, geo_id, sales_qty, sales_amt ORDER BY id) AS rnum
        FROM staging.customer_research) t
        WHERE t.rnum > 1);


-- 2. staging.user_activity_log
DELETE FROM staging.user_activity_log
WHERE id IN (
    SELECT id
    FROM (
        SELECT 
            id,
            ROW_NUMBER() OVER (partition BY action_id, customer_id, date_time, quantity ORDER BY id) AS rnum
        FROM staging.user_activity_log) t
        WHERE t.rnum > 1);


-- 3. staging.user_orders_log
DELETE FROM staging.user_orders_log
WHERE id IN (
    SELECT id
    FROM (
        SELECT 
            id,
            ROW_NUMBER() OVER (partition BY city_id, city_name, customer_id, date_time, first_name, item_id, item_name, last_name, quantity, payment_amount ORDER BY id) AS rnum
        FROM staging.user_orders_log) t
        WHERE t.rnum > 1);
