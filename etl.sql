-- [STEP: DROP_TEMP_VIEWS]
DROP VIEW IF EXISTS vw_customer_summary;
-- [STEP: DROP_TEMP_VIEWS]
DROP VIEW IF EXISTS vw_customer_orders;
-- [STEP: DROP_TEMP_VIEWS]
DROP VIEW IF EXISTS vw_orders;
-- [STEP: DROP_TEMP_VIEWS]
DROP VIEW IF EXISTS vw_customers;

-- [STEP: LOAD_CUSTOMERS]
-- {% if with_view %}
CREATE OR REPLACE TEMP VIEW vw_customers AS
-- {% endif %}
SELECT id, name FROM customers;

-- [STEP: LOAD_ORDERS]
-- {% if with_view %}
CREATE OR REPLACE TEMP VIEW vw_orders AS
-- {% endif %}
SELECT order_id, order_date, customer_id FROM orders;

-- [STEP: TRANSFORM_CUSTOMER_ORDERS]
-- {% if with_view %}
CREATE OR REPLACE TEMP VIEW vw_customer_orders AS
-- {% endif %}
WITH tmp_joined_data AS (
    SELECT c.id AS customer_id, c.name, o.order_id, o.order_date
    FROM vw_customers c
    JOIN vw_orders o ON c.id = o.customer_id
),
tmp_enriched_data AS (
    SELECT *,
           CAST(order_date AS DATE) AS order_date_cast,
           YEAR(order_date) AS order_year
    FROM tmp_joined_data
)
SELECT * FROM tmp_enriched_data;

-- [STEP: SUMMARIZE_CUSTOMERS]
-- {% if with_view %}
CREATE OR REPLACE TEMP VIEW vw_customer_summary AS
-- {% endif %}
SELECT customer_id, name, COUNT(order_id) AS order_count, MIN(order_date_cast) AS first_order
FROM vw_customer_orders
GROUP BY customer_id, name;

-- [STEP: INSERT_INTO]
-- 新增資料
INSERT INTO customers (id, name) VALUES (1, 'Alice');
