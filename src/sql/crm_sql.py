from jinja2 import Template

load_customers = Template("""
CREATE OR REPLACE TEMP VIEW vw_customers AS
SELECT id, name
FROM {{ dbname }}.customers;
""".strip())

load_orders = Template("""
CREATE OR REPLACE TEMP VIEW vw_orders AS
SELECT order_id, order_date, customer_id
FROM {{ dbname }}.orders;
""".strip())

transform_customer_orders = Template("""
CREATE OR REPLACE TEMP VIEW vw_customer_orders AS
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
SELECT *
FROM tmp_enriched_data;
""".strip())

summarize_customers = Template("""
CREATE OR REPLACE TEMP VIEW vw_customer_summary AS
SELECT customer_id,
       name,
       COUNT(order_id)      AS order_count,
       MIN(order_date_cast) AS first_order
FROM vw_customer_orders
GROUP BY customer_id, name;
""".strip())
