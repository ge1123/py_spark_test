# service.py
from pyspark.sql import SparkSession

# 集中定義所有 temp view 名稱（不使用物件語法）
VIEWS = {
    "VW_CUSTOMERS": "vw_customers",
    "VW_ORDERS": "vw_orders",
    "VW_CUSTOMER_ORDERS": "vw_customer_orders",
    "VW_CUSTOMER_SUMMARY": "vw_customer_summary"
}

# === Bronze Layer ===

def load_customers(spark: SparkSession):
    sql = "SELECT id, name FROM customers"
    df = spark.sql(sql)
    df.createOrReplaceTempView(VIEWS["VW_CUSTOMERS"])
    print(f"[Loaded: {VIEWS['VW_CUSTOMERS']}]\n{sql}")

def load_orders(spark: SparkSession):
    sql = "SELECT order_id, order_date, customer_id FROM orders"
    df = spark.sql(sql)
    df.createOrReplaceTempView(VIEWS["VW_ORDERS"])
    print(f"[Loaded: {VIEWS['VW_ORDERS']}]\n{sql}")

# === Silver Layer ===

def transform_customer_orders(spark: SparkSession):
    sql = f"""
        WITH tmp_joined_data AS (
            SELECT c.id AS customer_id, c.name, o.order_id, o.order_date
            FROM {VIEWS['VW_CUSTOMERS']} c
            JOIN {VIEWS['VW_ORDERS']} o ON c.id = o.customer_id
        ),
        tmp_enriched_data AS (
            SELECT *,
                   CAST(order_date AS DATE) AS order_date_cast,
                   YEAR(order_date) AS order_year
            FROM tmp_joined_data
        )
        SELECT * FROM tmp_enriched_data
    """
    df = spark.sql(sql)
    df.createOrReplaceTempView(VIEWS["VW_CUSTOMER_ORDERS"])
    print(f"[Transformed: {VIEWS['VW_CUSTOMER_ORDERS']}]\n{sql}")

# === Gold Layer ===

def summarize_customers(spark: SparkSession):
    sql = f"""
        SELECT customer_id, name, COUNT(order_id) AS order_count, MIN(order_date_cast) AS first_order
        FROM {VIEWS['VW_CUSTOMER_ORDERS']}
        GROUP BY customer_id, name
    """
    df = spark.sql(sql)
    df.createOrReplaceTempView(VIEWS["VW_CUSTOMER_SUMMARY"])
    print(f"[Summarized: {VIEWS['VW_CUSTOMER_SUMMARY']}]\n{sql}")

# === 清理暫存視圖 ===

def drop_temp_views(spark: SparkSession):
    for view_name in VIEWS.values():
        if spark.catalog.tableExists(view_name):
            spark.catalog.dropTempView(view_name)
            print(f"[View Dropped] {view_name}")

# === 主流程 ===

def run_etl(spark: SparkSession):
    # Step 1: Bronze Layer
    load_customers(spark)
    load_orders(spark)

    # Step 2: Silver Layer
    transform_customer_orders(spark)

    # Step 3: Gold Layer
    summarize_customers(spark)

    # Step 4: Output
    df = spark.sql(f"SELECT * FROM {VIEWS['VW_CUSTOMER_SUMMARY']}")
    print("\n✅ 客戶訂單彙總結果：")
    df.show()

    # Step 5: 清除暫存視圖
    drop_temp_views(spark)
