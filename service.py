from pyspark.sql import SparkSession
from context import get_spark, get_env

spark: SparkSession = None
view_list = []
VW_CUSTOMERS = "vw_customers"
VW_ORDERS = "vw_orders"
VW_CUSTOMER_ORDERS = "vw_customer_orders"
VW_CUSTOMER_SUMMARY = "vw_customer_summary"

def set_spark(spark_session: SparkSession):
    global spark
    spark = spark_session

# Bronze SQLs
def get_customer_sql():
    return f"""
        SELECT id, name
        FROM customers
    """

def get_orders_sql():
    return f"""
        SELECT order_id, order_date, customer_id
        FROM orders
    """

# Silver SQL
def get_enriched_order_data_sql():
    return f"""
        WITH tmp_joined_data AS (
            SELECT c.id AS customer_id, c.name, o.order_id, o.order_date
            FROM {VW_CUSTOMERS} c
            JOIN {VW_ORDERS} o ON c.id = o.customer_id
        ),
        tmp_enriched_data AS (
            SELECT *,
                CAST(order_date AS DATE) AS order_date_cast,
                YEAR(order_date) AS order_year
            FROM tmp_joined_data
        )
        SELECT * FROM tmp_enriched_data
    """

# Gold SQL
def get_customer_summary_sql():
    return f"""
        SELECT customer_id, name, COUNT(order_id) AS order_count, MIN(order_date_cast) AS first_order
        FROM {VW_CUSTOMER_ORDERS}
        GROUP BY customer_id, name
    """

def register_temp_view(sql_func, view_name: str):
    sql = sql_func()
    df = spark.sql(sql)
    df.createOrReplaceTempView(view_name)
    view_list.append(view_name)
    print(sql)

def clear_temp_view():
    for view in view_list:
        if spark.catalog.tableExists(view):
            spark.catalog.dropTempView(view)

def run_etl():
    # Step 1: Bronze
    register_temp_view(get_customer_sql, VW_CUSTOMERS)
    register_temp_view(get_orders_sql, VW_ORDERS)

    # Step 2: Silver
    register_temp_view(get_enriched_order_data_sql, VW_CUSTOMER_ORDERS)

    # Step 3: Gold
    register_temp_view(get_customer_summary_sql, VW_CUSTOMER_SUMMARY)

    # Step 4: 最終輸出
    df = spark.sql("SELECT * FROM vw_customer_summary")
    print("✅ 清洗後的客戶訂單彙總")
    df.show()

    # Step 5: 清理暫存 View
    clear_temp_view()
