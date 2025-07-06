# service.py
from pyspark.sql import SparkSession
from src.sql import crm_sql 


def run_all(spark: SparkSession):
    params = {"dbname": "test_crm_db"}

    print("▶ 建立 vw_customers")
    spark.sql(crm_sql.load_customers.render(**params))

    print("▶ 建立 vw_orders")
    spark.sql(crm_sql.load_orders.render(**params))

    print("▶ 建立 vw_customer_orders")
    spark.sql(crm_sql.transform_customer_orders.render(**params))

    print("▶ 建立 vw_customer_summary")
    spark.sql(crm_sql.summarize_customers.render(**params))

    print("✅ 所有 Temp View 建立完成")
