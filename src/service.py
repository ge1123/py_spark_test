# service.py
from pyspark.sql import SparkSession
from src.sql import crm_sql
import pandas as pd
from src.util.schema_validation import validate_against_delta_schema
import uuid


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


def download_csv(spark: SparkSession):
    pdf = pd.read_csv("opendata/123.csv")

    table_name = "test_crm_db.customers"
    validate_against_delta_schema(pdf, table_name, spark)

    df = spark.createDataFrame(pdf)
    
    temp_view = f"tmp_{uuid.uuid4().hex}"
    df.createOrReplaceTempView(temp_view)

    column_list = ", ".join(df.columns)

    spark.sql(f"""
              INSERT INTO {table_name} ({column_list})
              SELECT {column_list}
              FROM {temp_view}
              """)

    spark.sql(f"""select * from {table_name} """).show()
