from pyspark.sql import SparkSession
from delta.pip_utils import configure_spark_with_delta_pip
import os


def create_delta_table_only(
    spark,
    db: str,
    schema: str,
    table: str,
    data: list[tuple],
    columns: list[str],
    base_path: str = "/tmp/delta",
):
    """
    只建立 Delta table 檔案結構，不註冊 TempView。
    """
    full_path = os.path.join(base_path, db, schema, table)
    os.makedirs(full_path, exist_ok=True)

    df = spark.createDataFrame(data, columns)
    df.write.format("delta").option("overwriteSchema", "true").mode("overwrite").save(
        full_path
    )

    print(f"✅ Delta table created at: {full_path}")


def init_mock_delta_data(spark):
    base_path = "/tmp/delta"

    create_delta_table_only(
        spark,
        db="test",
        schema="crm_db",
        table="customers",
        data=[
            (1, "Alice", "Taipei"),
            (2, "Bob", "Kaohsiung"),
            (3, "Carol", "Taichung"),
        ],
        columns=["id", "name", "city"],
        base_path=base_path,
    )
    register_delta_table(
        spark, "test_crm_db", "customers", f"{base_path}/test/crm_db/customers"
    )

    create_delta_table_only(
        spark,
        db="test",
        schema="crm_db",
        table="orders",
        data=[(101, 1, "2024-01-01"), (102, 2, "2024-02-01"), (103, 1, "2024-03-15")],
        columns=["order_id", "customer_id", "order_date"],
        base_path=base_path,
    )
    register_delta_table(
        spark, "test_crm_db", "orders", f"{base_path}/test/crm_db/orders"
    )

    create_delta_table_only(
        spark,
        db="test",
        schema="marketing",
        table="campaigns",
        data=[(201, "Summer Sale", "2024-06-01"), (202, "Black Friday", "2024-11-29")],
        columns=["campaign_id", "title", "start_date"],
        base_path=base_path,
    )
    register_delta_table(
        spark, "test_marketing", "campaigns", f"{base_path}/test/marketing/campaigns"
    )


def register_delta_table(spark, db: str, table: str, location: str):
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {db}")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {db}.{table}
        USING DELTA
        LOCATION '{location}'
    """
    )
    print(f"🔗 Registered table: {db}.{table} at {location}")
