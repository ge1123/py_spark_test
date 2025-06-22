from pyspark.sql import SparkSession
from delta.pip_utils import configure_spark_with_delta_pip
import os

def create_delta_table_only(spark, db: str, schema: str, table: str, data: list[tuple], columns: list[str], base_path: str = "/tmp/delta"):
    """
    只建立 Delta table 檔案結構，不註冊 TempView。
    """
    full_path = os.path.join(base_path, db, schema, table)
    os.makedirs(full_path, exist_ok=True)

    df = spark.createDataFrame(data, columns)
    df.write.format("delta") \
        .option("overwriteSchema", "true") \
        .mode("overwrite") \
        .save(full_path)

    print(f"✅ Delta table created at: {full_path}")

def init_mock_delta_data(spark):
    """
    初始化所有 mock Delta table 的資料。
    """
    # ✅ 模擬 CRM 資料
    create_delta_table_only(
        spark,
        db="test",
        schema="crm_db",
        table="customers",
        data=[
            (1, "Alice", "Taipei"),
            (2, "Bob", "Kaohsiung"),
            (3, "Carol", "Taichung")
        ],
        columns=["id", "name", "city"]
    )

    create_delta_table_only(
        spark,
        db="test",
        schema="crm_db",
        table="orders",
        data=[
            (101, 1, "2024-01-01"),
            (102, 2, "2024-02-01"),
            (103, 1, "2024-03-15")
        ],
        columns=["order_id", "customer_id", "order_date"]
    )

    # ✅ 模擬行銷活動資料
    create_delta_table_only(
        spark,
        db="test",
        schema="marketing",
        table="campaigns",
        data=[
            (201, "Summer Sale", "2024-06-01"),
            (202, "Black Friday", "2024-11-29")
        ],
        columns=["campaign_id", "title", "start_date"]
    )
