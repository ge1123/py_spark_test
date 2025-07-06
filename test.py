from pyspark.sql import SparkSession
from delta.pip_utils import configure_spark_with_delta_pip
from create_mock_delta_structure import init_mock_delta_data
import main  # 假設 main.py 有 main(spark)
import os

print("👀 /opt/spark-warehouse 存在？", os.path.exists("/opt/spark-warehouse"))
print("📂 權限：", oct(os.stat("/opt/spark-warehouse").st_mode)[-3:])
print("👤 擁有者：", os.stat("/opt/spark-warehouse").st_uid)
builder = (
    SparkSession.builder.appName("FullETLTest")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
    .config("spark.sql.catalogImplementation", "hive")
    .config("spark.sql.warehouse.dir", "/opt/spark-warehouse")  # 與 compose 保持一致
    .enableHiveSupport()
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# 初始化 mock 資料
init_mock_delta_data(spark)

# 註冊 TempViews（你可以額外寫一支 register_view.py 來處理）
spark.sql("USE test_crm_db")
spark.sql("SELECT * FROM customers").show()

# 執行主程式邏輯
main.main()

spark.stop()
