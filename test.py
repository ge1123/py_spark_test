from pyspark.sql import SparkSession
from delta.pip_utils import configure_spark_with_delta_pip
from create_mock_delta_structure import init_mock_delta_data
import main  # 假設 main.py 有 main(spark)

builder = SparkSession.builder \
    .appName("FullETLTest") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# 初始化 mock 資料
init_mock_delta_data(spark)

# 註冊 TempViews（你可以額外寫一支 register_view.py 來處理）
spark.read.format("delta").load("/tmp/delta/test/crm_db/customers") \
    .createOrReplaceTempView("customers")
spark.read.format("delta").load("/tmp/delta/test/crm_db/orders") \
    .createOrReplaceTempView("orders")

# 執行主程式邏輯
main.main()

spark.stop()
