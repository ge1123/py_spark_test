from pyspark.sql import SparkSession
from delta.pip_utils import configure_spark_with_delta_pip
from init_data.init_mock_data import init_mock_delta_data
from src import main
from src.util.spark_session import get_spark_session
import os

print("👀 /opt/spark-warehouse 存在？", os.path.exists("/opt/spark-warehouse"))
print("📂 權限：", oct(os.stat("/opt/spark-warehouse").st_mode)[-3:])
print("👤 擁有者：", os.stat("/opt/spark-warehouse").st_uid)
spark = get_spark_session("FullETLTest")

# 初始化 mock 資料
init_mock_delta_data(spark)

# 執行主程式邏輯
main.main()

spark.stop()
