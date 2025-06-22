# main.py
from pyspark.sql import SparkSession
from delta.pip_utils import configure_spark_with_delta_pip
import service
from etl_context import ETLContext  # ⬅️ 確保這一行有加

def main():
    builder = SparkSession.builder \
        .appName("RunMainETL") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    service.run_etl(spark)

 
    spark.stop()

if __name__ == "__main__":
    main()
