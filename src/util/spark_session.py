from pyspark.sql import SparkSession
from delta.pip_utils import configure_spark_with_delta_pip

def get_spark_session(app_name: str = "SparkApp") -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.catalogImplementation", "hive")
        .config("spark.sql.warehouse.dir", "/opt/spark-warehouse")  # 與 docker-compose 對齊
        .enableHiveSupport()
    )

    return configure_spark_with_delta_pip(builder).getOrCreate()
