# main.py
from pyspark.sql import SparkSession
from delta.pip_utils import configure_spark_with_delta_pip
from src import service
from src.util.spark_session import get_spark_session


def main():

    spark = get_spark_session("FullETLTest")

    service.download_csv(spark)


    spark.stop()


if __name__ == "__main__":
    main()
