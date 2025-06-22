from pyspark.sql import SparkSession


class ETLContext:
    def __init__(self, spark: SparkSession, env: str = "dev"):
        self.spark = spark
        self.env = env
        self.view_list = []

    def register_temp_view(self, sql_func, view_name: str):
        sql = sql_func()
        df = self.spark.sql(sql)
        df.createOrReplaceTempView(view_name)
        self.view_list.append(view_name)

    def clear_views(self):
        for view in self.view_list:
            if self.spark.catalog.tableExists(view):
                self.spark.catalog.dropTempView(view)
