import re
from pathlib import Path
from jinja2 import Template
from pyspark.sql import SparkSession
import util.easy_run


def run_etl(spark):
    params = {"with_view": True, "yyyymm": "202506"}

    # 取得所有 STEP
    steps = util.easy_run.parse_etl_steps("etl.sql")

    # 逐步執行每個 STEP
    for step_name, step_sql in steps:
        util.easy_run.execute_step(spark, step_name, step_sql, params)


def run_etl2(spark):
    params = {"with_view": True, "yyyymm": "202506"}

    util.easy_run.run_single_step(spark, "etl.sql", "LOAD_CUSTOMERS", params)
    spark.table("vw_customers").cache()

    util.easy_run.run_single_step(spark, "etl.sql", "LOAD_ORDERS", params)
    util.easy_run.run_single_step(spark, "etl.sql", "TRANSFORM_CUSTOMER_ORDERS", params)
    util.easy_run.run_single_step(spark, "etl.sql", "SUMMARIZE_CUSTOMERS", params)
    spark.sql("select * from vw_customer_summary").show()
    util.easy_run.run_single_step(spark, "etl.sql", "INSERT_INTO", params)


def easy_run(spark):
    util.easy_run.run_sql_template(
        spark, sql_file_path="etl.sql", params={"with_view": True, "yyyymm": "202506"}
    )
