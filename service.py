# service.py
import util.easy_run
from pyspark.sql import SparkSession


def run_all(spark: SparkSession):
    params = {"with_view": True, "yyyymm": "202506"}

    steps = util.easy_run.parse_etl_steps("etl.sql")
    for step_name, step_sql, output_view, input_views in steps:
        print(
            f"\n🔗 Lineage: {output_view or '(none)'} <- {', '.join(input_views) or '(none)'}"
        )
        util.easy_run.execute_step(spark, step_name, step_sql, params)


def run_steps(spark: SparkSession):
    params = {"with_view": True, "yyyymm": "202506"}

    util.easy_run.run_single_step(spark, "etl.sql", "LOAD_CUSTOMERS", params)
    spark.table("vw_customers").cache()

    util.easy_run.run_single_step(spark, "etl.sql", "LOAD_ORDERS", params)
    util.easy_run.run_single_step(spark, "etl.sql", "TRANSFORM_CUSTOMER_ORDERS", params)
    util.easy_run.run_single_step(spark, "etl.sql", "SUMMARIZE_CUSTOMERS", params)

    spark.sql("SELECT * FROM vw_customer_summary").show()

    util.easy_run.run_single_step(spark, "etl.sql", "INSERT_INTO", params)


def run_sql(spark: SparkSession):
    params = {"with_view": True, "yyyymm": "202506"}
    util.easy_run.run_sql_template(spark, sql_file_path="etl.sql", params=params)
