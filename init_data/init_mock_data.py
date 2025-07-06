from pyspark.sql import SparkSession
from delta.pip_utils import configure_spark_with_delta_pip

from init_data.create_table import create_delta_table_only
from init_data.register_table import register_delta_table


def init_mock_delta_data(spark):
    base_path = "/tmp/delta"

    create_delta_table_only(
        spark,
        db="test",
        schema="crm_db",
        table="customers",
        data=[
            (1, "Alice", "Taipei"),
            (2, "Bob", "Kaohsiung"),
            (3, "Carol", "Taichung"),
        ],
        columns=["id", "name", "city"],
        base_path=base_path,
    )
    register_delta_table(
        spark, "test_crm_db", "customers", f"{base_path}/test/crm_db/customers"
    )

    create_delta_table_only(
        spark,
        db="test",
        schema="crm_db",
        table="orders",
        data=[(101, 1, "2024-01-01"), (102, 2, "2024-02-01"), (103, 1, "2024-03-15")],
        columns=["order_id", "customer_id", "order_date"],
        base_path=base_path,
    )
    register_delta_table(
        spark, "test_crm_db", "orders", f"{base_path}/test/crm_db/orders"
    )

    create_delta_table_only(
        spark,
        db="test",
        schema="marketing",
        table="campaigns",
        data=[(201, "Summer Sale", "2024-06-01"), (202, "Black Friday", "2024-11-29")],
        columns=["campaign_id", "title", "start_date"],
        base_path=base_path,
    )
    register_delta_table(
        spark, "test_marketing", "campaigns", f"{base_path}/test/marketing/campaigns"
    )


