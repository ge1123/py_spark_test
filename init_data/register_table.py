def register_delta_table(spark, db: str, table: str, location: str):
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {db}")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {db}.{table}
        USING DELTA
        LOCATION '{location}'
    """
    )
    print(f"🔗 Registered table: {db}.{table} at {location}")
