import os


def create_delta_table_only(
    spark,
    db: str,
    schema: str,
    table: str,
    data: list[tuple],
    columns: list[str],
    base_path: str = "/tmp/delta",
):
    """
    只建立 Delta table 檔案結構，不註冊 TempView。
    """
    full_path = os.path.join(base_path, db, schema, table)
    os.makedirs(full_path, exist_ok=True)

    df = spark.createDataFrame(data, columns)
    df.write.format("delta").option("overwriteSchema", "true").mode("overwrite").save(
        full_path
    )

    print(f"✅ Delta table created at: {full_path}")
