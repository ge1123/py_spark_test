from pyspark.sql import SparkSession
import pandas as pd

def validate_against_delta_schema(df: pd.DataFrame, delta_table: str, spark: SparkSession):
    delta_schema = spark.table(delta_table).schema

    # 只驗證有在 df 裡的欄位
    errors = []
    for field in delta_schema:
        col = field.name
        if col not in df.columns:
            continue  # 或視需求報錯

        delta_type = field.dataType.typeName()
        pandas_type = str(df.dtypes[col])

        # 判斷相容性
        if delta_type == "string" and pandas_type in ["object", "string"]:
            continue
        elif delta_type in ["int", "long"] and pandas_type == "int64":
            continue
        elif delta_type in ["float", "double"] and pandas_type == "float64":
            continue
        elif delta_type == "boolean" and pandas_type == "bool":
            continue
        elif delta_type == "timestamp" and "datetime" in pandas_type:
            continue
        else:
            errors.append(f"❌ 欄位 {col} 型別不符，Delta: {delta_type}, Pandas: {pandas_type}")

    if errors:
        print("\n".join(errors))
        raise TypeError("❌ 資料型別驗證失敗")
    else:
        print("✅ 所有欄位型別驗證通過")