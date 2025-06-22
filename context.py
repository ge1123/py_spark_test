# context.py
from pyspark.sql import SparkSession

_spark: SparkSession = None
_env: str = None

def set_context(spark_session: SparkSession, env: str = "dev"):
    global _spark, _env
    _spark = spark_session
    _env = env

def get_spark() -> SparkSession:
    if _spark is None:
        raise RuntimeError("SparkSession 尚未初始化，請先呼叫 set_context()")
    return _spark

def get_env() -> str:
    return _env or "dev"
