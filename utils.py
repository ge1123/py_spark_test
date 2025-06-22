# utils.py
from functools import wraps
from typing import Callable
from pyspark.sql import DataFrame

# 提供外部模組使用的註冊清單
_registered_views = []

def register_view(view_name: str):
    """
    裝飾器：將 DataFrame 註冊為 TempView，並記錄到全域變數中
    """
    def decorator(func: Callable):
        @wraps(func)
        def wrapper(*args, **kwargs) -> DataFrame:
            df = func(*args, **kwargs)
            if not isinstance(df, DataFrame):
                raise ValueError(f"{func.__name__} 必須回傳 DataFrame")
            df.createOrReplaceTempView(view_name)
            _registered_views.append((view_name, func.__name__))
            return df
        return wrapper
    return decorator

def get_registered_views():
    """
    回傳已註冊的 TempView 名稱與對應來源函數
    """
    return _registered_views