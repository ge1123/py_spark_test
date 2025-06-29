from pathlib import Path
from jinja2 import Template
from pyspark.sql import SparkSession
import re
from typing import List, Tuple, Dict


def run_sql_template(spark: SparkSession, sql_file_path: str, params: dict):
    """
    執行整份 SQL 模板（使用 Jinja2 渲染），並以 ; 拆段執行。
    若格式錯誤或有語法錯誤會顯示清楚。
    """
    sql_text = Path(sql_file_path).read_text(encoding="utf-8")
    final_sql = Template(sql_text).render(**params)

    print(f"🚀 執行 SQL 檔案: {sql_file_path}")
    print("------------ SQL ------------")
    print(final_sql)
    print("-----------------------------")

    # 檢查至少有一段 SQL
    sql_statements = [s.strip() for s in final_sql.strip().split(";") if s.strip()]
    if not sql_statements:
        raise ValueError(
            "❌ SQL 模板執行失敗：未找到任何有效 SQL 語句。請確認是否以分號結尾。"
        )

    # 執行每段 SQL，若失敗就印出是哪一段錯
    for i, stmt in enumerate(sql_statements, start=1):
        try:
            print(f"\n▶ 執行第 {i} 段 SQL：")
            print(stmt)
            spark.sql(stmt)
        except Exception as e:
            print(f"❌ 第 {i} 段 SQL 執行錯誤：{e}")
            raise


def parse_etl_steps(sql_file_path: str) -> List[Tuple[str, str]]:
    """
    拆解 SQL 檔案，取得所有 STEP 名稱與對應 SQL 內容。
    若偵測有未包裝進 STEP 的 SQL 區段或 STEP 名稱含空格，會拋出錯誤。
    """
    sql_text = Path(sql_file_path).read_text(encoding="utf-8")

    # 找出所有 STEP 區塊
    step_blocks = re.findall(
        r"--\s*\[STEP:\s*(.*?)\s*\](.*?)(?=--\s*\[STEP:|\Z)",
        sql_text,
        re.DOTALL,
    )

    # 🔒 檢查每個 step name 是否含有空格
    for step_name, _ in step_blocks:
        if " " in step_name.strip():
            raise ValueError(
                f"❌ STEP 名稱不合法：`{step_name}` 含有空白，請改為例如 INSERT_INTO 或 VW_CUSTOMERS"
            )

    # 檢查所有 SQL 區段總量 ≈ STEP 區塊數
    raw_sql_without_steps = re.sub(r"--\s*\[STEP:\s*.*?\s*\]", "", sql_text)
    all_sql_stmts = [
        s.strip() for s in raw_sql_without_steps.strip().split(";") if s.strip()
    ]

    if len(all_sql_stmts) > len(step_blocks):
        raise ValueError(
            f"❌ 檢查失敗：偵測到 {len(all_sql_stmts)} 段 SQL，但只有 {len(step_blocks)} 個 STEP。\n"
            f"請確認所有 SQL 都有對應的 `-- [STEP: NAME]` 區塊標籤，且格式正確。"
        )

    return step_blocks


def execute_step(
    spark: SparkSession, step_name: str, step_sql: str, params: Dict
) -> bool:
    """
    執行單一 STEP。傳回 True 表示成功，False 表示錯誤。
    """
    print(f"\n🚀 STEP: {step_name.strip()}")
    try:
        final_sql = Template(step_sql.strip()).render(**params)
        print(final_sql)
        spark.sql(final_sql)
        return True
    except Exception as e:
        print(f"❌ STEP {step_name.strip()} 執行錯誤：{e}")
        return False


def get_step_by_name(sql_file_path: str, target_step: str) -> Tuple[str, str]:
    """
    根據 STEP 名稱從 SQL 檔案中擷取對應的 step_name 和 step_sql。
    若找不到則 raise 錯誤。
    """
    steps = parse_etl_steps(sql_file_path)
    for step_name, step_sql in steps:
        if step_name.strip().upper() == target_step.strip().upper():
            return step_name, step_sql
    raise ValueError(f"❌ 找不到 STEP: {target_step}")


def run_single_step(
    spark: SparkSession, sql_file_path: str, step_name: str, params: Dict
):
    """
    從 SQL 檔案中抓出指定 STEP，並執行它。
    """
    matched_step_name, step_sql = get_step_by_name(sql_file_path, step_name)
    return execute_step(spark, matched_step_name, step_sql, params)
