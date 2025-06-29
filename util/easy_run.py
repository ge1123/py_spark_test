# util/easy_run.py
import re
from pathlib import Path
from typing import List, Tuple, Dict, Optional
from jinja2 import Template
from pyspark.sql import SparkSession


def run_sql_template(spark: SparkSession, sql_file_path: str, params: dict):
    sql_text = Path(sql_file_path).read_text(encoding="utf-8")
    final_sql = Template(sql_text).render(**params)

    print(f"🚀 執行 SQL 檔案: {sql_file_path}")
    print("------------ SQL ------------")
    print(final_sql)
    print("-----------------------------")

    sql_statements = [s.strip() for s in final_sql.strip().split(";") if s.strip()]
    if not sql_statements:
        raise ValueError("❌ SQL 模板執行失敗：沒有有效 SQL。請確認以分號結尾。")

    for i, stmt in enumerate(sql_statements, start=1):
        try:
            print(f"\n▶ 執行第 {i} 段 SQL：")
            print(stmt)
            spark.sql(stmt)
        except Exception as e:
            print(f"❌ 第 {i} 段 SQL 執行錯誤：{e}")
            raise


def parse_etl_steps(
    sql_file_path: str,
) -> List[Tuple[str, str, Optional[str], List[str]]]:
    sql_text = Path(sql_file_path).read_text(encoding="utf-8")

    step_blocks = re.findall(
        r"--\s*\[STEP:\s*(.*?)\s*\](.*?)(?=--\s*\[STEP:|\Z)",
        sql_text,
        re.DOTALL,
    )

    result = []
    for step_name, step_sql in step_blocks:
        if " " in step_name.strip():
            raise ValueError(f"❌ STEP 名稱不合法：`{step_name}` 含空格，請使用底線。")
        output_view, input_views = extract_lineage_from_sql(step_sql)
        result.append((step_name.strip(), step_sql.strip(), output_view, input_views))

    # 驗證所有 SQL 都在 STEP 區塊中
    raw_sql_without_steps = re.sub(r"--\s*\[STEP:\s*.*?\s*\]", "", sql_text)
    all_sql_stmts = [
        s.strip() for s in raw_sql_without_steps.strip().split(";") if s.strip()
    ]
    if len(all_sql_stmts) > len(result):
        raise ValueError(
            f"❌ 檢查失敗：偵測到 {len(all_sql_stmts)} 段 SQL，但只有 {len(result)} 個 STEP。\n"
            f"請檢查每段 SQL 是否都有對應的 -- [STEP: XXX] 標籤。"
        )

    return result


def extract_lineage_from_sql(sql: str) -> Tuple[Optional[str], List[str]]:
    output_match = re.search(
        r"CREATE\s+(?:OR\s+REPLACE\s+)?TEMP\s+VIEW\s+(\w+)", sql, re.IGNORECASE
    )
    output_view = output_match.group(1) if output_match else None

    input_views = re.findall(r"(?:FROM|JOIN)\s+(\w+)", sql, re.IGNORECASE)
    return output_view, list(set(input_views))  # 去重複


def execute_step(
    spark: SparkSession, step_name: str, step_sql: str, params: Dict
) -> bool:
    print(f"\n🚀 STEP: {step_name.strip()}")
    try:
        final_sql = Template(step_sql.strip()).render(**params)
        print(final_sql)
        spark.sql(final_sql)
        return True
    except Exception as e:
        print(f"❌ STEP {step_name.strip()} 執行錯誤：{e}")
        return False


def get_step_by_name(
    sql_file_path: str, target_step: str
) -> Tuple[str, str, Optional[str], List[str]]:
    steps = parse_etl_steps(sql_file_path)
    for step_name, step_sql, output_view, input_views in steps:
        if step_name.strip().upper() == target_step.strip().upper():
            return step_name, step_sql, output_view, input_views
    raise ValueError(f"❌ 找不到 STEP: {target_step}")


def run_single_step(
    spark: SparkSession, sql_file_path: str, step_name: str, params: Dict
):
    step_name, step_sql, output_view, input_views = get_step_by_name(
        sql_file_path, step_name
    )
    success = execute_step(spark, step_name, step_sql, params)
    if success:
        print(
            f"🔗 Lineage: {output_view or '(none)'} <- {', '.join(input_views) or '(none)'}"
        )
    return success
