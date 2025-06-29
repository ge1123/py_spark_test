# ✅ 啟動 Spark 容器（使用 docker-compose）
```bash
docker compose -f docker-compose.spark.yml up --build -d
```

# ✅ 進入 Spark 容器
```bash
docker exec -it spark bash
```

# ✅ 切換到專案目錄，執行 ETL 腳本（使用 Python）
```bash
cd /opt/project
python test.py
```

# ✅ 關閉 Spark 容器
```bash
docker compose -f docker-compose.spark.yml down
```

# ✅ 重新建構 Spark 容器（清除快取）
```bash
docker compose -f docker-compose.spark.yml build --no-cache
```

# ✅ 安裝指定版本（在容器內執行）
```bash
pip install pyspark==3.4.2 delta-spark==2.4.0
```

# ✅ 檢查環境版本（在容器內）
python --version
pyspark --version
pip show delta-spark
spark-shell --version

# ✅ 測試 delta-core 是否被 ivy 下載（可選）
find ~/.ivy2.5.2 -name "delta-*.jar"
<!-- 
# 🚫 用不到的（已棄用，僅備查）
# spark-submit \
#   --packages io.delta:delta-core_2.12:2.4.0 \
#   --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
#   --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
#   --conf spark.hadoop.delta.logStore.class=org.apache.spark.sql.delta.storage.LocalLogStore \
#   test.py

# spark-submit $SPARK_SUBMIT_OPTIONS test.py -->



| 方法名稱     | 是否自動跑所有步驟 | 可指定步驟執行 | 顯示 lineage | 支援 cache 操作 | 適合對象         | 備註                        |
|--------------|--------------------|----------------|---------------|------------------|------------------|-----------------------------|
| `run_all`    | ✅ 是              | ❌ 否          | ✅ 會顯示     | ❌ 不包含        | 分析師／簡易驗證 | 每個 step 順序自動執行      |
| `run_steps`  | ✅（手動列出）     | ✅ 是          | ✅ 會顯示     | ✅ 支援          | 開發者／除錯     | 可插入 cache 操作等邏輯     |
| `run_sql`    | ✅ 是              | ❌ 否          | ❌ 不顯示     | ❌ 不包含        | 分析師／快速執行 | 直接用分號切整份 SQL 執行   |
