FROM bitnami/spark:3.4.2-debian-11-r0

# 切換為 root 身份建立使用者
USER root
RUN pip install pyspark==3.4.2 delta-spark==2.4.0 jinja2

# 建立使用者（bitnami 映像內已預設 spark 使用者 uid=1001）
RUN useradd -u 1001 -m sparkuser || true

# ✅ 建立 Hive Metastore 倉儲目錄，並給 sparkuser 權限
RUN mkdir -p /opt/spark-warehouse && chown -R 1001:1001 /opt/spark-warehouse

# 切回非 root 使用者執行
USER 1001

# 指定工作目錄
WORKDIR /opt/project
