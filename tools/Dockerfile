# Инструменты лабораторной: Python-клиент Trino, PySpark 3.5 + Iceberg (REST + S3), openpyxl.
FROM python:3.12-slim-bookworm

RUN mkdir -p /usr/share/man/man1 \
 && apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless curl procps \
 && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir pyspark==3.5.6 trino==0.340.0 openpyxl==3.1.5

# Iceberg для Spark 3.5: runtime (каталог, чтение таблиц) + aws-bundle (S3FileIO для MinIO)
ARG ICEBERG_VERSION=1.9.2
RUN mkdir -p /opt/jars \
 && curl -fsSL --retry 5 -o /opt/jars/iceberg-spark-runtime.jar \
    https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar \
 && curl -fsSL --retry 5 -o /opt/jars/iceberg-aws-bundle.jar \
    https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar

ENV ICEBERG_VERSION=${ICEBERG_VERSION} PYTHONUNBUFFERED=1
WORKDIR /work
ENTRYPOINT ["python", "/work/tools/lab.py"]
