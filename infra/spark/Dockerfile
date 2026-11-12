FROM spark:4.2.0-python3

USER root

# Spark 4.2.0 ships with Hadoop 3.5.0 (spark pom.xml). S3A needs the matching
# connector and the AWS SDK v2 bundle pinned by hadoop-project 3.5.0 (2.35.4).
ARG HADOOP_VERSION=3.5.0
ARG AWS_SDK_VERSION=2.35.4
ARG POSTGRES_JDBC_VERSION=42.7.14
RUN set -eu; \
    wget -q "https://repo.maven.apache.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_VERSION}/hadoop-aws-${HADOOP_VERSION}.jar" -O "/opt/spark/jars/hadoop-aws-${HADOOP_VERSION}.jar"; \
    wget -q "https://repo.maven.apache.org/maven2/software/amazon/awssdk/bundle/${AWS_SDK_VERSION}/bundle-${AWS_SDK_VERSION}.jar" -O "/opt/spark/jars/bundle-${AWS_SDK_VERSION}.jar"; \
    wget -q "https://repo.maven.apache.org/maven2/org/postgresql/postgresql/${POSTGRES_JDBC_VERSION}/postgresql-${POSTGRES_JDBC_VERSION}.jar" -O "/opt/spark/jars/postgresql-${POSTGRES_JDBC_VERSION}.jar"; \
    test -s "/opt/spark/jars/hadoop-aws-${HADOOP_VERSION}.jar"; \
    test -s "/opt/spark/jars/bundle-${AWS_SDK_VERSION}.jar"; \
    test -s "/opt/spark/jars/postgresql-${POSTGRES_JDBC_VERSION}.jar"

ARG S3_ACCELERATOR_VERSION=1.3.1
RUN set -eu; \
    wget -q "https://repo.maven.apache.org/maven2/software/amazon/s3/analyticsaccelerator/analyticsaccelerator-s3/${S3_ACCELERATOR_VERSION}/analyticsaccelerator-s3-${S3_ACCELERATOR_VERSION}.jar" -O "/opt/spark/jars/analyticsaccelerator-s3-${S3_ACCELERATOR_VERSION}.jar"; \
    test -s "/opt/spark/jars/analyticsaccelerator-s3-${S3_ACCELERATOR_VERSION}.jar"

# ClickHouse JDBC (shaded) for the Cloudberry -> ClickHouse Spark job.
ARG CLICKHOUSE_JDBC_VERSION=0.11.0
RUN set -eu; \
    wget -q "https://repo.maven.apache.org/maven2/com/clickhouse/clickhouse-jdbc/${CLICKHOUSE_JDBC_VERSION}/clickhouse-jdbc-${CLICKHOUSE_JDBC_VERSION}-all.jar" -O "/opt/spark/jars/clickhouse-jdbc-${CLICKHOUSE_JDBC_VERSION}-all.jar"; \
    test -s "/opt/spark/jars/clickhouse-jdbc-${CLICKHOUSE_JDBC_VERSION}-all.jar"

# S3A settings for MinIO; credentials come from AWS_* environment variables.
COPY spark-defaults.conf /opt/spark/conf/spark-defaults.conf

USER spark
