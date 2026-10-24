FROM python:3.11-slim
RUN pip install --no-cache-dir "dbt-core==1.10.23" "dbt-postgres==1.10.2"
WORKDIR /workspace/dbt
ENTRYPOINT ["dbt"]
