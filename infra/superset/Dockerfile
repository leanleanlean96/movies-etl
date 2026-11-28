FROM apache/superset:6.1.0-lean

USER root
RUN uv pip install --no-cache clickhouse-connect==0.15.1 psycopg2-binary==2.9.12
COPY . /app/etl-pet/
ENV SUPERSET_CONFIG_PATH=/app/etl-pet/superset_config.py
USER superset
