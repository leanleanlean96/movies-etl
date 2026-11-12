# Python version is pinned to match the local uv environment (.python-version).
ARG AIRFLOW_VERSION=3.3.0
ARG PYTHON_VERSION=3.11
FROM apache/airflow:${AIRFLOW_VERSION}-python${PYTHON_VERSION}

ARG AIRFLOW_VERSION=3.3.0
ARG PYTHON_VERSION=3.11
# Official constraints keep transitive dependencies compatible with this Airflow release.
RUN pip install --no-cache-dir \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt" \
    "apache-airflow==${AIRFLOW_VERSION}" \
    "apache-airflow-providers-amazon==9.31.0" \
    "apache-airflow-providers-docker==4.5.7" \
    "apache-airflow-providers-http==6.0.4" \
    "apache-airflow-providers-smtp==3.0.1" \
    "boto3>=1.35,<2" \
    "requests>=2.32,<3"
