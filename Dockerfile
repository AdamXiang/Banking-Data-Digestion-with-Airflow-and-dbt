# Dockerfile
FROM apache/airflow:2.9.3

USER root

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# Switch to airflow user first
USER airflow

# Set up env variable
ENV PYTHONWARNINGS="ignore::core.CryptographyDeprecationWarning,ignore::SyntaxWarning"

# Install dbt packages
RUN uv pip install --no-cache-dir \
    dbt-core \
    dbt-snowflake \
    apache-airflow-providers-snowflake