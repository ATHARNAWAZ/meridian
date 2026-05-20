FROM python:3.10-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl git \
    && rm -rf /var/lib/apt/lists/*

# Install uv
RUN pip install uv

COPY pyproject.toml .
COPY meridian/ ./meridian/
COPY libs/ ./libs/
COPY dbt_project/ ./dbt_project/
COPY duckdb/ ./duckdb/
COPY dashboard/ ./dashboard/
COPY quarantine/ ./quarantine/
COPY iceberg/ ./iceberg/

RUN uv pip install --system -e "libs/quarantine_dq"
RUN uv pip install --system streamlit plotly duckdb pyiceberg boto3 prometheus-client kafka-python python-dotenv rich pydantic fastavro

EXPOSE 8501

HEALTHCHECK CMD curl --fail http://localhost:8501/_stcore/health || exit 1

CMD ["streamlit", "run", "dashboard/app.py", "--server.port=8501", "--server.address=0.0.0.0"]
