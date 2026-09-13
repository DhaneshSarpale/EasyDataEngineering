"""AWS-MODE reference: the same extraction patterns over JDBC / RDS.

This module is a *reference* (not executed in LOCAL MODE). It shows how the
identical full/incremental patterns look against a real PostgreSQL/MySQL
source using:

- **psycopg2 / SQLAlchemy** for a plain Python extractor, and
- **AWS Glue DynamicFrame** via ``create_dynamic_frame.from_options`` /
  JDBC connection for a serverless Spark extractor.

Credentials are resolved from AWS Secrets Manager - never hardcoded.
"""

from __future__ import annotations

PYTHON_JDBC_EXAMPLE = r"""
import pandas as pd
from sqlalchemy import create_engine, text
from dataforge.common.secrets import get_secret_dict

def make_engine(secret_name: str, region: str = "eu-west-1"):
    s = get_secret_dict(secret_name, region)   # {host, port, dbname, username, password}
    url = f"postgresql+psycopg2://{s['username']}:{s['password']}@{s['host']}:{s['port']}/{s['dbname']}"
    return create_engine(url, pool_pre_ping=True)

def full_extract(engine, table: str) -> pd.DataFrame:
    return pd.read_sql(text(f"SELECT * FROM {table}"), engine)

def incremental_extract(engine, table: str, last_ts: str) -> pd.DataFrame:
    q = text(f"SELECT * FROM {table} WHERE updated_at > :ts ORDER BY updated_at")
    return pd.read_sql(q, engine, params={"ts": last_ts})
"""

GLUE_JDBC_EXAMPLE = r"""
# Runs inside an AWS Glue job. The JDBC connection "dataforge-rds" is
# defined in the Glue Data Catalog and references Secrets Manager for creds.
from awsglue.context import GlueContext
from pyspark.context import SparkContext

glue = GlueContext(SparkContext.getOrCreate())

dyf = glue.create_dynamic_frame.from_options(
    connection_type="postgresql",
    connection_options={
        "useConnectionProperties": "true",
        "connectionName": "dataforge-rds",
        "dbtable": "public.customers",
    },
)

dyf_incr = glue.create_dynamic_frame.from_options(
    connection_type="postgresql",
    connection_options={
        "useConnectionProperties": "true",
        "connectionName": "dataforge-rds",
        "dbtable": "(SELECT * FROM public.transactions WHERE updated_at > '{last_ts}') AS t",
    },
)

glue.write_dynamic_frame.from_options(
    frame=dyf_incr,
    connection_type="s3",
    connection_options={"path": "s3://dataforge-dev-bronze/transactions/", "partitionKeys": ["year", "month", "day"]},
    format="parquet",
    format_options={"compression": "snappy"},
)
"""

FULL_SQL = "SELECT * FROM {table};"
INCREMENTAL_TS_SQL = "SELECT * FROM {table} WHERE updated_at > '{last_success_timestamp}';"
INCREMENTAL_ID_SQL = "SELECT * FROM {table} WHERE {id_col} > '{last_processed_id}';"
