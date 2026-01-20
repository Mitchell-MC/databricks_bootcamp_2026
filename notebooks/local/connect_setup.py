"""Databricks Connect configuration and setup.

This module provides a SparkSession connected to your Databricks cluster.

## Setup Instructions

1. Set your DATABRICKS_TOKEN environment variable:
   ```powershell
   $env:DATABRICKS_TOKEN = "your-personal-access-token"
   ```

2. Or configure Databricks CLI:
   ```bash
   databricks auth login --host https://dbc-6b168dbb-2988.cloud.databricks.com
   ```

## Usage
    from connect_setup import get_spark
    spark = get_spark()
    df = spark.sql("SELECT 1")
    df.show()
"""

import os
from typing import Optional

from databricks.connect import DatabricksSession

# Default configuration - update these for your environment
DEFAULT_HOST = "https://dbc-6b168dbb-2988.cloud.databricks.com"
DEFAULT_CLUSTER_ID = "849e23eed5fb0c11"


def get_spark(
    host: Optional[str] = None,
    token: Optional[str] = None,
    cluster_id: Optional[str] = None,
) -> DatabricksSession:
    """Get a SparkSession connected to Databricks.

    Args:
        host: Databricks workspace URL (or set DATABRICKS_HOST env var).
        token: Personal access token (or set DATABRICKS_TOKEN env var).
        cluster_id: Cluster ID to connect to (or set DATABRICKS_CLUSTER_ID env var).

    Returns:
        DatabricksSession: A Spark session connected to Databricks.

    Raises:
        ValueError: If required configuration is missing.
    """
    # Get config from args, environment, or defaults
    host = host or os.getenv("DATABRICKS_HOST") or DEFAULT_HOST
    token = token or os.getenv("DATABRICKS_TOKEN")
    cluster_id = cluster_id or os.getenv("DATABRICKS_CLUSTER_ID") or DEFAULT_CLUSTER_ID

    # Build session
    builder = DatabricksSession.builder

    if host:
        builder = builder.host(host)
    if token:
        builder = builder.token(token)
    if cluster_id:
        builder = builder.clusterId(cluster_id)

    spark = builder.getOrCreate()

    print(f"Connected to Databricks")
    print(f"  Spark version: {spark.version}")

    return spark


def test_connection() -> bool:
    """Test the Databricks connection.

    Returns:
        bool: True if connection is successful.
    """
    try:
        spark = get_spark()
        result = spark.sql("SELECT 1 as test").collect()
        print(f"Connection test passed: {result}")
        return True
    except Exception as e:
        print(f"Connection test failed: {e}")
        return False


if __name__ == "__main__":
    print("Testing Databricks Connect...")
    print()
    test_connection()
