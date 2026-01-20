"""Bronze layer for GitHub Archive Lakehouse.

The Bronze layer handles raw data ingestion from GH Archive,
storing events in their original JSON format with minimal transformation.
"""

from github_lakehouse.bronze.ingestion_job import BronzeIngestionJob
from github_lakehouse.bronze.schemas import (
    BRONZE_TABLE_SCHEMA,
    RawEventRecord,
)

__all__ = [
    "BronzeIngestionJob",
    "BRONZE_TABLE_SCHEMA",
    "RawEventRecord",
]
