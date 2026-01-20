"""Schema definitions for Bronze layer tables.

This module defines:
- Spark StructType schema for raw events table
- Pydantic model for raw event records
- Schema validation utilities
"""

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field


class RawEventRecord(BaseModel):
    """Pydantic model for a raw GitHub event record in Bronze layer.

    This represents the minimal schema used for Bronze ingestion,
    storing the full event JSON as a string for later processing.

    Attributes:
        event_id: Unique event identifier from GH Archive.
        event_type: Type of GitHub event (PushEvent, WatchEvent, etc.).
        raw_json: Complete event JSON as a string.
        source_file: Name of the source archive file.
        ingested_at: Timestamp when record was ingested.
        rescued_data: Any data that couldn't be parsed (from Auto Loader).
    """

    event_id: str = Field(
        description="Unique event identifier from GH Archive",
    )
    event_type: str = Field(
        description="Type of GitHub event",
    )
    raw_json: str = Field(
        description="Complete event JSON as a string",
    )
    source_file: str = Field(
        description="Name of the source archive file",
    )
    ingested_at: datetime = Field(
        description="Timestamp when record was ingested",
    )
    rescued_data: str | None = Field(
        default=None,
        description="Data that couldn't be parsed (Auto Loader rescue)",
    )

    @classmethod
    def from_json_event(
        cls,
        event: dict[str, Any],
        source_file: str,
        ingested_at: datetime,
        raw_json: str,
    ) -> "RawEventRecord":
        """Create a RawEventRecord from a parsed JSON event.

        Args:
            event: Parsed JSON event dictionary.
            source_file: Name of the source archive file.
            ingested_at: Timestamp of ingestion.
            raw_json: Original JSON string.

        Returns:
            RawEventRecord: A new record instance.

        Raises:
            KeyError: If required fields are missing.
        """
        return cls(
            event_id=str(event["id"]),
            event_type=event["type"],
            raw_json=raw_json,
            source_file=source_file,
            ingested_at=ingested_at,
            rescued_data=None,
        )


# Spark SQL DDL for Bronze raw_events table
BRONZE_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS {full_table_name} (
    event_id STRING NOT NULL COMMENT 'Unique event identifier from GH Archive',
    event_type STRING NOT NULL COMMENT 'Type of GitHub event',
    raw_json STRING NOT NULL COMMENT 'Complete event JSON as string',
    source_file STRING NOT NULL COMMENT 'Source archive filename',
    ingested_at TIMESTAMP NOT NULL COMMENT 'Ingestion timestamp',
    _rescued_data STRING COMMENT 'Auto Loader rescued data'
)
USING DELTA
PARTITIONED BY (event_type)
COMMENT 'Raw GitHub events from GH Archive (Bronze layer)'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
)
"""

# Spark StructType schema as a string (for use in Databricks)
# Reason: String schema is more portable for notebooks and Auto Loader configs
BRONZE_TABLE_SCHEMA = """
    event_id STRING NOT NULL,
    event_type STRING NOT NULL,
    raw_json STRING NOT NULL,
    source_file STRING NOT NULL,
    ingested_at TIMESTAMP NOT NULL,
    _rescued_data STRING
"""


def get_bronze_table_ddl(full_table_name: str) -> str:
    """Generate DDL for creating the Bronze raw_events table.

    Args:
        full_table_name: Fully qualified table name (catalog.schema.table).

    Returns:
        str: SQL DDL statement.
    """
    return BRONZE_TABLE_DDL.format(full_table_name=full_table_name)
