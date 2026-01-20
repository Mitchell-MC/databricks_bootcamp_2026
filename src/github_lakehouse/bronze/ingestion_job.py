"""Bronze layer ingestion job for GitHub Archive data.

This module provides the BronzeIngestionJob class for ingesting raw
GitHub events from GH Archive files into the Bronze Delta table.
"""

import json
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from github_lakehouse.bronze.schemas import RawEventRecord
from github_lakehouse.config import BronzeConfig, LakehouseConfig


@dataclass
class IngestionResult:
    """Result of a Bronze ingestion operation.

    Attributes:
        records_processed: Number of records successfully processed.
        records_failed: Number of records that failed processing.
        source_file: Name of the source file processed.
        started_at: Timestamp when ingestion started.
        completed_at: Timestamp when ingestion completed.
        errors: List of error messages (if any).
    """

    records_processed: int
    records_failed: int
    source_file: str
    started_at: datetime
    completed_at: datetime
    errors: list[str]

    @property
    def success(self) -> bool:
        """Check if ingestion was fully successful.

        Returns:
            bool: True if no records failed.
        """
        return self.records_failed == 0

    @property
    def duration_seconds(self) -> float:
        """Calculate ingestion duration in seconds.

        Returns:
            float: Duration in seconds.
        """
        return (self.completed_at - self.started_at).total_seconds()


class BronzeIngestionJob:
    """Job for ingesting raw GitHub events into Bronze layer.

    This class handles parsing GH Archive JSON files and converting
    them into RawEventRecord instances for Bronze table ingestion.

    Attributes:
        config: Lakehouse configuration.
        bronze_config: Bronze layer specific configuration.
    """

    def __init__(self, config: LakehouseConfig) -> None:
        """Initialize the Bronze ingestion job.

        Args:
            config: Lakehouse configuration instance.
        """
        self.config = config
        self.bronze_config: BronzeConfig = config.bronze

    def parse_jsonl_content(
        self,
        content: str,
        source_file: str,
    ) -> Iterator[RawEventRecord]:
        """Parse newline-delimited JSON content into RawEventRecords.

        Args:
            content: Newline-delimited JSON string.
            source_file: Name of the source file.

        Yields:
            RawEventRecord: Parsed event records.

        Raises:
            ValueError: If a line cannot be parsed as JSON.
        """
        ingested_at = datetime.now(timezone.utc)

        for line_num, line in enumerate(content.strip().split("\n"), start=1):
            line = line.strip()
            if not line:
                continue

            try:
                event = json.loads(line)
                yield RawEventRecord.from_json_event(
                    event=event,
                    source_file=source_file,
                    ingested_at=ingested_at,
                    raw_json=line,
                )
            except json.JSONDecodeError as e:
                raise ValueError(
                    f"Invalid JSON at line {line_num} in {source_file}: {e}"
                ) from e
            except KeyError as e:
                raise ValueError(
                    f"Missing required field at line {line_num} in {source_file}: {e}"
                ) from e

    def parse_jsonl_content_safe(
        self,
        content: str,
        source_file: str,
    ) -> tuple[list[RawEventRecord], list[str]]:
        """Parse JSONL content, collecting errors instead of raising.

        Args:
            content: Newline-delimited JSON string.
            source_file: Name of the source file.

        Returns:
            tuple: (list of successful records, list of error messages).
        """
        records: list[RawEventRecord] = []
        errors: list[str] = []
        ingested_at = datetime.now(timezone.utc)

        for line_num, line in enumerate(content.strip().split("\n"), start=1):
            line = line.strip()
            if not line:
                continue

            try:
                event = json.loads(line)
                record = RawEventRecord.from_json_event(
                    event=event,
                    source_file=source_file,
                    ingested_at=ingested_at,
                    raw_json=line,
                )
                records.append(record)
            except (json.JSONDecodeError, KeyError, TypeError) as e:
                errors.append(f"Line {line_num}: {type(e).__name__}: {e}")

        return records, errors

    def records_to_dicts(
        self,
        records: list[RawEventRecord],
    ) -> list[dict[str, Any]]:
        """Convert RawEventRecords to dictionaries for DataFrame creation.

        Args:
            records: List of RawEventRecord instances.

        Returns:
            list: List of dictionaries suitable for Spark DataFrame.
        """
        return [
            {
                "event_id": r.event_id,
                "event_type": r.event_type,
                "raw_json": r.raw_json,
                "source_file": r.source_file,
                "ingested_at": r.ingested_at,
                "_rescued_data": r.rescued_data,
            }
            for r in records
        ]

    def process_file(
        self,
        content: str,
        source_file: str,
    ) -> IngestionResult:
        """Process a single GH Archive file for ingestion.

        Args:
            content: Decompressed file content (JSONL format).
            source_file: Name of the source file.

        Returns:
            IngestionResult: Result of the ingestion operation.
        """
        started_at = datetime.now(timezone.utc)
        records, errors = self.parse_jsonl_content_safe(content, source_file)
        completed_at = datetime.now(timezone.utc)

        return IngestionResult(
            records_processed=len(records),
            records_failed=len(errors),
            source_file=source_file,
            started_at=started_at,
            completed_at=completed_at,
            errors=errors,
        )

    def get_spark_write_options(self) -> dict[str, str]:
        """Get Spark DataFrameWriter options for Bronze table.

        Returns:
            dict: Options for df.write.options(**options).
        """
        return {
            "mergeSchema": "true",
            "checkpointLocation": self.config.get_checkpoint_path(
                "bronze", "raw_events"
            ),
        }

    def get_auto_loader_options(self, source_path: str) -> dict[str, str]:
        """Get Auto Loader options for streaming ingestion.

        Args:
            source_path: Path to source files (cloud storage).

        Returns:
            dict: Options for spark.readStream.format("cloudFiles").
        """
        return {
            "cloudFiles.format": "json",
            "cloudFiles.schemaLocation": f"{self.config.storage_root}/schemas/bronze",
            "cloudFiles.inferColumnTypes": "true",
            "cloudFiles.schemaEvolutionMode": "rescue",
            "rescuedDataColumn": self.bronze_config.rescue_column_name,
            "pathGlobFilter": "*.json.gz",
        }
