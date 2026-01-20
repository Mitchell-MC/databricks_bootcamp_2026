"""Unit tests for Bronze layer schemas and ingestion."""

import json
from datetime import date, datetime, timezone
from typing import Any

import pytest

from github_lakehouse.bronze.ingestion_job import BronzeIngestionJob, IngestionResult
from github_lakehouse.bronze.schemas import (
    BRONZE_TABLE_SCHEMA,
    RawEventRecord,
    get_bronze_table_ddl,
)
from github_lakehouse.config import GHArchiveConfig, LakehouseConfig


class TestRawEventRecord:
    """Tests for RawEventRecord model."""

    def test_from_json_event_push_event(
        self, sample_push_event: dict[str, Any]
    ) -> None:
        """Test creating record from PushEvent JSON."""
        source_file = "2024-01-15-10.json.gz"
        ingested_at = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        raw_json = json.dumps(sample_push_event)

        record = RawEventRecord.from_json_event(
            event=sample_push_event,
            source_file=source_file,
            ingested_at=ingested_at,
            raw_json=raw_json,
        )

        assert record.event_id == "12345678901"
        assert record.event_type == "PushEvent"
        assert record.source_file == source_file
        assert record.ingested_at == ingested_at
        assert record.raw_json == raw_json
        assert record.rescued_data is None

    def test_from_json_event_watch_event(
        self, sample_watch_event: dict[str, Any]
    ) -> None:
        """Test creating record from WatchEvent JSON."""
        source_file = "2024-01-15-11.json.gz"
        ingested_at = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        raw_json = json.dumps(sample_watch_event)

        record = RawEventRecord.from_json_event(
            event=sample_watch_event,
            source_file=source_file,
            ingested_at=ingested_at,
            raw_json=raw_json,
        )

        assert record.event_id == "12345678902"
        assert record.event_type == "WatchEvent"

    def test_from_json_event_missing_id_raises(self) -> None:
        """Test that missing id field raises KeyError."""
        invalid_event = {"type": "PushEvent"}

        with pytest.raises(KeyError):
            RawEventRecord.from_json_event(
                event=invalid_event,
                source_file="test.json.gz",
                ingested_at=datetime.now(timezone.utc),
                raw_json="{}",
            )

    def test_from_json_event_missing_type_raises(self) -> None:
        """Test that missing type field raises KeyError."""
        invalid_event = {"id": "123"}

        with pytest.raises(KeyError):
            RawEventRecord.from_json_event(
                event=invalid_event,
                source_file="test.json.gz",
                ingested_at=datetime.now(timezone.utc),
                raw_json="{}",
            )


class TestBronzeTableSchema:
    """Tests for Bronze table schema definitions."""

    def test_schema_contains_required_columns(self) -> None:
        """Test that schema contains all required columns."""
        required_columns = [
            "event_id",
            "event_type",
            "raw_json",
            "source_file",
            "ingested_at",
        ]
        for col in required_columns:
            assert col in BRONZE_TABLE_SCHEMA

    def test_get_bronze_table_ddl_formats_name(self) -> None:
        """Test that DDL correctly formats table name."""
        ddl = get_bronze_table_ddl("github_lakehouse.bronze.raw_events")

        assert "github_lakehouse.bronze.raw_events" in ddl
        assert "CREATE TABLE IF NOT EXISTS" in ddl
        assert "USING DELTA" in ddl
        assert "PARTITIONED BY (event_type)" in ddl


class TestBronzeIngestionJob:
    """Tests for BronzeIngestionJob."""

    @pytest.fixture
    def config(self) -> LakehouseConfig:
        """Create test configuration."""
        return LakehouseConfig(
            gh_archive=GHArchiveConfig(
                start_date=date(2024, 1, 1),
                end_date=date(2024, 1, 1),
            ),
        )

    @pytest.fixture
    def job(self, config: LakehouseConfig) -> BronzeIngestionJob:
        """Create test ingestion job."""
        return BronzeIngestionJob(config)

    def test_parse_jsonl_content_single_event(
        self,
        job: BronzeIngestionJob,
        sample_push_event: dict[str, Any],
    ) -> None:
        """Test parsing JSONL with single event."""
        content = json.dumps(sample_push_event)
        source_file = "2024-01-15-10.json.gz"

        records = list(job.parse_jsonl_content(content, source_file))

        assert len(records) == 1
        assert records[0].event_id == "12345678901"
        assert records[0].event_type == "PushEvent"

    def test_parse_jsonl_content_multiple_events(
        self,
        job: BronzeIngestionJob,
        sample_events_jsonl: str,
    ) -> None:
        """Test parsing JSONL with multiple events."""
        source_file = "2024-01-15-10.json.gz"

        records = list(job.parse_jsonl_content(sample_events_jsonl, source_file))

        assert len(records) == 3
        event_types = [r.event_type for r in records]
        assert "PushEvent" in event_types
        assert "WatchEvent" in event_types
        assert "PullRequestEvent" in event_types

    def test_parse_jsonl_content_empty_lines(
        self,
        job: BronzeIngestionJob,
        sample_push_event: dict[str, Any],
    ) -> None:
        """Test that empty lines are skipped."""
        content = f"\n{json.dumps(sample_push_event)}\n\n"
        source_file = "test.json.gz"

        records = list(job.parse_jsonl_content(content, source_file))

        assert len(records) == 1

    def test_parse_jsonl_content_invalid_json_raises(
        self,
        job: BronzeIngestionJob,
    ) -> None:
        """Test that invalid JSON raises ValueError."""
        content = "not valid json"
        source_file = "test.json.gz"

        with pytest.raises(ValueError, match="Invalid JSON"):
            list(job.parse_jsonl_content(content, source_file))

    def test_parse_jsonl_content_safe_collects_errors(
        self,
        job: BronzeIngestionJob,
        sample_push_event: dict[str, Any],
    ) -> None:
        """Test safe parsing collects errors instead of raising."""
        valid_line = json.dumps(sample_push_event)
        invalid_line = "not valid json"
        content = f"{valid_line}\n{invalid_line}"
        source_file = "test.json.gz"

        records, errors = job.parse_jsonl_content_safe(content, source_file)

        assert len(records) == 1
        assert len(errors) == 1
        assert "Line 2" in errors[0]

    def test_records_to_dicts(
        self,
        job: BronzeIngestionJob,
        sample_push_event: dict[str, Any],
    ) -> None:
        """Test converting records to dictionaries."""
        ingested_at = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        record = RawEventRecord.from_json_event(
            event=sample_push_event,
            source_file="test.json.gz",
            ingested_at=ingested_at,
            raw_json=json.dumps(sample_push_event),
        )

        dicts = job.records_to_dicts([record])

        assert len(dicts) == 1
        assert dicts[0]["event_id"] == "12345678901"
        assert dicts[0]["event_type"] == "PushEvent"
        assert dicts[0]["ingested_at"] == ingested_at

    def test_process_file_returns_result(
        self,
        job: BronzeIngestionJob,
        sample_events_jsonl: str,
    ) -> None:
        """Test process_file returns IngestionResult."""
        source_file = "2024-01-15-10.json.gz"

        result = job.process_file(sample_events_jsonl, source_file)

        assert isinstance(result, IngestionResult)
        assert result.records_processed == 3
        assert result.records_failed == 0
        assert result.source_file == source_file
        assert result.success is True

    def test_process_file_with_errors(
        self,
        job: BronzeIngestionJob,
        sample_push_event: dict[str, Any],
    ) -> None:
        """Test process_file handles errors gracefully."""
        content = f"{json.dumps(sample_push_event)}\ninvalid json"
        source_file = "test.json.gz"

        result = job.process_file(content, source_file)

        assert result.records_processed == 1
        assert result.records_failed == 1
        assert result.success is False
        assert len(result.errors) == 1


class TestIngestionResult:
    """Tests for IngestionResult dataclass."""

    def test_success_true_when_no_failures(self) -> None:
        """Test success is True when no failures."""
        result = IngestionResult(
            records_processed=100,
            records_failed=0,
            source_file="test.json.gz",
            started_at=datetime(2024, 1, 15, 12, 0, 0),
            completed_at=datetime(2024, 1, 15, 12, 0, 10),
            errors=[],
        )

        assert result.success is True

    def test_success_false_when_failures(self) -> None:
        """Test success is False when there are failures."""
        result = IngestionResult(
            records_processed=99,
            records_failed=1,
            source_file="test.json.gz",
            started_at=datetime(2024, 1, 15, 12, 0, 0),
            completed_at=datetime(2024, 1, 15, 12, 0, 10),
            errors=["Error on line 50"],
        )

        assert result.success is False

    def test_duration_seconds(self) -> None:
        """Test duration calculation."""
        result = IngestionResult(
            records_processed=100,
            records_failed=0,
            source_file="test.json.gz",
            started_at=datetime(2024, 1, 15, 12, 0, 0),
            completed_at=datetime(2024, 1, 15, 12, 0, 30),
            errors=[],
        )

        assert result.duration_seconds == 30.0
