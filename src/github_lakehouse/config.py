"""Configuration models for GitHub Archive Lakehouse.

This module defines Pydantic configuration models for:
- GH Archive data source settings
- Delta Lake table configurations
- Streaming pipeline settings
- Databricks workspace settings
"""

from datetime import date
from enum import Enum

from pydantic import BaseModel, Field, computed_field


class EventType(str, Enum):
    """GitHub event types supported by GH Archive."""

    PUSH = "PushEvent"
    PULL_REQUEST = "PullRequestEvent"
    PULL_REQUEST_REVIEW = "PullRequestReviewEvent"
    PULL_REQUEST_REVIEW_COMMENT = "PullRequestReviewCommentEvent"
    ISSUES = "IssuesEvent"
    ISSUE_COMMENT = "IssueCommentEvent"
    WATCH = "WatchEvent"
    FORK = "ForkEvent"
    CREATE = "CreateEvent"
    DELETE = "DeleteEvent"
    RELEASE = "ReleaseEvent"
    MEMBER = "MemberEvent"
    PUBLIC = "PublicEvent"
    GOLLUM = "GollumEvent"
    COMMIT_COMMENT = "CommitCommentEvent"


class GHArchiveConfig(BaseModel):
    """Configuration for GitHub Archive data source.

    Attributes:
        base_url: Base URL for GH Archive files.
        start_date: Start date for data ingestion.
        end_date: End date for data ingestion (None = today).
        hours_per_batch: Number of hours to process per batch.
        download_path: Local path for downloaded files.
        timeout_seconds: HTTP request timeout.
        max_retries: Maximum retry attempts for failed downloads.
    """

    base_url: str = Field(
        default="https://data.gharchive.org",
        description="Base URL for GH Archive files",
    )
    start_date: date = Field(
        description="Start date for data ingestion (inclusive)",
    )
    end_date: date | None = Field(
        default=None,
        description="End date for data ingestion (inclusive, None = today)",
    )
    hours_per_batch: int = Field(
        default=24,
        ge=1,
        le=168,
        description="Number of hours to process per batch (1-168)",
    )
    download_path: str = Field(
        default="/tmp/gharchive",
        description="Local path for downloaded archive files",
    )
    timeout_seconds: int = Field(
        default=60,
        ge=10,
        le=300,
        description="HTTP request timeout in seconds",
    )
    max_retries: int = Field(
        default=3,
        ge=0,
        le=10,
        description="Maximum retry attempts for failed downloads",
    )

    def get_archive_url(self, archive_date: date, hour: int) -> str:
        """Generate URL for a specific archive file.

        Args:
            archive_date: Date of the archive.
            hour: Hour of the archive (0-23).

        Returns:
            str: Full URL to the archive file.

        Raises:
            ValueError: If hour is not in range 0-23.
        """
        if not 0 <= hour <= 23:
            raise ValueError(f"Hour must be 0-23, got {hour}")
        filename = f"{archive_date.isoformat()}-{hour}.json.gz"
        return f"{self.base_url}/{filename}"


class DeltaTableConfig(BaseModel):
    """Configuration for a Delta Lake table.

    Attributes:
        catalog: Unity Catalog name.
        schema_name: Schema (database) name.
        table_name: Table name.
        partition_columns: Columns to partition by.
        z_order_columns: Columns for Z-ordering optimization.
    """

    catalog: str = Field(
        default="github_lakehouse",
        description="Unity Catalog name",
    )
    schema_name: str = Field(
        description="Schema (database) name",
    )
    table_name: str = Field(
        description="Table name",
    )
    partition_columns: list[str] = Field(
        default_factory=list,
        description="Columns to partition by",
    )
    z_order_columns: list[str] = Field(
        default_factory=list,
        description="Columns for Z-ordering optimization",
    )

    @computed_field
    @property
    def full_table_name(self) -> str:
        """Get fully qualified table name.

        Returns:
            str: Catalog.schema.table format.
        """
        return f"{self.catalog}.{self.schema_name}.{self.table_name}"


class BronzeConfig(BaseModel):
    """Configuration for Bronze layer tables.

    Attributes:
        raw_events_table: Configuration for raw events table.
        checkpoint_location: Checkpoint location for streaming.
        rescue_column_name: Column name for Auto Loader rescued data.
    """

    raw_events_table: DeltaTableConfig = Field(
        default_factory=lambda: DeltaTableConfig(
            schema_name="bronze",
            table_name="raw_events",
            partition_columns=["event_type"],
        ),
    )
    checkpoint_location: str = Field(
        default="/checkpoints/bronze/raw_events",
        description="Checkpoint location for streaming ingestion",
    )
    rescue_column_name: str = Field(
        default="_rescued_data",
        description="Column name for Auto Loader rescued data",
    )


class SilverConfig(BaseModel):
    """Configuration for Silver layer tables.

    Attributes:
        events_table: Configuration for cleansed events table.
        checkpoint_location: Checkpoint location for streaming.
        dedup_window_hours: Window size for deduplication (hours).
    """

    events_table: DeltaTableConfig = Field(
        default_factory=lambda: DeltaTableConfig(
            schema_name="silver",
            table_name="events",
            partition_columns=["event_type", "event_date"],
            z_order_columns=["repo_id", "actor_id"],
        ),
    )
    checkpoint_location: str = Field(
        default="/checkpoints/silver/events",
        description="Checkpoint location for streaming transformation",
    )
    dedup_window_hours: int = Field(
        default=48,
        ge=1,
        le=168,
        description="Window size for deduplication in hours",
    )


class GoldConfig(BaseModel):
    """Configuration for Gold layer tables.

    Attributes:
        daily_stats_table: Daily statistics table config.
        repo_metrics_table: Repository metrics table config.
        trending_repos_table: Trending repositories table config.
        user_activity_table: User activity table config.
    """

    daily_stats_table: DeltaTableConfig = Field(
        default_factory=lambda: DeltaTableConfig(
            schema_name="gold",
            table_name="daily_stats",
            partition_columns=["date"],
        ),
    )
    repo_metrics_table: DeltaTableConfig = Field(
        default_factory=lambda: DeltaTableConfig(
            schema_name="gold",
            table_name="repo_metrics",
            partition_columns=["date"],
            z_order_columns=["repo_id"],
        ),
    )
    trending_repos_table: DeltaTableConfig = Field(
        default_factory=lambda: DeltaTableConfig(
            schema_name="gold",
            table_name="trending_repos",
            partition_columns=["date"],
        ),
    )
    user_activity_table: DeltaTableConfig = Field(
        default_factory=lambda: DeltaTableConfig(
            schema_name="gold",
            table_name="user_activity",
            partition_columns=["date"],
            z_order_columns=["actor_id"],
        ),
    )


class StreamingConfig(BaseModel):
    """Configuration for Structured Streaming pipelines.

    Attributes:
        trigger_interval: Trigger interval for streaming (e.g., "10 seconds").
        watermark_delay: Watermark delay for late data (e.g., "1 hour").
        max_files_per_trigger: Maximum files per trigger for Auto Loader.
        output_mode: Streaming output mode (append, complete, update).
    """

    trigger_interval: str = Field(
        default="10 seconds",
        description="Trigger interval for streaming queries",
    )
    watermark_delay: str = Field(
        default="1 hour",
        description="Watermark delay for handling late data",
    )
    max_files_per_trigger: int = Field(
        default=100,
        ge=1,
        le=10000,
        description="Maximum files per trigger for Auto Loader",
    )
    output_mode: str = Field(
        default="append",
        pattern="^(append|complete|update)$",
        description="Streaming output mode",
    )


class LakehouseConfig(BaseModel):
    """Main configuration for GitHub Archive Lakehouse.

    This is the top-level configuration class that aggregates all
    layer-specific configurations.

    Attributes:
        gh_archive: GitHub Archive data source configuration.
        bronze: Bronze layer configuration.
        silver: Silver layer configuration.
        gold: Gold layer configuration.
        streaming: Streaming pipeline configuration.
        storage_root: Root path for Delta tables.
    """

    gh_archive: GHArchiveConfig
    bronze: BronzeConfig = Field(default_factory=BronzeConfig)
    silver: SilverConfig = Field(default_factory=SilverConfig)
    gold: GoldConfig = Field(default_factory=GoldConfig)
    streaming: StreamingConfig = Field(default_factory=StreamingConfig)
    storage_root: str = Field(
        default="/mnt/github_lakehouse",
        description="Root path for Delta table storage",
    )

    def get_checkpoint_path(self, layer: str, table: str) -> str:
        """Get full checkpoint path for a streaming table.

        Args:
            layer: Layer name (bronze, silver, gold).
            table: Table name.

        Returns:
            str: Full checkpoint path.
        """
        return f"{self.storage_root}/checkpoints/{layer}/{table}"

    def get_table_path(self, layer: str, table: str) -> str:
        """Get full storage path for a Delta table.

        Args:
            layer: Layer name (bronze, silver, gold).
            table: Table name.

        Returns:
            str: Full table storage path.
        """
        return f"{self.storage_root}/{layer}/{table}"
