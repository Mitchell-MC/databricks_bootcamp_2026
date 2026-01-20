"""Bronze Layer Sample Load - Local Version with Databricks Connect.

This script downloads sample GitHub Archive data and loads it to the Bronze table
using Databricks Connect.

Usage:
    python 02_bronze_sample_load_local.py
"""

import gzip
import json
from datetime import date, datetime, timezone

import requests
from pyspark.sql.types import StringType, StructField, StructType, TimestampType

from connect_setup import get_spark

# =============================================================================
# Configuration
# =============================================================================

CATALOG_NAME = "main"  # Unity Catalog (no hive_metastore)
BRONZE_SCHEMA = "github_bronze"
BRONZE_TABLE = f"{CATALOG_NAME}.{BRONZE_SCHEMA}.raw_events"

# Sample data config
SAMPLE_DATE = date(2024, 1, 15)  # Adjust to a recent date
SAMPLE_HOURS = [10, 11]  # Download 2 hours of data

print("=" * 60)
print("BRONZE LAYER - SAMPLE DATA LOAD")
print("=" * 60)
print(f"Target table: {BRONZE_TABLE}")
print(f"Sample date: {SAMPLE_DATE}")
print(f"Sample hours: {SAMPLE_HOURS}")
print()

# =============================================================================
# Connect to Databricks
# =============================================================================

print("Connecting to Databricks...")
spark = get_spark()
print()

# =============================================================================
# Download Sample Data from GH Archive
# =============================================================================


def download_gharchive_hour(archive_date: date, hour: int) -> list[dict]:
    """Download and parse one hour of GH Archive data."""
    url = f"https://data.gharchive.org/{archive_date.isoformat()}-{hour}.json.gz"
    print(f"  Downloading: {url}")

    try:
        response = requests.get(url, timeout=120)
        response.raise_for_status()

        decompressed = gzip.decompress(response.content).decode("utf-8")
        events = []

        for line in decompressed.strip().split("\n"):
            if line:
                try:
                    events.append(json.loads(line))
                except json.JSONDecodeError:
                    continue

        print(f"    Parsed {len(events):,} events")
        return events

    except Exception as e:
        print(f"    Error: {e}")
        return []


print("Downloading sample data from GH Archive...")
all_events = []
for hour in SAMPLE_HOURS:
    events = download_gharchive_hour(SAMPLE_DATE, hour)
    all_events.extend(events)

print(f"\nTotal events downloaded: {len(all_events):,}")
print()

# =============================================================================
# Transform to Bronze Format
# =============================================================================

print("Transforming to Bronze format...")

ingested_at = datetime.now(timezone.utc)
bronze_records = []

for event in all_events:
    bronze_records.append(
        {
            "event_id": str(event.get("id", "")),
            "event_type": event.get("type", "Unknown"),
            "raw_json": json.dumps(event),
            "source_file": f"{SAMPLE_DATE.isoformat()}-sample.json.gz",
            "ingested_at": ingested_at,
            "_rescued_data": None,
        }
    )

print(f"  Bronze records prepared: {len(bronze_records):,}")
print()

# =============================================================================
# Load into Bronze Table
# =============================================================================

print("Loading into Bronze table...")

# Define schema
bronze_schema = StructType(
    [
        StructField("event_id", StringType(), False),
        StructField("event_type", StringType(), False),
        StructField("raw_json", StringType(), False),
        StructField("source_file", StringType(), False),
        StructField("ingested_at", TimestampType(), False),
        StructField("_rescued_data", StringType(), True),
    ]
)

# Create DataFrame
bronze_df = spark.createDataFrame(bronze_records, schema=bronze_schema)
print(f"  DataFrame created with {bronze_df.count():,} rows")

# Write to Bronze table (append mode)
bronze_df.write.format("delta").mode("append").partitionBy("event_type").saveAsTable(
    BRONZE_TABLE
)

print(f"  Data written to {BRONZE_TABLE}")
print()

# =============================================================================
# Validate the Load
# =============================================================================

print("Validating the load...")

# Count rows by event type
print("\nRows by event type:")
spark.sql(
    f"""
    SELECT
        event_type,
        COUNT(*) as event_count,
        COUNT(DISTINCT event_id) as unique_events
    FROM {BRONZE_TABLE}
    GROUP BY event_type
    ORDER BY event_count DESC
"""
).show(truncate=False)

# Table statistics
print("\nTable statistics:")
spark.sql(
    f"""
    SELECT
        COUNT(*) as total_events,
        COUNT(DISTINCT event_type) as event_types,
        COUNT(DISTINCT source_file) as source_files,
        MIN(ingested_at) as first_ingested,
        MAX(ingested_at) as last_ingested
    FROM {BRONZE_TABLE}
"""
).show(truncate=False)

# =============================================================================
# Summary
# =============================================================================

print("=" * 60)
print("SAMPLE LOAD COMPLETE")
print("=" * 60)
print(
    """
Sample data loaded successfully. Next steps:
1. Run 03_bronze_eda_local.py for detailed exploratory analysis
2. Implement Silver layer transformations
"""
)
