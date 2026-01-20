"""Bronze Layer Setup - Local Version with Databricks Connect.

This script creates the Bronze layer Delta tables using Databricks Connect.
Run this locally to execute on your remote Databricks cluster.

Usage:
    python 01_bronze_setup_local.py
"""

from connect_setup import get_spark

# =============================================================================
# Configuration - Unity Catalog (no hive_metastore)
# =============================================================================

CATALOG_NAME = "main"  # Unity Catalog default
BRONZE_SCHEMA = "github_bronze"
SILVER_SCHEMA = "github_silver"
GOLD_SCHEMA = "github_gold"

print("=" * 60)
print("BRONZE LAYER SETUP - Unity Catalog")
print("=" * 60)
print(f"Catalog: {CATALOG_NAME}")
print(f"Schemas: {BRONZE_SCHEMA}, {SILVER_SCHEMA}, {GOLD_SCHEMA}")
print()

# =============================================================================
# Connect to Databricks
# =============================================================================

print("Connecting to Databricks...")
spark = get_spark()
print()

# =============================================================================
# Create Catalog and Schemas
# =============================================================================

print("Setting up Unity Catalog schemas...")

# Use Unity Catalog exclusively (no hive_metastore fallback)
spark.sql(f"USE CATALOG {CATALOG_NAME}")
print(f"  Using Unity Catalog: {CATALOG_NAME}")

# Create schemas for each layer
for schema in [BRONZE_SCHEMA, SILVER_SCHEMA, GOLD_SCHEMA]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    print(f"  Created schema: {CATALOG_NAME}.{schema}")

print()

# =============================================================================
# Create Bronze Raw Events Table
# =============================================================================

print("Creating Bronze raw_events table...")

bronze_ddl = f"""
CREATE TABLE IF NOT EXISTS {CATALOG_NAME}.{BRONZE_SCHEMA}.raw_events (
    event_id STRING NOT NULL COMMENT 'Unique event identifier from GH Archive',
    event_type STRING NOT NULL COMMENT 'Type of GitHub event (PushEvent, WatchEvent, etc.)',
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

spark.sql(bronze_ddl)
print("  Created Bronze raw_events table")
print()

# =============================================================================
# Validate Table Structure
# =============================================================================

print("Validating table structure...")

# Describe the table
desc_df = spark.sql(
    f"DESCRIBE EXTENDED {CATALOG_NAME}.{BRONZE_SCHEMA}.raw_events"
)
print("\nTable Schema:")
desc_df.show(truncate=False)

# Show table properties
props_df = spark.sql(
    f"SHOW TBLPROPERTIES {CATALOG_NAME}.{BRONZE_SCHEMA}.raw_events"
)
print("\nTable Properties:")
props_df.show(truncate=False)

# =============================================================================
# Summary
# =============================================================================

print("=" * 60)
print("SETUP COMPLETE")
print("=" * 60)
print(f"""
Bronze layer setup complete. The following tables are ready:

| Table | Description |
|-------|-------------|
| {CATALOG_NAME}.{BRONZE_SCHEMA}.raw_events | Raw GitHub events (partitioned by event_type) |

Next steps:
1. Run 02_bronze_sample_load_local.py to load sample data
2. Run 03_bronze_eda_local.py for exploratory analysis
""")
