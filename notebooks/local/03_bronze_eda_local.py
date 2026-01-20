"""Bronze Layer EDA - Local Version with Databricks Connect.

This script performs exploratory analysis of the GitHub Archive data
in the Bronze layer using Databricks Connect.

Usage:
    python 03_bronze_eda_local.py
"""

from connect_setup import get_spark

# =============================================================================
# Configuration
# =============================================================================

CATALOG_NAME = "main"  # Unity Catalog (no hive_metastore)
BRONZE_SCHEMA = "github_bronze"
BRONZE_TABLE = f"{CATALOG_NAME}.{BRONZE_SCHEMA}.raw_events"

print("=" * 60)
print("BRONZE LAYER - EXPLORATORY DATA ANALYSIS")
print("=" * 60)
print(f"Analyzing table: {BRONZE_TABLE}")
print()

# =============================================================================
# Connect to Databricks
# =============================================================================

print("Connecting to Databricks...")
spark = get_spark()
print()

# =============================================================================
# 1. Data Overview
# =============================================================================

print("=" * 60)
print("1. DATA OVERVIEW")
print("=" * 60)

# Basic statistics
print("\n1.1 Basic Statistics")
print("-" * 40)

stats = spark.sql(
    f"""
    SELECT
        COUNT(*) as total_rows,
        COUNT(DISTINCT event_id) as distinct_events,
        COUNT(DISTINCT event_type) as distinct_types,
        COUNT(DISTINCT source_file) as source_files
    FROM {BRONZE_TABLE}
"""
).collect()[0]

print(f"  Total rows: {stats['total_rows']:,}")
print(f"  Distinct event IDs: {stats['distinct_events']:,}")
print(f"  Distinct event types: {stats['distinct_types']}")
print(f"  Source files: {stats['source_files']}")

if stats["total_rows"] > 0:
    dup_rate = (stats["total_rows"] - stats["distinct_events"]) / stats["total_rows"] * 100
    print(f"  Duplicate rate: {dup_rate:.2f}%")

# =============================================================================
# 2. Event Type Analysis
# =============================================================================

print("\n" + "=" * 60)
print("2. EVENT TYPE ANALYSIS")
print("=" * 60)

print("\n2.1 Event Type Distribution")
print("-" * 40)

spark.sql(
    f"""
    SELECT
        event_type,
        COUNT(*) as count,
        ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
    FROM {BRONZE_TABLE}
    GROUP BY event_type
    ORDER BY count DESC
"""
).show(truncate=False)

print("\n2.2 Event Categories")
print("-" * 40)

spark.sql(
    f"""
    SELECT
        CASE
            WHEN event_type IN ('PushEvent', 'CreateEvent', 'DeleteEvent', 'CommitCommentEvent') THEN 'Code Activity'
            WHEN event_type IN ('PullRequestEvent', 'PullRequestReviewEvent', 'PullRequestReviewCommentEvent') THEN 'Pull Requests'
            WHEN event_type IN ('IssuesEvent', 'IssueCommentEvent') THEN 'Issues'
            WHEN event_type IN ('WatchEvent', 'ForkEvent') THEN 'Social'
            WHEN event_type IN ('ReleaseEvent') THEN 'Releases'
            ELSE 'Other'
        END as category,
        COUNT(*) as count,
        ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
    FROM {BRONZE_TABLE}
    GROUP BY 1
    ORDER BY count DESC
"""
).show(truncate=False)

# =============================================================================
# 3. Actor Analysis
# =============================================================================

print("\n" + "=" * 60)
print("3. ACTOR ANALYSIS")
print("=" * 60)

print("\n3.1 Top 10 Most Active Users")
print("-" * 40)

spark.sql(
    f"""
    SELECT
        GET_JSON_OBJECT(raw_json, '$.actor.login') as actor_login,
        COUNT(*) as event_count,
        COUNT(DISTINCT event_type) as event_types,
        COUNT(DISTINCT GET_JSON_OBJECT(raw_json, '$.repo.name')) as repos
    FROM {BRONZE_TABLE}
    GROUP BY 1
    ORDER BY event_count DESC
    LIMIT 10
"""
).show(truncate=False)

# =============================================================================
# 4. Repository Analysis
# =============================================================================

print("\n" + "=" * 60)
print("4. REPOSITORY ANALYSIS")
print("=" * 60)

print("\n4.1 Top 10 Most Active Repositories")
print("-" * 40)

spark.sql(
    f"""
    SELECT
        GET_JSON_OBJECT(raw_json, '$.repo.name') as repo_name,
        COUNT(*) as event_count,
        COUNT(DISTINCT event_type) as event_types,
        COUNT(DISTINCT GET_JSON_OBJECT(raw_json, '$.actor.login')) as unique_actors
    FROM {BRONZE_TABLE}
    GROUP BY 1
    ORDER BY event_count DESC
    LIMIT 10
"""
).show(truncate=False)

print("\n4.2 Most Starred Repos (WatchEvents)")
print("-" * 40)

spark.sql(
    f"""
    SELECT
        GET_JSON_OBJECT(raw_json, '$.repo.name') as repo,
        COUNT(*) as star_count
    FROM {BRONZE_TABLE}
    WHERE event_type = 'WatchEvent'
    GROUP BY 1
    ORDER BY star_count DESC
    LIMIT 10
"""
).show(truncate=False)

# =============================================================================
# 5. Data Quality
# =============================================================================

print("\n" + "=" * 60)
print("5. DATA QUALITY")
print("=" * 60)

print("\n5.1 Null Analysis")
print("-" * 40)

spark.sql(
    f"""
    SELECT
        SUM(CASE WHEN event_id IS NULL THEN 1 ELSE 0 END) as null_event_id,
        SUM(CASE WHEN event_type IS NULL THEN 1 ELSE 0 END) as null_event_type,
        SUM(CASE WHEN raw_json IS NULL THEN 1 ELSE 0 END) as null_raw_json,
        SUM(CASE WHEN source_file IS NULL THEN 1 ELSE 0 END) as null_source_file,
        SUM(CASE WHEN _rescued_data IS NOT NULL THEN 1 ELSE 0 END) as has_rescued_data
    FROM {BRONZE_TABLE}
"""
).show(truncate=False)

print("\n5.2 Duplicate Event IDs")
print("-" * 40)

spark.sql(
    f"""
    WITH duplicates AS (
        SELECT event_id, COUNT(*) as cnt
        FROM {BRONZE_TABLE}
        GROUP BY event_id
    )
    SELECT
        SUM(CASE WHEN cnt = 1 THEN 1 ELSE 0 END) as unique_events,
        SUM(CASE WHEN cnt > 1 THEN 1 ELSE 0 END) as duplicate_event_ids,
        SUM(CASE WHEN cnt > 1 THEN cnt ELSE 0 END) as total_duplicate_rows
    FROM duplicates
"""
).show(truncate=False)

# =============================================================================
# 6. Sample Events
# =============================================================================

print("\n" + "=" * 60)
print("6. SAMPLE EVENTS")
print("=" * 60)

print("\n6.1 Sample PushEvents")
print("-" * 40)

spark.sql(
    f"""
    SELECT
        GET_JSON_OBJECT(raw_json, '$.repo.name') as repo,
        GET_JSON_OBJECT(raw_json, '$.actor.login') as actor,
        GET_JSON_OBJECT(raw_json, '$.payload.size') as commit_count,
        GET_JSON_OBJECT(raw_json, '$.payload.ref') as branch_ref
    FROM {BRONZE_TABLE}
    WHERE event_type = 'PushEvent'
    LIMIT 5
"""
).show(truncate=False)

print("\n6.2 Sample PullRequestEvents")
print("-" * 40)

spark.sql(
    f"""
    SELECT
        GET_JSON_OBJECT(raw_json, '$.repo.name') as repo,
        GET_JSON_OBJECT(raw_json, '$.payload.action') as action,
        GET_JSON_OBJECT(raw_json, '$.payload.pull_request.title') as pr_title
    FROM {BRONZE_TABLE}
    WHERE event_type = 'PullRequestEvent'
    LIMIT 5
"""
).show(truncate=False)

# =============================================================================
# Summary
# =============================================================================

print("\n" + "=" * 60)
print("EDA COMPLETE")
print("=" * 60)
print(
    """
Key findings have been displayed above. Review the output for:
- Event type distribution
- Most active users and repositories
- Data quality metrics
- Sample event structures

Next steps:
1. Implement Silver layer transformations
2. Build Gold layer aggregations
"""
)
