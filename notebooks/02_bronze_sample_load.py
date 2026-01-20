# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer - 3-Month Data Load (Idempotent)
# MAGIC
# MAGIC **Runs on:** Databricks Cluster with Unity Catalog
# MAGIC
# MAGIC Loads 3 months of GitHub Archive data into the Bronze table.
# MAGIC 
# MAGIC **Key Features:**
# MAGIC - **Idempotent**: Safe to re-run - uses MERGE to prevent duplicates
# MAGIC - **3-Month Range**: Configurable date range (default: Jan-Mar 2024)
# MAGIC - **Direct HTTP Load**: Streams from gharchive.org
# MAGIC
# MAGIC **Data Source:** https://www.gharchive.org/
# MAGIC **Format:** NDJSON (gzip compressed)
# MAGIC **URL Pattern:** `https://data.gharchive.org/YYYY-MM-DD-H.json.gz`

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

from datetime import datetime, timedelta
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

# Date range configuration - 3 months of data
START_DATE = "2024-01-01"
END_DATE = "2024-03-31"

# Sample hours per day (reduce for faster loads, increase for more data)
# 24 = all hours, 1 = one hour per day
HOURS_PER_DAY = 1  # Set to 24 for full data

print(f"Loading GH Archive data from {START_DATE} to {END_DATE}")
print(f"Sampling {HOURS_PER_DAY} hour(s) per day")

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG main;
# MAGIC USE SCHEMA github_bronze;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate URL List for 3 Months

# COMMAND ----------

def generate_gharchive_urls(start_date: str, end_date: str, hours_per_day: int = 1) -> list:
    """
    Generate list of GH Archive URLs for a date range.
    
    Args:
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
        hours_per_day: Number of hours to sample per day (1-24)
    
    Returns:
        List of GH Archive URLs
    """
    urls = []
    current = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    
    # Determine which hours to sample
    if hours_per_day >= 24:
        sample_hours = list(range(24))
    else:
        # Spread samples across the day
        step = 24 // hours_per_day
        sample_hours = list(range(0, 24, step))[:hours_per_day]
    
    while current <= end:
        date_str = current.strftime("%Y-%m-%d")
        for hour in sample_hours:
            url = f"https://data.gharchive.org/{date_str}-{hour}.json.gz"
            urls.append(url)
        current += timedelta(days=1)
    
    return urls

# Generate URLs
urls = generate_gharchive_urls(START_DATE, END_DATE, HOURS_PER_DAY)
print(f"Generated {len(urls)} URLs to load")
print(f"First URL: {urls[0]}")
print(f"Last URL: {urls[-1]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Data with Idempotent MERGE
# MAGIC 
# MAGIC Uses MERGE to ensure re-running the notebook doesn't create duplicates.

# COMMAND ----------

def load_gharchive_file(url: str):
    """
    Load a single GH Archive file and return a DataFrame.
    Returns None if the file cannot be loaded.
    """
    try:
        # Extract filename from URL for source tracking
        filename = url.split("/")[-1]
        
        # Read JSON directly from URL
        df = (spark.read
              .option("multiLine", "false")
              .json(url)
              .withColumn("source_file", F.lit(filename))
              .withColumn("ingested_at", F.current_timestamp()))
        
        return df
    except Exception as e:
        print(f"Warning: Could not load {url}: {str(e)[:100]}")
        return None

# COMMAND ----------

# MAGIC %md
# MAGIC ## Batch Load with Progress Tracking

# COMMAND ----------

from pyspark.sql import DataFrame

def merge_to_bronze(df: DataFrame):
    """
    Merge DataFrame into bronze table (idempotent).
    Uses event_id as the unique key to prevent duplicates.
    """
    # Prepare data for merge
    staged = df.select(
        F.col("id").alias("event_id"),
        F.col("type").alias("event_type"),
        F.to_json(F.struct("*")).alias("raw_json"),
        F.col("source_file"),
        F.col("ingested_at")
    ).filter(F.col("event_id").isNotNull())
    
    # Create temp view for merge
    staged.createOrReplaceTempView("staged_events")
    
    # Idempotent merge - only insert new events
    spark.sql("""
        MERGE INTO main.github_bronze.raw_events AS target
        USING staged_events AS source
        ON target.event_id = source.event_id
        WHEN NOT MATCHED THEN
            INSERT (event_id, event_type, raw_json, source_file, ingested_at, _rescued_data)
            VALUES (source.event_id, source.event_type, source.raw_json, source.source_file, source.ingested_at, NULL)
    """)

# COMMAND ----------

# Process URLs in batches
BATCH_SIZE = 7  # Process 7 days at a time

total_loaded = 0
total_errors = 0

for i in range(0, len(urls), BATCH_SIZE):
    batch_urls = urls[i:i + BATCH_SIZE]
    batch_num = (i // BATCH_SIZE) + 1
    total_batches = (len(urls) + BATCH_SIZE - 1) // BATCH_SIZE
    
    print(f"\n--- Batch {batch_num}/{total_batches} ({len(batch_urls)} files) ---")
    
    # Load and union all files in batch
    batch_dfs = []
    for url in batch_urls:
        df = load_gharchive_file(url)
        if df is not None:
            batch_dfs.append(df)
            total_loaded += 1
        else:
            total_errors += 1
    
    if batch_dfs:
        # Union all DataFrames in batch
        combined_df = batch_dfs[0]
        for df in batch_dfs[1:]:
            combined_df = combined_df.unionByName(df, allowMissingColumns=True)
        
        # Merge to bronze table (idempotent)
        event_count = combined_df.count()
        merge_to_bronze(combined_df)
        print(f"Merged {event_count:,} events from batch {batch_num}")

print(f"\n=== Load Complete ===")
print(f"Files loaded: {total_loaded}")
print(f"Files with errors: {total_errors}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validate 3-Month Load

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Event counts by type
# MAGIC SELECT event_type, FORMAT_NUMBER(COUNT(*), 0) as count
# MAGIC FROM main.github_bronze.raw_events
# MAGIC GROUP BY event_type
# MAGIC ORDER BY COUNT(*) DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Monthly distribution across 3 months
# MAGIC SELECT 
# MAGIC     DATE_FORMAT(
# MAGIC         TO_TIMESTAMP(get_json_object(raw_json, '$.created_at')), 
# MAGIC         'yyyy-MM'
# MAGIC     ) as month,
# MAGIC     FORMAT_NUMBER(COUNT(*), 0) as events
# MAGIC FROM main.github_bronze.raw_events
# MAGIC GROUP BY 1
# MAGIC ORDER BY 1

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Summary statistics
# MAGIC SELECT
# MAGIC     FORMAT_NUMBER(COUNT(*), 0) as total_events,
# MAGIC     COUNT(DISTINCT event_type) as event_types,
# MAGIC     FORMAT_NUMBER(COUNT(DISTINCT get_json_object(raw_json, '$.repo.id')), 0) as unique_repos,
# MAGIC     FORMAT_NUMBER(COUNT(DISTINCT get_json_object(raw_json, '$.actor.id')), 0) as unique_actors,
# MAGIC     MIN(get_json_object(raw_json, '$.created_at')) as earliest_event,
# MAGIC     MAX(get_json_object(raw_json, '$.created_at')) as latest_event
# MAGIC FROM main.github_bronze.raw_events

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify idempotency: Check for duplicates
# MAGIC SELECT 
# MAGIC     CASE 
# MAGIC         WHEN COUNT(*) = COUNT(DISTINCT event_id) THEN '✅ No duplicates - idempotent load working'
# MAGIC         ELSE '❌ Duplicates found: ' || (COUNT(*) - COUNT(DISTINCT event_id))
# MAGIC     END as idempotency_check
# MAGIC FROM main.github_bronze.raw_events

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3-Month Load Complete
# MAGIC
# MAGIC Data loaded from **January 1 - March 31, 2024** (configurable).
# MAGIC
# MAGIC **Idempotency:** This notebook can be safely re-run. The MERGE operation
# MAGIC only inserts events with new `event_id` values, preventing duplicates.
# MAGIC
# MAGIC **Next Steps:**
# MAGIC - Run `03_bronze_eda` for exploratory analysis
# MAGIC - Run `04_silver_transform` for data processing
