# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer - Exploratory Data Analysis
# MAGIC
# MAGIC **Runs on:** SQL Serverless Warehouse, Unity Catalog
# MAGIC
# MAGIC Analyzes GitHub Archive data in the Bronze layer.

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG main

# COMMAND ----------

# MAGIC %sql
# MAGIC USE SCHEMA github_bronze

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Data Overview

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Basic statistics
# MAGIC SELECT
# MAGIC     COUNT(*) as total_rows,
# MAGIC     APPROX_COUNT_DISTINCT(event_id) as approx_unique_events,
# MAGIC     COUNT(DISTINCT event_type) as distinct_event_types,
# MAGIC     MIN(ingested_at) as earliest_ingestion,
# MAGIC     MAX(ingested_at) as latest_ingestion
# MAGIC FROM main.github_bronze.raw_events

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE main.github_bronze.raw_events

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Event Type Analysis

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Event type distribution
# MAGIC SELECT
# MAGIC     event_type,
# MAGIC     COUNT(*) as count,
# MAGIC     ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
# MAGIC FROM main.github_bronze.raw_events
# MAGIC GROUP BY event_type
# MAGIC ORDER BY count DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Event categories
# MAGIC SELECT
# MAGIC     CASE
# MAGIC         WHEN event_type IN ('PushEvent', 'CreateEvent', 'DeleteEvent') THEN 'Code Activity'
# MAGIC         WHEN event_type IN ('PullRequestEvent', 'PullRequestReviewEvent') THEN 'Pull Requests'
# MAGIC         WHEN event_type IN ('IssuesEvent', 'IssueCommentEvent') THEN 'Issues'
# MAGIC         WHEN event_type IN ('WatchEvent', 'ForkEvent') THEN 'Social'
# MAGIC         ELSE 'Other'
# MAGIC     END as category,
# MAGIC     COUNT(*) as count
# MAGIC FROM main.github_bronze.raw_events
# MAGIC GROUP BY 1
# MAGIC ORDER BY count DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. JSON Structure Analysis

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Parse JSON and analyze actors
# MAGIC SELECT
# MAGIC     GET_JSON_OBJECT(raw_json, '$.actor.login') as actor_login,
# MAGIC     COUNT(*) as event_count,
# MAGIC     COLLECT_SET(event_type) as event_types
# MAGIC FROM main.github_bronze.raw_events
# MAGIC GROUP BY 1
# MAGIC ORDER BY event_count DESC
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Parse JSON and analyze repositories
# MAGIC SELECT
# MAGIC     GET_JSON_OBJECT(raw_json, '$.repo.name') as repo_name,
# MAGIC     COUNT(*) as event_count,
# MAGIC     COUNT(DISTINCT event_type) as event_types
# MAGIC FROM main.github_bronze.raw_events
# MAGIC GROUP BY 1
# MAGIC ORDER BY event_count DESC
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Temporal Analysis

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Events by ingestion time
# MAGIC SELECT
# MAGIC     DATE_TRUNC('hour', ingested_at) as ingestion_hour,
# MAGIC     COUNT(*) as events
# MAGIC FROM main.github_bronze.raw_events
# MAGIC GROUP BY 1
# MAGIC ORDER BY 1

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Event timestamps from JSON
# MAGIC SELECT
# MAGIC     event_id,
# MAGIC     event_type,
# MAGIC     TO_TIMESTAMP(GET_JSON_OBJECT(raw_json, '$.created_at')) as event_time,
# MAGIC     ingested_at
# MAGIC FROM main.github_bronze.raw_events
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Data Quality

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Null analysis
# MAGIC SELECT
# MAGIC     COUNT(*) as total_rows,
# MAGIC     SUM(CASE WHEN event_id IS NULL THEN 1 ELSE 0 END) as null_event_id,
# MAGIC     SUM(CASE WHEN event_type IS NULL THEN 1 ELSE 0 END) as null_event_type,
# MAGIC     SUM(CASE WHEN raw_json IS NULL THEN 1 ELSE 0 END) as null_raw_json,
# MAGIC     SUM(CASE WHEN _rescued_data IS NOT NULL THEN 1 ELSE 0 END) as has_rescued_data
# MAGIC FROM main.github_bronze.raw_events

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Duplicate detection
# MAGIC SELECT
# MAGIC     COUNT(*) as total_rows,
# MAGIC     APPROX_COUNT_DISTINCT(event_id) as unique_events,
# MAGIC     COUNT(*) - APPROX_COUNT_DISTINCT(event_id) as duplicate_rows
# MAGIC FROM main.github_bronze.raw_events

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Find duplicates
# MAGIC SELECT event_id, COUNT(*) as occurrences
# MAGIC FROM main.github_bronze.raw_events
# MAGIC GROUP BY event_id
# MAGIC HAVING COUNT(*) > 1
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Event Deep Dives

# COMMAND ----------

# MAGIC %sql
# MAGIC -- PushEvent analysis
# MAGIC SELECT
# MAGIC     GET_JSON_OBJECT(raw_json, '$.repo.name') as repo,
# MAGIC     GET_JSON_OBJECT(raw_json, '$.actor.login') as actor,
# MAGIC     CAST(GET_JSON_OBJECT(raw_json, '$.payload.size') AS INT) as commits,
# MAGIC     GET_JSON_OBJECT(raw_json, '$.payload.ref') as branch
# MAGIC FROM main.github_bronze.raw_events
# MAGIC WHERE event_type = 'PushEvent'
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC -- WatchEvent (stars) analysis
# MAGIC SELECT
# MAGIC     GET_JSON_OBJECT(raw_json, '$.repo.name') as repo,
# MAGIC     COUNT(*) as stars
# MAGIC FROM main.github_bronze.raw_events
# MAGIC WHERE event_type = 'WatchEvent'
# MAGIC GROUP BY 1
# MAGIC ORDER BY stars DESC
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC -- PullRequest analysis
# MAGIC SELECT
# MAGIC     GET_JSON_OBJECT(raw_json, '$.payload.action') as action,
# MAGIC     COUNT(*) as count
# MAGIC FROM main.github_bronze.raw_events
# MAGIC WHERE event_type = 'PullRequestEvent'
# MAGIC GROUP BY 1
# MAGIC ORDER BY count DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Sample payload for each event type
# MAGIC SELECT event_type, FIRST(raw_json) as sample_json
# MAGIC FROM main.github_bronze.raw_events
# MAGIC GROUP BY event_type
# MAGIC ORDER BY event_type

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC **Key Findings:**
# MAGIC 1. Event distribution by type
# MAGIC 2. Top actors and repositories
# MAGIC 3. Data quality status
# MAGIC
# MAGIC **Next Steps:**
# MAGIC 1. Implement Silver layer transformations
# MAGIC 2. Build Gold layer aggregations
