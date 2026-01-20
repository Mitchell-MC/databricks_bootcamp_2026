# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer - Event Flattening
# MAGIC
# MAGIC **Runs on:** SQL Serverless Warehouse, Unity Catalog
# MAGIC
# MAGIC Transforms Bronze raw JSON events into flattened Silver table with:
# MAGIC - Extracted actor, repo, and timestamp fields
# MAGIC - `is_external_actor` flag (actor != repo owner)
# MAGIC - Partitioned by `event_month` (YYYY-MM)

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG main

# COMMAND ----------

# MAGIC %sql
# MAGIC USE SCHEMA github_silver

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Create Silver Events Table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS main.github_silver.events (
# MAGIC     event_id STRING NOT NULL COMMENT 'Unique event identifier',
# MAGIC     event_type STRING NOT NULL COMMENT 'Type of GitHub event',
# MAGIC     actor_id BIGINT COMMENT 'GitHub user ID who triggered event',
# MAGIC     actor_login STRING COMMENT 'GitHub username',
# MAGIC     repo_id BIGINT COMMENT 'Repository ID',
# MAGIC     repo_name STRING COMMENT 'Repository name (owner/repo)',
# MAGIC     repo_owner STRING COMMENT 'Repository owner extracted from repo_name',
# MAGIC     is_external_actor BOOLEAN COMMENT 'True if actor is not the repo owner',
# MAGIC     created_at TIMESTAMP COMMENT 'Event timestamp from GitHub',
# MAGIC     event_month STRING COMMENT 'YYYY-MM format for partitioning',
# MAGIC     payload_json STRING COMMENT 'Event-specific payload as JSON',
# MAGIC     processed_at TIMESTAMP COMMENT 'Processing timestamp'
# MAGIC )
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (event_month)
# MAGIC COMMENT 'Flattened GitHub events with external actor flag (Silver layer)'
# MAGIC TBLPROPERTIES (
# MAGIC     'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC     'delta.autoOptimize.autoCompact' = 'true'
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE main.github_silver.events

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Transform Bronze to Silver
# MAGIC
# MAGIC Extract fields from JSON and compute `is_external_actor`.

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO main.github_silver.events
# MAGIC SELECT
# MAGIC     event_id,
# MAGIC     event_type,
# MAGIC     CAST(GET_JSON_OBJECT(raw_json, '$.actor.id') AS BIGINT) as actor_id,
# MAGIC     GET_JSON_OBJECT(raw_json, '$.actor.login') as actor_login,
# MAGIC     CAST(GET_JSON_OBJECT(raw_json, '$.repo.id') AS BIGINT) as repo_id,
# MAGIC     GET_JSON_OBJECT(raw_json, '$.repo.name') as repo_name,
# MAGIC     SPLIT(GET_JSON_OBJECT(raw_json, '$.repo.name'), '/')[0] as repo_owner,
# MAGIC     -- External actor = actor is not the repo owner
# MAGIC     GET_JSON_OBJECT(raw_json, '$.actor.login') !=
# MAGIC         SPLIT(GET_JSON_OBJECT(raw_json, '$.repo.name'), '/')[0] as is_external_actor,
# MAGIC     TO_TIMESTAMP(GET_JSON_OBJECT(raw_json, '$.created_at')) as created_at,
# MAGIC     DATE_FORMAT(TO_TIMESTAMP(GET_JSON_OBJECT(raw_json, '$.created_at')), 'yyyy-MM') as event_month,
# MAGIC     GET_JSON_OBJECT(raw_json, '$.payload') as payload_json,
# MAGIC     CURRENT_TIMESTAMP() as processed_at
# MAGIC FROM main.github_bronze.raw_events
# MAGIC WHERE event_id NOT IN (SELECT event_id FROM main.github_silver.events)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Validate Transformation

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Row counts comparison
# MAGIC SELECT
# MAGIC     'Bronze' as layer, COUNT(*) as row_count
# MAGIC FROM main.github_bronze.raw_events
# MAGIC UNION ALL
# MAGIC SELECT
# MAGIC     'Silver' as layer, COUNT(*) as row_count
# MAGIC FROM main.github_silver.events

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify is_external_actor flag
# MAGIC SELECT
# MAGIC     is_external_actor,
# MAGIC     COUNT(*) as event_count,
# MAGIC     COUNT(DISTINCT actor_login) as unique_actors
# MAGIC FROM main.github_silver.events
# MAGIC GROUP BY is_external_actor

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Sample data with external actor flag
# MAGIC SELECT
# MAGIC     event_id,
# MAGIC     event_type,
# MAGIC     actor_login,
# MAGIC     repo_owner,
# MAGIC     is_external_actor,
# MAGIC     event_month
# MAGIC FROM main.github_silver.events
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Event distribution by month
# MAGIC SELECT
# MAGIC     event_month,
# MAGIC     COUNT(*) as events,
# MAGIC     SUM(CASE WHEN is_external_actor THEN 1 ELSE 0 END) as external_events,
# MAGIC     ROUND(SUM(CASE WHEN is_external_actor THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as external_pct
# MAGIC FROM main.github_silver.events
# MAGIC GROUP BY event_month
# MAGIC ORDER BY event_month

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC **Silver Layer Complete:**
# MAGIC - Flattened JSON fields (actor, repo, timestamps)
# MAGIC - Added `is_external_actor` flag for engagement analysis
# MAGIC - Partitioned by `event_month` for efficient queries
# MAGIC
# MAGIC **Next:** Run `05_gold_repo_monthly` for monthly aggregations
