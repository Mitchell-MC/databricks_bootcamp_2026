# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer Setup
# MAGIC
# MAGIC **Runs on:** SQL Serverless Warehouse, Unity Catalog
# MAGIC
# MAGIC Creates schemas and tables for the GitHub Archive Lakehouse.

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG main

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS github_bronze
# MAGIC COMMENT 'Bronze layer - raw GitHub Archive events'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS github_silver
# MAGIC COMMENT 'Silver layer - cleansed and typed events'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS github_gold
# MAGIC COMMENT 'Gold layer - aggregated metrics and analytics'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS main.github_bronze.raw_events (
# MAGIC     event_id STRING NOT NULL COMMENT 'Unique event identifier from GH Archive',
# MAGIC     event_type STRING NOT NULL COMMENT 'Type of GitHub event',
# MAGIC     raw_json STRING NOT NULL COMMENT 'Complete event JSON as string',
# MAGIC     source_file STRING NOT NULL COMMENT 'Source archive filename',
# MAGIC     ingested_at TIMESTAMP NOT NULL COMMENT 'Ingestion timestamp',
# MAGIC     _rescued_data STRING COMMENT 'Auto Loader rescued data'
# MAGIC )
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (event_type)
# MAGIC COMMENT 'Raw GitHub events from GH Archive (Bronze layer)'
# MAGIC TBLPROPERTIES (
# MAGIC     'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC     'delta.autoOptimize.autoCompact' = 'true'
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Layer - 24-Hour Activity Table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS main.github_gold.repo_activity_24h (
# MAGIC     -- Primary Key
# MAGIC     repo_id BIGINT NOT NULL COMMENT 'Repository ID',
# MAGIC     repo_name STRING NOT NULL COMMENT 'Repository name (owner/repo)',
# MAGIC
# MAGIC     -- Time Window
# MAGIC     window_start TIMESTAMP COMMENT '24-hour window start',
# MAGIC     window_end TIMESTAMP COMMENT '24-hour window end',
# MAGIC     last_event_at TIMESTAMP COMMENT 'Most recent event timestamp',
# MAGIC
# MAGIC     -- Top Statistics (24h)
# MAGIC     total_events_24h INT COMMENT 'Total events in 24-hour window',
# MAGIC     external_events_24h INT COMMENT 'External (non-owner) events in 24h',
# MAGIC
# MAGIC     -- Event Type Breakdown
# MAGIC     push_count_24h INT COMMENT 'Push events in 24h',
# MAGIC     star_count_24h INT COMMENT 'Star/watch events in 24h',
# MAGIC     fork_count_24h INT COMMENT 'Fork events in 24h',
# MAGIC     pr_count_24h INT COMMENT 'Pull request events in 24h',
# MAGIC     issue_count_24h INT COMMENT 'Issue events in 24h',
# MAGIC     comment_count_24h INT COMMENT 'Comment events in 24h',
# MAGIC
# MAGIC     -- Running Averages
# MAGIC     avg_hourly_events DECIMAL(10,2) COMMENT 'Average events per hour in 24h',
# MAGIC     avg_hourly_external_events DECIMAL(10,2) COMMENT 'Avg external events per hour',
# MAGIC
# MAGIC     -- Sentiment Approximation
# MAGIC     engagement_velocity DECIMAL(10,2) COMMENT 'Events per hour acceleration',
# MAGIC     external_ratio DECIMAL(5,2) COMMENT 'External events / total events',
# MAGIC     sentiment_score STRING COMMENT 'hot, warm, neutral, cooling',
# MAGIC
# MAGIC     -- Unique Actors
# MAGIC     unique_actors_24h INT COMMENT 'Distinct users in 24h',
# MAGIC     unique_external_actors_24h INT COMMENT 'Distinct external users in 24h',
# MAGIC
# MAGIC     -- Top Contributors (nested JSON)
# MAGIC     top_actors_24h STRING COMMENT 'JSON: top 5 actors by event count',
# MAGIC
# MAGIC     -- Dashboard Flags
# MAGIC     is_trending BOOLEAN COMMENT 'True if engagement_velocity > 1.5',
# MAGIC     is_hot BOOLEAN COMMENT 'True if external_ratio > 0.7 and events > 10',
# MAGIC
# MAGIC     -- Metadata
# MAGIC     processed_at TIMESTAMP COMMENT 'Last update timestamp'
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Real-time 24-hour repository activity for dashboard (auto-expires inactive repos)'
# MAGIC TBLPROPERTIES (
# MAGIC     'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC     'delta.autoOptimize.autoCompact' = 'true'
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE EXTENDED main.github_bronze.raw_events

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW SCHEMAS IN main LIKE 'github*'

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup Complete
# MAGIC
# MAGIC | Resource | Status |
# MAGIC |----------|--------|
# MAGIC | `main.github_bronze` | Created |
# MAGIC | `main.github_silver` | Created |
# MAGIC | `main.github_gold` | Created |
# MAGIC | `main.github_bronze.raw_events` | Created |
# MAGIC | `main.github_gold.repo_activity_24h` | Created |
# MAGIC
# MAGIC **Next:** Run `02_bronze_sample_load` to load sample data, or `08_full_data_load` for 3 months
