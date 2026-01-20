# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer - Real-Time 24-Hour Activity Dashboard
# MAGIC
# MAGIC **Runs on:** SQL Serverless Warehouse, Unity Catalog
# MAGIC
# MAGIC Creates a streaming table tracking the past 24 hours of repository activity:
# MAGIC - **Upserts** repos with activity in the past 24 hours
# MAGIC - **Deletes** repos with no activity in 24-hour window
# MAGIC - **Dashboard metrics:** totals, running averages, sentiment approximation
# MAGIC - **Real-time flags:** is_trending, is_hot, sentiment_score

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG main

# COMMAND ----------

# MAGIC %sql
# MAGIC USE SCHEMA github_gold

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Create 24-Hour Activity Table

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
# MAGIC     engagement_velocity DECIMAL(10,2) COMMENT 'Events per hour acceleration (recent 6h vs prior 6h)',
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
# MAGIC DESCRIBE TABLE main.github_gold.repo_activity_24h

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Aggregate 24-Hour Activity from Silver

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create 24-hour aggregation with velocity and sentiment calculations
# MAGIC CREATE OR REPLACE TEMP VIEW repo_24h_agg AS
# MAGIC WITH recent_events AS (
# MAGIC     -- Filter to last 24 hours
# MAGIC     SELECT *
# MAGIC     FROM main.github_silver.events
# MAGIC     WHERE created_at >= CURRENT_TIMESTAMP() - INTERVAL 24 HOURS
# MAGIC ),
# MAGIC hourly_buckets AS (
# MAGIC     -- Bucket events by hour for velocity calculation
# MAGIC     SELECT
# MAGIC         repo_id,
# MAGIC         DATE_TRUNC('hour', created_at) as hour_bucket,
# MAGIC         COUNT(*) as events_in_hour
# MAGIC     FROM recent_events
# MAGIC     GROUP BY repo_id, DATE_TRUNC('hour', created_at)
# MAGIC ),
# MAGIC velocity_calc AS (
# MAGIC     -- Calculate engagement velocity (recent 6h avg - prior 6h avg)
# MAGIC     SELECT
# MAGIC         repo_id,
# MAGIC         AVG(events_in_hour) as avg_hourly,
# MAGIC         COALESCE(
# MAGIC             AVG(CASE WHEN hour_bucket >= CURRENT_TIMESTAMP() - INTERVAL 6 HOURS
# MAGIC                      THEN events_in_hour END), 0
# MAGIC         ) - COALESCE(
# MAGIC             AVG(CASE WHEN hour_bucket < CURRENT_TIMESTAMP() - INTERVAL 6 HOURS
# MAGIC                       AND hour_bucket >= CURRENT_TIMESTAMP() - INTERVAL 12 HOURS
# MAGIC                      THEN events_in_hour END), 0
# MAGIC         ) as velocity
# MAGIC     FROM hourly_buckets
# MAGIC     GROUP BY repo_id
# MAGIC ),
# MAGIC actor_counts AS (
# MAGIC     -- Get top actors per repo
# MAGIC     SELECT
# MAGIC         repo_id,
# MAGIC         actor_login,
# MAGIC         COUNT(*) as actor_events,
# MAGIC         ROW_NUMBER() OVER (PARTITION BY repo_id ORDER BY COUNT(*) DESC) as actor_rank
# MAGIC     FROM recent_events
# MAGIC     GROUP BY repo_id, actor_login
# MAGIC ),
# MAGIC top_actors AS (
# MAGIC     -- Collect top 5 actors as JSON
# MAGIC     SELECT
# MAGIC         repo_id,
# MAGIC         TO_JSON(COLLECT_LIST(NAMED_STRUCT('actor', actor_login, 'events', actor_events))) as top_actors_json
# MAGIC     FROM actor_counts
# MAGIC     WHERE actor_rank <= 5
# MAGIC     GROUP BY repo_id
# MAGIC )
# MAGIC SELECT
# MAGIC     e.repo_id,
# MAGIC     FIRST(e.repo_name) as repo_name,
# MAGIC
# MAGIC     -- Time window
# MAGIC     CURRENT_TIMESTAMP() - INTERVAL 24 HOURS as window_start,
# MAGIC     CURRENT_TIMESTAMP() as window_end,
# MAGIC     MAX(e.created_at) as last_event_at,
# MAGIC
# MAGIC     -- Top statistics
# MAGIC     COUNT(*) as total_events_24h,
# MAGIC     SUM(CASE WHEN e.is_external_actor THEN 1 ELSE 0 END) as external_events_24h,
# MAGIC
# MAGIC     -- Event type breakdown
# MAGIC     SUM(CASE WHEN e.event_type = 'PushEvent' THEN 1 ELSE 0 END) as push_count_24h,
# MAGIC     SUM(CASE WHEN e.event_type = 'WatchEvent' THEN 1 ELSE 0 END) as star_count_24h,
# MAGIC     SUM(CASE WHEN e.event_type = 'ForkEvent' THEN 1 ELSE 0 END) as fork_count_24h,
# MAGIC     SUM(CASE WHEN e.event_type = 'PullRequestEvent' THEN 1 ELSE 0 END) as pr_count_24h,
# MAGIC     SUM(CASE WHEN e.event_type = 'IssuesEvent' THEN 1 ELSE 0 END) as issue_count_24h,
# MAGIC     SUM(CASE WHEN e.event_type IN ('IssueCommentEvent', 'PullRequestReviewCommentEvent') THEN 1 ELSE 0 END) as comment_count_24h,
# MAGIC
# MAGIC     -- Running averages
# MAGIC     ROUND(COALESCE(v.avg_hourly, COUNT(*) / 24.0), 2) as avg_hourly_events,
# MAGIC     ROUND(COALESCE(v.avg_hourly, COUNT(*) / 24.0) *
# MAGIC           (SUM(CASE WHEN e.is_external_actor THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0)), 2) as avg_hourly_external_events,
# MAGIC
# MAGIC     -- Velocity and sentiment
# MAGIC     ROUND(COALESCE(v.velocity, 0), 2) as engagement_velocity,
# MAGIC     ROUND(SUM(CASE WHEN e.is_external_actor THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2) as external_ratio,
# MAGIC
# MAGIC     -- Sentiment score based on velocity and external engagement
# MAGIC     CASE
# MAGIC         WHEN v.velocity > 2 AND
# MAGIC              (SUM(CASE WHEN e.is_external_actor THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0)) > 0.5
# MAGIC              THEN 'hot'
# MAGIC         WHEN v.velocity > 0 THEN 'warm'
# MAGIC         WHEN v.velocity < -1 THEN 'cooling'
# MAGIC         ELSE 'neutral'
# MAGIC     END as sentiment_score,
# MAGIC
# MAGIC     -- Unique actors
# MAGIC     COUNT(DISTINCT e.actor_login) as unique_actors_24h,
# MAGIC     COUNT(DISTINCT CASE WHEN e.is_external_actor THEN e.actor_login END) as unique_external_actors_24h,
# MAGIC
# MAGIC     -- Top actors JSON
# MAGIC     COALESCE(t.top_actors_json, '[]') as top_actors_24h,
# MAGIC
# MAGIC     -- Dashboard flags
# MAGIC     COALESCE(v.velocity, 0) > 1.5 as is_trending,
# MAGIC     (SUM(CASE WHEN e.is_external_actor THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0)) > 0.7
# MAGIC         AND COUNT(*) > 10 as is_hot,
# MAGIC
# MAGIC     CURRENT_TIMESTAMP() as processed_at
# MAGIC
# MAGIC FROM recent_events e
# MAGIC LEFT JOIN velocity_calc v ON e.repo_id = v.repo_id
# MAGIC LEFT JOIN top_actors t ON e.repo_id = t.repo_id
# MAGIC GROUP BY e.repo_id, v.avg_hourly, v.velocity, t.top_actors_json

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Upsert Active Repos (MERGE)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Upsert repos with activity in the past 24 hours
# MAGIC MERGE INTO main.github_gold.repo_activity_24h AS target
# MAGIC USING repo_24h_agg AS source
# MAGIC ON target.repo_id = source.repo_id
# MAGIC WHEN MATCHED THEN UPDATE SET
# MAGIC     repo_name = source.repo_name,
# MAGIC     window_start = source.window_start,
# MAGIC     window_end = source.window_end,
# MAGIC     last_event_at = source.last_event_at,
# MAGIC     total_events_24h = source.total_events_24h,
# MAGIC     external_events_24h = source.external_events_24h,
# MAGIC     push_count_24h = source.push_count_24h,
# MAGIC     star_count_24h = source.star_count_24h,
# MAGIC     fork_count_24h = source.fork_count_24h,
# MAGIC     pr_count_24h = source.pr_count_24h,
# MAGIC     issue_count_24h = source.issue_count_24h,
# MAGIC     comment_count_24h = source.comment_count_24h,
# MAGIC     avg_hourly_events = source.avg_hourly_events,
# MAGIC     avg_hourly_external_events = source.avg_hourly_external_events,
# MAGIC     engagement_velocity = source.engagement_velocity,
# MAGIC     external_ratio = source.external_ratio,
# MAGIC     sentiment_score = source.sentiment_score,
# MAGIC     unique_actors_24h = source.unique_actors_24h,
# MAGIC     unique_external_actors_24h = source.unique_external_actors_24h,
# MAGIC     top_actors_24h = source.top_actors_24h,
# MAGIC     is_trending = source.is_trending,
# MAGIC     is_hot = source.is_hot,
# MAGIC     processed_at = source.processed_at
# MAGIC WHEN NOT MATCHED THEN INSERT *

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Delete Inactive Repos (No Activity in 24h)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Remove repos with no activity in the past 24 hours
# MAGIC DELETE FROM main.github_gold.repo_activity_24h
# MAGIC WHERE last_event_at < CURRENT_TIMESTAMP() - INTERVAL 24 HOURS

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Validate 24-Hour Table

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Overview statistics
# MAGIC SELECT
# MAGIC     COUNT(*) as repos_active_24h,
# MAGIC     SUM(total_events_24h) as total_events,
# MAGIC     SUM(external_events_24h) as external_events,
# MAGIC     ROUND(AVG(external_ratio), 2) as avg_external_ratio,
# MAGIC     MIN(last_event_at) as oldest_event,
# MAGIC     MAX(last_event_at) as newest_event
# MAGIC FROM main.github_gold.repo_activity_24h

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Sentiment distribution
# MAGIC SELECT
# MAGIC     sentiment_score,
# MAGIC     COUNT(*) as repo_count,
# MAGIC     ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as pct,
# MAGIC     ROUND(AVG(total_events_24h), 1) as avg_events,
# MAGIC     ROUND(AVG(external_ratio), 2) as avg_external_ratio
# MAGIC FROM main.github_gold.repo_activity_24h
# MAGIC GROUP BY sentiment_score
# MAGIC ORDER BY repo_count DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Top 10 hottest repos right now
# MAGIC SELECT
# MAGIC     repo_name,
# MAGIC     total_events_24h,
# MAGIC     external_events_24h,
# MAGIC     external_ratio,
# MAGIC     sentiment_score,
# MAGIC     engagement_velocity,
# MAGIC     is_trending,
# MAGIC     is_hot
# MAGIC FROM main.github_gold.repo_activity_24h
# MAGIC WHERE is_hot = TRUE
# MAGIC ORDER BY engagement_velocity DESC
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Trending repos (positive velocity)
# MAGIC SELECT
# MAGIC     repo_name,
# MAGIC     sentiment_score,
# MAGIC     engagement_velocity,
# MAGIC     unique_external_actors_24h,
# MAGIC     star_count_24h,
# MAGIC     fork_count_24h,
# MAGIC     pr_count_24h
# MAGIC FROM main.github_gold.repo_activity_24h
# MAGIC WHERE is_trending = TRUE
# MAGIC ORDER BY engagement_velocity DESC
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Running averages for time-series chart
# MAGIC SELECT
# MAGIC     repo_name,
# MAGIC     avg_hourly_events,
# MAGIC     avg_hourly_external_events,
# MAGIC     total_events_24h,
# MAGIC     last_event_at
# MAGIC FROM main.github_gold.repo_activity_24h
# MAGIC ORDER BY avg_hourly_events DESC
# MAGIC LIMIT 20

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Repos cooling down (negative velocity)
# MAGIC SELECT
# MAGIC     repo_name,
# MAGIC     sentiment_score,
# MAGIC     engagement_velocity,
# MAGIC     total_events_24h,
# MAGIC     external_ratio
# MAGIC FROM main.github_gold.repo_activity_24h
# MAGIC WHERE sentiment_score = 'cooling'
# MAGIC ORDER BY engagement_velocity ASC
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Sample top actors JSON
# MAGIC SELECT
# MAGIC     repo_name,
# MAGIC     total_events_24h,
# MAGIC     top_actors_24h
# MAGIC FROM main.github_gold.repo_activity_24h
# MAGIC WHERE total_events_24h > 5
# MAGIC LIMIT 5

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Dashboard Aggregate Views

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create summary view for dashboard header
# MAGIC CREATE OR REPLACE TEMP VIEW dashboard_summary AS
# MAGIC SELECT
# MAGIC     COUNT(*) as active_repos,
# MAGIC     SUM(total_events_24h) as total_events_24h,
# MAGIC     SUM(external_events_24h) as external_events_24h,
# MAGIC     SUM(CASE WHEN is_hot THEN 1 ELSE 0 END) as hot_repos,
# MAGIC     SUM(CASE WHEN is_trending THEN 1 ELSE 0 END) as trending_repos,
# MAGIC     ROUND(AVG(external_ratio), 2) as avg_external_ratio,
# MAGIC     ROUND(AVG(engagement_velocity), 2) as avg_velocity
# MAGIC FROM main.github_gold.repo_activity_24h

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM dashboard_summary

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Event type totals for pie chart
# MAGIC SELECT
# MAGIC     'Push' as event_type, SUM(push_count_24h) as count FROM main.github_gold.repo_activity_24h
# MAGIC UNION ALL SELECT 'Star', SUM(star_count_24h) FROM main.github_gold.repo_activity_24h
# MAGIC UNION ALL SELECT 'Fork', SUM(fork_count_24h) FROM main.github_gold.repo_activity_24h
# MAGIC UNION ALL SELECT 'PR', SUM(pr_count_24h) FROM main.github_gold.repo_activity_24h
# MAGIC UNION ALL SELECT 'Issue', SUM(issue_count_24h) FROM main.github_gold.repo_activity_24h
# MAGIC UNION ALL SELECT 'Comment', SUM(comment_count_24h) FROM main.github_gold.repo_activity_24h
# MAGIC ORDER BY count DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC **Gold repo_activity_24h Complete:**
# MAGIC - One row per repo with 24-hour activity
# MAGIC - Auto-expires (deletes) repos with no activity
# MAGIC - Sentiment scoring: hot, warm, neutral, cooling
# MAGIC - Dashboard flags: is_trending, is_hot
# MAGIC - Running averages and velocity calculations
# MAGIC
# MAGIC **Schedule:** Run this notebook every 5-15 minutes for real-time updates
# MAGIC
# MAGIC **Dashboard Metrics Available:**
# MAGIC 1. Total active repos / events / external events
# MAGIC 2. Hottest repos (high external engagement + velocity)
# MAGIC 3. Trending repos (positive velocity)
# MAGIC 4. Sentiment distribution
# MAGIC 5. Event type breakdown
