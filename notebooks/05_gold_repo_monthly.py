# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer - Repository Monthly Activity
# MAGIC
# MAGIC **Runs on:** SQL Serverless Warehouse, Unity Catalog
# MAGIC
# MAGIC Creates monthly aggregated repo activity with:
# MAGIC - **SCD Type 2:** `first_activity_date`, `last_activity_date`, `is_current`
# MAGIC - **Total counts:** per event type
# MAGIC - **External (non-author) counts:** unique and total
# MAGIC - **Nested JSON:** all activity details for the month

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG main

# COMMAND ----------

# MAGIC %sql
# MAGIC USE SCHEMA github_gold

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Create Repository Monthly Activity Table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS main.github_gold.repo_monthly_activity (
# MAGIC     -- Primary Key
# MAGIC     repo_id BIGINT NOT NULL COMMENT 'Repository ID',
# MAGIC     repo_name STRING NOT NULL COMMENT 'Repository name (owner/repo)',
# MAGIC     repo_owner STRING NOT NULL COMMENT 'Repository owner',
# MAGIC     activity_month STRING NOT NULL COMMENT 'YYYY-MM partition key',
# MAGIC
# MAGIC     -- SCD Type 2 Columns
# MAGIC     first_activity_date DATE COMMENT 'First activity ever for this repo',
# MAGIC     last_activity_date DATE COMMENT 'Last activity ever for this repo',
# MAGIC     is_current BOOLEAN COMMENT 'True if this is the latest month with activity',
# MAGIC
# MAGIC     -- Event Counts (Total)
# MAGIC     total_push_count INT COMMENT 'Total push events this month',
# MAGIC     total_watch_count INT COMMENT 'Total watch/star events this month',
# MAGIC     total_fork_count INT COMMENT 'Total fork events this month',
# MAGIC     total_pr_count INT COMMENT 'Total pull request events this month',
# MAGIC     total_issue_count INT COMMENT 'Total issue events this month',
# MAGIC     total_comment_count INT COMMENT 'Total comment events this month',
# MAGIC     total_create_count INT COMMENT 'Total create events this month',
# MAGIC
# MAGIC     -- External (Non-Author) Unique Counts
# MAGIC     unique_external_pushers INT COMMENT 'Unique external users who pushed',
# MAGIC     unique_external_stargazers INT COMMENT 'Unique external users who starred',
# MAGIC     unique_external_forkers INT COMMENT 'Unique external users who forked',
# MAGIC     unique_external_pr_authors INT COMMENT 'Unique external PR authors',
# MAGIC     unique_external_issue_authors INT COMMENT 'Unique external issue authors',
# MAGIC     unique_external_commenters INT COMMENT 'Unique external commenters',
# MAGIC
# MAGIC     -- External (Non-Author) Total Counts
# MAGIC     external_push_count INT COMMENT 'Total pushes by external users',
# MAGIC     external_watch_count INT COMMENT 'Total stars by external users',
# MAGIC     external_fork_count INT COMMENT 'Total forks by external users',
# MAGIC     external_pr_count INT COMMENT 'Total PRs by external users',
# MAGIC     external_issue_count INT COMMENT 'Total issues by external users',
# MAGIC     external_comment_count INT COMMENT 'Total comments by external users',
# MAGIC
# MAGIC     -- Nested JSON: All Activity Details
# MAGIC     activity_details STRING COMMENT 'JSON array of all events this month',
# MAGIC
# MAGIC     -- Metadata
# MAGIC     processed_at TIMESTAMP COMMENT 'Processing timestamp'
# MAGIC )
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (activity_month)
# MAGIC COMMENT 'Monthly repository activity with SCD2 and external engagement metrics (Gold layer)'
# MAGIC TBLPROPERTIES (
# MAGIC     'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC     'delta.autoOptimize.autoCompact' = 'true'
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE main.github_gold.repo_monthly_activity

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Aggregate Silver to Gold (Monthly Grain)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create temporary aggregation with window functions for SCD2
# MAGIC CREATE OR REPLACE TEMP VIEW repo_monthly_agg AS
# MAGIC WITH monthly_stats AS (
# MAGIC     SELECT
# MAGIC         repo_id,
# MAGIC         repo_name,
# MAGIC         repo_owner,
# MAGIC         event_month,
# MAGIC
# MAGIC         -- First/Last activity dates for this month
# MAGIC         MIN(DATE(created_at)) as month_first_activity,
# MAGIC         MAX(DATE(created_at)) as month_last_activity,
# MAGIC
# MAGIC         -- Total Counts
# MAGIC         SUM(CASE WHEN event_type = 'PushEvent' THEN 1 ELSE 0 END) as total_push_count,
# MAGIC         SUM(CASE WHEN event_type = 'WatchEvent' THEN 1 ELSE 0 END) as total_watch_count,
# MAGIC         SUM(CASE WHEN event_type = 'ForkEvent' THEN 1 ELSE 0 END) as total_fork_count,
# MAGIC         SUM(CASE WHEN event_type = 'PullRequestEvent' THEN 1 ELSE 0 END) as total_pr_count,
# MAGIC         SUM(CASE WHEN event_type = 'IssuesEvent' THEN 1 ELSE 0 END) as total_issue_count,
# MAGIC         SUM(CASE WHEN event_type IN ('IssueCommentEvent', 'PullRequestReviewCommentEvent') THEN 1 ELSE 0 END) as total_comment_count,
# MAGIC         SUM(CASE WHEN event_type = 'CreateEvent' THEN 1 ELSE 0 END) as total_create_count,
# MAGIC
# MAGIC         -- Unique External Counts
# MAGIC         COUNT(DISTINCT CASE WHEN event_type = 'PushEvent' AND is_external_actor THEN actor_login END) as unique_external_pushers,
# MAGIC         COUNT(DISTINCT CASE WHEN event_type = 'WatchEvent' AND is_external_actor THEN actor_login END) as unique_external_stargazers,
# MAGIC         COUNT(DISTINCT CASE WHEN event_type = 'ForkEvent' AND is_external_actor THEN actor_login END) as unique_external_forkers,
# MAGIC         COUNT(DISTINCT CASE WHEN event_type = 'PullRequestEvent' AND is_external_actor THEN actor_login END) as unique_external_pr_authors,
# MAGIC         COUNT(DISTINCT CASE WHEN event_type = 'IssuesEvent' AND is_external_actor THEN actor_login END) as unique_external_issue_authors,
# MAGIC         COUNT(DISTINCT CASE WHEN event_type IN ('IssueCommentEvent', 'PullRequestReviewCommentEvent') AND is_external_actor THEN actor_login END) as unique_external_commenters,
# MAGIC
# MAGIC         -- Total External Counts
# MAGIC         SUM(CASE WHEN event_type = 'PushEvent' AND is_external_actor THEN 1 ELSE 0 END) as external_push_count,
# MAGIC         SUM(CASE WHEN event_type = 'WatchEvent' AND is_external_actor THEN 1 ELSE 0 END) as external_watch_count,
# MAGIC         SUM(CASE WHEN event_type = 'ForkEvent' AND is_external_actor THEN 1 ELSE 0 END) as external_fork_count,
# MAGIC         SUM(CASE WHEN event_type = 'PullRequestEvent' AND is_external_actor THEN 1 ELSE 0 END) as external_pr_count,
# MAGIC         SUM(CASE WHEN event_type = 'IssuesEvent' AND is_external_actor THEN 1 ELSE 0 END) as external_issue_count,
# MAGIC         SUM(CASE WHEN event_type IN ('IssueCommentEvent', 'PullRequestReviewCommentEvent') AND is_external_actor THEN 1 ELSE 0 END) as external_comment_count,
# MAGIC
# MAGIC         -- Activity details as JSON array
# MAGIC         TO_JSON(COLLECT_LIST(
# MAGIC             NAMED_STRUCT(
# MAGIC                 'event_type', event_type,
# MAGIC                 'actor', actor_login,
# MAGIC                 'is_external', is_external_actor,
# MAGIC                 'created_at', DATE_FORMAT(created_at, 'yyyy-MM-dd HH:mm:ss')
# MAGIC             )
# MAGIC         )) as activity_details
# MAGIC
# MAGIC     FROM main.github_silver.events
# MAGIC     GROUP BY repo_id, repo_name, repo_owner, event_month
# MAGIC )
# MAGIC SELECT
# MAGIC     repo_id,
# MAGIC     repo_name,
# MAGIC     repo_owner,
# MAGIC     event_month as activity_month,
# MAGIC
# MAGIC     -- SCD Type 2: First/Last across ALL months for this repo
# MAGIC     MIN(month_first_activity) OVER (PARTITION BY repo_id) as first_activity_date,
# MAGIC     MAX(month_last_activity) OVER (PARTITION BY repo_id) as last_activity_date,
# MAGIC     event_month = MAX(event_month) OVER (PARTITION BY repo_id) as is_current,
# MAGIC
# MAGIC     -- Counts
# MAGIC     total_push_count,
# MAGIC     total_watch_count,
# MAGIC     total_fork_count,
# MAGIC     total_pr_count,
# MAGIC     total_issue_count,
# MAGIC     total_comment_count,
# MAGIC     total_create_count,
# MAGIC
# MAGIC     unique_external_pushers,
# MAGIC     unique_external_stargazers,
# MAGIC     unique_external_forkers,
# MAGIC     unique_external_pr_authors,
# MAGIC     unique_external_issue_authors,
# MAGIC     unique_external_commenters,
# MAGIC
# MAGIC     external_push_count,
# MAGIC     external_watch_count,
# MAGIC     external_fork_count,
# MAGIC     external_pr_count,
# MAGIC     external_issue_count,
# MAGIC     external_comment_count,
# MAGIC
# MAGIC     activity_details,
# MAGIC     CURRENT_TIMESTAMP() as processed_at
# MAGIC FROM monthly_stats

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Insert/Merge into Gold table
# MAGIC MERGE INTO main.github_gold.repo_monthly_activity AS target
# MAGIC USING repo_monthly_agg AS source
# MAGIC ON target.repo_id = source.repo_id AND target.activity_month = source.activity_month
# MAGIC WHEN MATCHED THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Validate Gold Layer

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Row counts by month
# MAGIC SELECT
# MAGIC     activity_month,
# MAGIC     COUNT(*) as repos_active,
# MAGIC     SUM(total_push_count) as total_pushes,
# MAGIC     SUM(external_watch_count) as external_stars
# MAGIC FROM main.github_gold.repo_monthly_activity
# MAGIC GROUP BY activity_month
# MAGIC ORDER BY activity_month

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Top repos by external engagement this month
# MAGIC SELECT
# MAGIC     repo_name,
# MAGIC     activity_month,
# MAGIC     external_watch_count as external_stars,
# MAGIC     external_fork_count as external_forks,
# MAGIC     external_pr_count as external_prs,
# MAGIC     unique_external_stargazers,
# MAGIC     is_current
# MAGIC FROM main.github_gold.repo_monthly_activity
# MAGIC ORDER BY external_watch_count DESC
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SCD Type 2 validation: repos with activity spanning multiple months
# MAGIC SELECT
# MAGIC     repo_name,
# MAGIC     first_activity_date,
# MAGIC     last_activity_date,
# MAGIC     DATEDIFF(last_activity_date, first_activity_date) as lifespan_days,
# MAGIC     COUNT(*) as months_with_activity
# MAGIC FROM main.github_gold.repo_monthly_activity
# MAGIC GROUP BY repo_name, first_activity_date, last_activity_date
# MAGIC HAVING COUNT(*) > 1
# MAGIC ORDER BY lifespan_days DESC
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Sample activity_details JSON
# MAGIC SELECT
# MAGIC     repo_name,
# MAGIC     activity_month,
# MAGIC     activity_details
# MAGIC FROM main.github_gold.repo_monthly_activity
# MAGIC LIMIT 3

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC **Gold repo_monthly_activity Complete:**
# MAGIC - Monthly grain: one row per repo per month
# MAGIC - SCD Type 2: `first_activity_date`, `last_activity_date`, `is_current`
# MAGIC - Total + External counts for all event types
# MAGIC - Nested JSON with activity details
# MAGIC
# MAGIC **Next:** Run `06_gold_repo_lifespan` for cumulative summary
