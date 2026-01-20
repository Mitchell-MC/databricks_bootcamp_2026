# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer - Repository Lifespan Summary
# MAGIC
# MAGIC **Runs on:** SQL Serverless Warehouse, Unity Catalog
# MAGIC
# MAGIC Creates cumulative repository lifespan metrics:
# MAGIC - **Lifespan:** first/last activity dates, days active
# MAGIC - **Engagement:** cumulative external engagement totals
# MAGIC - **Health:** days since last activity, engagement trend

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG main

# COMMAND ----------

# MAGIC %sql
# MAGIC USE SCHEMA github_gold

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Create Repository Lifespan Table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS main.github_gold.repo_lifespan (
# MAGIC     -- Primary Key
# MAGIC     repo_id BIGINT NOT NULL COMMENT 'Repository ID',
# MAGIC     repo_name STRING NOT NULL COMMENT 'Repository name (owner/repo)',
# MAGIC     repo_owner STRING NOT NULL COMMENT 'Repository owner',
# MAGIC
# MAGIC     -- Lifespan Dates
# MAGIC     first_activity_date DATE COMMENT 'Date of first activity',
# MAGIC     last_activity_date DATE COMMENT 'Date of last activity',
# MAGIC     lifespan_days INT COMMENT 'Days between first and last activity',
# MAGIC     months_active INT COMMENT 'Number of months with activity',
# MAGIC
# MAGIC     -- Cumulative External Engagement
# MAGIC     total_external_stars INT COMMENT 'Cumulative stars from external users',
# MAGIC     total_external_forks INT COMMENT 'Cumulative forks from external users',
# MAGIC     total_external_prs INT COMMENT 'Cumulative PRs from external users',
# MAGIC     total_external_issues INT COMMENT 'Cumulative issues from external users',
# MAGIC     total_unique_contributors INT COMMENT 'Unique external contributors ever',
# MAGIC
# MAGIC     -- Health Indicators
# MAGIC     days_since_last_activity INT COMMENT 'Days since last activity',
# MAGIC     avg_monthly_external_engagement DECIMAL(10,2) COMMENT 'Avg external events per active month',
# MAGIC     engagement_trend STRING COMMENT 'growing, stable, declining, or dormant',
# MAGIC
# MAGIC     -- Metadata
# MAGIC     processed_at TIMESTAMP COMMENT 'Processing timestamp'
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Repository lifespan and health summary (Gold layer)'
# MAGIC TBLPROPERTIES (
# MAGIC     'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC     'delta.autoOptimize.autoCompact' = 'true'
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE main.github_gold.repo_lifespan

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Aggregate Monthly to Lifespan Summary

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create lifespan aggregation with trend calculation
# MAGIC CREATE OR REPLACE TEMP VIEW repo_lifespan_agg AS
# MAGIC WITH repo_totals AS (
# MAGIC     SELECT
# MAGIC         repo_id,
# MAGIC         repo_name,
# MAGIC         repo_owner,
# MAGIC
# MAGIC         -- Lifespan dates (same across all monthly rows due to SCD2)
# MAGIC         MIN(first_activity_date) as first_activity_date,
# MAGIC         MAX(last_activity_date) as last_activity_date,
# MAGIC         DATEDIFF(MAX(last_activity_date), MIN(first_activity_date)) as lifespan_days,
# MAGIC         COUNT(DISTINCT activity_month) as months_active,
# MAGIC
# MAGIC         -- Cumulative external engagement
# MAGIC         SUM(external_watch_count) as total_external_stars,
# MAGIC         SUM(external_fork_count) as total_external_forks,
# MAGIC         SUM(external_pr_count) as total_external_prs,
# MAGIC         SUM(external_issue_count) as total_external_issues,
# MAGIC
# MAGIC         -- Total external events for avg calculation
# MAGIC         SUM(external_watch_count + external_fork_count + external_pr_count +
# MAGIC             external_issue_count + external_push_count + external_comment_count) as total_external_events
# MAGIC
# MAGIC     FROM main.github_gold.repo_monthly_activity
# MAGIC     GROUP BY repo_id, repo_name, repo_owner
# MAGIC ),
# MAGIC unique_contributors AS (
# MAGIC     -- Count unique external contributors from Silver layer
# MAGIC     SELECT
# MAGIC         repo_id,
# MAGIC         COUNT(DISTINCT actor_login) as total_unique_contributors
# MAGIC     FROM main.github_silver.events
# MAGIC     WHERE is_external_actor = TRUE
# MAGIC     GROUP BY repo_id
# MAGIC ),
# MAGIC recent_trend AS (
# MAGIC     -- Calculate engagement trend (last 3 months vs prior 3 months)
# MAGIC     SELECT
# MAGIC         repo_id,
# MAGIC         SUM(CASE WHEN activity_month >= DATE_FORMAT(ADD_MONTHS(CURRENT_DATE(), -3), 'yyyy-MM')
# MAGIC                  THEN external_watch_count + external_fork_count + external_pr_count
# MAGIC                  ELSE 0 END) as recent_engagement,
# MAGIC         SUM(CASE WHEN activity_month >= DATE_FORMAT(ADD_MONTHS(CURRENT_DATE(), -6), 'yyyy-MM')
# MAGIC                   AND activity_month < DATE_FORMAT(ADD_MONTHS(CURRENT_DATE(), -3), 'yyyy-MM')
# MAGIC                  THEN external_watch_count + external_fork_count + external_pr_count
# MAGIC                  ELSE 0 END) as prior_engagement
# MAGIC     FROM main.github_gold.repo_monthly_activity
# MAGIC     GROUP BY repo_id
# MAGIC )
# MAGIC SELECT
# MAGIC     t.repo_id,
# MAGIC     t.repo_name,
# MAGIC     t.repo_owner,
# MAGIC
# MAGIC     t.first_activity_date,
# MAGIC     t.last_activity_date,
# MAGIC     t.lifespan_days,
# MAGIC     t.months_active,
# MAGIC
# MAGIC     t.total_external_stars,
# MAGIC     t.total_external_forks,
# MAGIC     t.total_external_prs,
# MAGIC     t.total_external_issues,
# MAGIC     COALESCE(c.total_unique_contributors, 0) as total_unique_contributors,
# MAGIC
# MAGIC     DATEDIFF(CURRENT_DATE(), t.last_activity_date) as days_since_last_activity,
# MAGIC     ROUND(t.total_external_events / NULLIF(t.months_active, 0), 2) as avg_monthly_external_engagement,
# MAGIC
# MAGIC     -- Engagement trend classification
# MAGIC     CASE
# MAGIC         WHEN DATEDIFF(CURRENT_DATE(), t.last_activity_date) > 180 THEN 'dormant'
# MAGIC         WHEN r.recent_engagement > r.prior_engagement * 1.2 THEN 'growing'
# MAGIC         WHEN r.recent_engagement < r.prior_engagement * 0.8 THEN 'declining'
# MAGIC         ELSE 'stable'
# MAGIC     END as engagement_trend,
# MAGIC
# MAGIC     CURRENT_TIMESTAMP() as processed_at
# MAGIC
# MAGIC FROM repo_totals t
# MAGIC LEFT JOIN unique_contributors c ON t.repo_id = c.repo_id
# MAGIC LEFT JOIN recent_trend r ON t.repo_id = r.repo_id

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Upsert into lifespan table
# MAGIC MERGE INTO main.github_gold.repo_lifespan AS target
# MAGIC USING repo_lifespan_agg AS source
# MAGIC ON target.repo_id = source.repo_id
# MAGIC WHEN MATCHED THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Validate Lifespan Summary

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Overview statistics
# MAGIC SELECT
# MAGIC     COUNT(*) as total_repos,
# MAGIC     AVG(lifespan_days) as avg_lifespan_days,
# MAGIC     AVG(months_active) as avg_months_active,
# MAGIC     AVG(total_external_stars) as avg_external_stars,
# MAGIC     AVG(total_unique_contributors) as avg_unique_contributors
# MAGIC FROM main.github_gold.repo_lifespan

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Repos by engagement trend
# MAGIC SELECT
# MAGIC     engagement_trend,
# MAGIC     COUNT(*) as repo_count,
# MAGIC     AVG(lifespan_days) as avg_lifespan,
# MAGIC     AVG(total_external_stars) as avg_stars,
# MAGIC     AVG(days_since_last_activity) as avg_days_inactive
# MAGIC FROM main.github_gold.repo_lifespan
# MAGIC GROUP BY engagement_trend
# MAGIC ORDER BY repo_count DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Top repos by external engagement
# MAGIC SELECT
# MAGIC     repo_name,
# MAGIC     lifespan_days,
# MAGIC     months_active,
# MAGIC     total_external_stars,
# MAGIC     total_external_forks,
# MAGIC     total_unique_contributors,
# MAGIC     engagement_trend,
# MAGIC     days_since_last_activity
# MAGIC FROM main.github_gold.repo_lifespan
# MAGIC ORDER BY total_external_stars + total_external_forks + total_external_prs DESC
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Repos at risk (declining or dormant)
# MAGIC SELECT
# MAGIC     repo_name,
# MAGIC     engagement_trend,
# MAGIC     days_since_last_activity,
# MAGIC     last_activity_date,
# MAGIC     total_external_stars,
# MAGIC     avg_monthly_external_engagement
# MAGIC FROM main.github_gold.repo_lifespan
# MAGIC WHERE engagement_trend IN ('declining', 'dormant')
# MAGIC ORDER BY total_external_stars DESC
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Lifespan distribution
# MAGIC SELECT
# MAGIC     CASE
# MAGIC         WHEN lifespan_days <= 7 THEN '0-7 days'
# MAGIC         WHEN lifespan_days <= 30 THEN '8-30 days'
# MAGIC         WHEN lifespan_days <= 90 THEN '1-3 months'
# MAGIC         WHEN lifespan_days <= 180 THEN '3-6 months'
# MAGIC         WHEN lifespan_days <= 365 THEN '6-12 months'
# MAGIC         ELSE '1+ year'
# MAGIC     END as lifespan_bucket,
# MAGIC     COUNT(*) as repo_count,
# MAGIC     AVG(total_external_stars) as avg_stars,
# MAGIC     AVG(total_unique_contributors) as avg_contributors
# MAGIC FROM main.github_gold.repo_lifespan
# MAGIC GROUP BY 1
# MAGIC ORDER BY
# MAGIC     CASE
# MAGIC         WHEN lifespan_days <= 7 THEN 1
# MAGIC         WHEN lifespan_days <= 30 THEN 2
# MAGIC         WHEN lifespan_days <= 90 THEN 3
# MAGIC         WHEN lifespan_days <= 180 THEN 4
# MAGIC         WHEN lifespan_days <= 365 THEN 5
# MAGIC         ELSE 6
# MAGIC     END

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC **Gold repo_lifespan Complete:**
# MAGIC - One row per repo (cumulative summary)
# MAGIC - Lifespan metrics: `lifespan_days`, `months_active`
# MAGIC - External engagement totals: stars, forks, PRs, issues, contributors
# MAGIC - Health indicators: `days_since_last_activity`, `engagement_trend`
# MAGIC
# MAGIC **Key Insights Available:**
# MAGIC 1. Which repos have longest lifespan?
# MAGIC 2. Which repos are growing vs declining?
# MAGIC 3. What patterns correlate with repo survival?
# MAGIC 4. Which dormant repos had high engagement (revival candidates)?
