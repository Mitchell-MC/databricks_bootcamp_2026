# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer - Full 3-Month Data Load
# MAGIC
# MAGIC **Runs on:** SQL Serverless Warehouse, Unity Catalog
# MAGIC
# MAGIC Loads 3 months of GitHub Archive data for proper testing:
# MAGIC - **SCD Type 2 validation** (repos spanning multiple months)
# MAGIC - **Trend detection** (growing/declining/dormant patterns)
# MAGIC - **24-hour streaming** behavior with real data
# MAGIC
# MAGIC **Data Source:** https://www.gharchive.org/
# MAGIC **Format:** NDJSON (gzip compressed)
# MAGIC **URL Pattern:** `https://data.gharchive.org/YYYY-MM-DD-H.json.gz`

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG main

# COMMAND ----------

# MAGIC %sql
# MAGIC USE SCHEMA github_bronze

# COMMAND ----------

# MAGIC %md
# MAGIC ## Option 1: Load from Cloud Storage (Recommended)
# MAGIC
# MAGIC If you have GH Archive files in cloud storage (Azure ADLS, AWS S3, GCS):

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Example: Load from Azure Data Lake Storage
# MAGIC -- Uncomment and modify the path to your storage location
# MAGIC
# MAGIC -- COPY INTO main.github_bronze.raw_events
# MAGIC -- FROM 'abfss://gharchive@yourstorage.dfs.core.windows.net/2024-01-*.json.gz'
# MAGIC -- FILEFORMAT = JSON
# MAGIC -- FORMAT_OPTIONS ('multiLine' = 'false')
# MAGIC -- COPY_OPTIONS ('mergeSchema' = 'true');
# MAGIC
# MAGIC -- COPY INTO main.github_bronze.raw_events
# MAGIC -- FROM 'abfss://gharchive@yourstorage.dfs.core.windows.net/2024-02-*.json.gz'
# MAGIC -- FILEFORMAT = JSON;
# MAGIC
# MAGIC -- COPY INTO main.github_bronze.raw_events
# MAGIC -- FROM 'abfss://gharchive@yourstorage.dfs.core.windows.net/2024-03-*.json.gz'
# MAGIC -- FILEFORMAT = JSON;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Option 2: Load from Unity Catalog Volume
# MAGIC
# MAGIC If you've uploaded files to a Unity Catalog Volume:

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Example: Load from UC Volume
# MAGIC -- Uncomment and modify the path to your volume
# MAGIC
# MAGIC -- COPY INTO main.github_bronze.raw_events
# MAGIC -- FROM '/Volumes/main/github_bronze/gharchive/'
# MAGIC -- FILEFORMAT = JSON
# MAGIC -- PATTERN = '2024-01-*.json.gz'
# MAGIC -- COPY_OPTIONS ('mergeSchema' = 'true');

# COMMAND ----------

# MAGIC %md
# MAGIC ## Option 3: Generate Extended Sample Data
# MAGIC
# MAGIC For testing without downloading full GH Archive data,
# MAGIC insert sample data spanning 3 months:

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Generate sample data for January 2024
# MAGIC INSERT INTO main.github_bronze.raw_events (event_id, event_type, raw_json, source_file, ingested_at, _rescued_data)
# MAGIC VALUES
# MAGIC -- Week 1 January
# MAGIC ('35200000001', 'PushEvent', '{"id":"35200000001","type":"PushEvent","actor":{"id":12345,"login":"dev_alice","avatar_url":"https://avatars.githubusercontent.com/u/12345"},"repo":{"id":200001,"name":"startup/api","url":"https://api.github.com/repos/startup/api"},"payload":{"push_id":16000000001,"size":5,"distinct_size":5,"ref":"refs/heads/main","commits":[{"sha":"aaa111","message":"Initial commit"}]},"public":true,"created_at":"2024-01-02T09:00:00Z"}', '2024-01-02-9.json.gz', CURRENT_TIMESTAMP(), NULL),
# MAGIC ('35200000002', 'WatchEvent', '{"id":"35200000002","type":"WatchEvent","actor":{"id":23456,"login":"fan_bob","avatar_url":"https://avatars.githubusercontent.com/u/23456"},"repo":{"id":200001,"name":"startup/api","url":"https://api.github.com/repos/startup/api"},"payload":{"action":"started"},"public":true,"created_at":"2024-01-03T10:00:00Z"}', '2024-01-03-10.json.gz', CURRENT_TIMESTAMP(), NULL),
# MAGIC ('35200000003', 'ForkEvent', '{"id":"35200000003","type":"ForkEvent","actor":{"id":34567,"login":"contributor_carol","avatar_url":"https://avatars.githubusercontent.com/u/34567"},"repo":{"id":200001,"name":"startup/api","url":"https://api.github.com/repos/startup/api"},"payload":{"forkee":{"id":300001,"name":"api-fork","full_name":"contributor_carol/api-fork"}},"public":true,"created_at":"2024-01-05T14:00:00Z"}', '2024-01-05-14.json.gz', CURRENT_TIMESTAMP(), NULL),
# MAGIC -- Week 2 January
# MAGIC ('35200000004', 'PullRequestEvent', '{"id":"35200000004","type":"PullRequestEvent","actor":{"id":34567,"login":"contributor_carol","avatar_url":"https://avatars.githubusercontent.com/u/34567"},"repo":{"id":200001,"name":"startup/api","url":"https://api.github.com/repos/startup/api"},"payload":{"action":"opened","number":1,"pull_request":{"id":7000001,"title":"Add authentication","state":"open","merged":false}},"public":true,"created_at":"2024-01-08T11:00:00Z"}', '2024-01-08-11.json.gz', CURRENT_TIMESTAMP(), NULL),
# MAGIC ('35200000005', 'IssuesEvent', '{"id":"35200000005","type":"IssuesEvent","actor":{"id":45678,"login":"user_david","avatar_url":"https://avatars.githubusercontent.com/u/45678"},"repo":{"id":200001,"name":"startup/api","url":"https://api.github.com/repos/startup/api"},"payload":{"action":"opened","issue":{"id":8000001,"number":1,"title":"Bug: Login fails","state":"open"}},"public":true,"created_at":"2024-01-10T16:00:00Z"}', '2024-01-10-16.json.gz', CURRENT_TIMESTAMP(), NULL),
# MAGIC -- Week 3-4 January
# MAGIC ('35200000006', 'PushEvent', '{"id":"35200000006","type":"PushEvent","actor":{"id":12345,"login":"dev_alice","avatar_url":"https://avatars.githubusercontent.com/u/12345"},"repo":{"id":200001,"name":"startup/api","url":"https://api.github.com/repos/startup/api"},"payload":{"push_id":16000000002,"size":3,"distinct_size":3,"ref":"refs/heads/main","commits":[{"sha":"bbb222","message":"Fix login bug"}]},"public":true,"created_at":"2024-01-15T10:00:00Z"}', '2024-01-15-10.json.gz', CURRENT_TIMESTAMP(), NULL),
# MAGIC ('35200000007', 'WatchEvent', '{"id":"35200000007","type":"WatchEvent","actor":{"id":56789,"login":"fan_eve","avatar_url":"https://avatars.githubusercontent.com/u/56789"},"repo":{"id":200001,"name":"startup/api","url":"https://api.github.com/repos/startup/api"},"payload":{"action":"started"},"public":true,"created_at":"2024-01-20T08:00:00Z"}', '2024-01-20-8.json.gz', CURRENT_TIMESTAMP(), NULL),
# MAGIC ('35200000008', 'PullRequestEvent', '{"id":"35200000008","type":"PullRequestEvent","actor":{"id":12345,"login":"dev_alice","avatar_url":"https://avatars.githubusercontent.com/u/12345"},"repo":{"id":200001,"name":"startup/api","url":"https://api.github.com/repos/startup/api"},"payload":{"action":"closed","number":1,"pull_request":{"id":7000001,"title":"Add authentication","state":"closed","merged":true}},"public":true,"created_at":"2024-01-25T15:00:00Z"}', '2024-01-25-15.json.gz', CURRENT_TIMESTAMP(), NULL)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Generate sample data for February 2024
# MAGIC INSERT INTO main.github_bronze.raw_events (event_id, event_type, raw_json, source_file, ingested_at, _rescued_data)
# MAGIC VALUES
# MAGIC -- Week 1 February (growing engagement)
# MAGIC ('35200000009', 'WatchEvent', '{"id":"35200000009","type":"WatchEvent","actor":{"id":67890,"login":"fan_frank","avatar_url":"https://avatars.githubusercontent.com/u/67890"},"repo":{"id":200001,"name":"startup/api","url":"https://api.github.com/repos/startup/api"},"payload":{"action":"started"},"public":true,"created_at":"2024-02-01T09:00:00Z"}', '2024-02-01-9.json.gz', CURRENT_TIMESTAMP(), NULL),
# MAGIC ('35200000010', 'WatchEvent', '{"id":"35200000010","type":"WatchEvent","actor":{"id":78901,"login":"fan_grace","avatar_url":"https://avatars.githubusercontent.com/u/78901"},"repo":{"id":200001,"name":"startup/api","url":"https://api.github.com/repos/startup/api"},"payload":{"action":"started"},"public":true,"created_at":"2024-02-02T10:00:00Z"}', '2024-02-02-10.json.gz', CURRENT_TIMESTAMP(), NULL),
# MAGIC ('35200000011', 'ForkEvent', '{"id":"35200000011","type":"ForkEvent","actor":{"id":89012,"login":"contributor_henry","avatar_url":"https://avatars.githubusercontent.com/u/89012"},"repo":{"id":200001,"name":"startup/api","url":"https://api.github.com/repos/startup/api"},"payload":{"forkee":{"id":300002,"name":"api-fork-2","full_name":"contributor_henry/api-fork-2"}},"public":true,"created_at":"2024-02-05T14:00:00Z"}', '2024-02-05-14.json.gz', CURRENT_TIMESTAMP(), NULL),
# MAGIC -- Week 2-3 February
# MAGIC ('35200000012', 'PullRequestEvent', '{"id":"35200000012","type":"PullRequestEvent","actor":{"id":89012,"login":"contributor_henry","avatar_url":"https://avatars.githubusercontent.com/u/89012"},"repo":{"id":200001,"name":"startup/api","url":"https://api.github.com/repos/startup/api"},"payload":{"action":"opened","number":2,"pull_request":{"id":7000002,"title":"Add rate limiting","state":"open","merged":false}},"public":true,"created_at":"2024-02-10T11:00:00Z"}', '2024-02-10-11.json.gz', CURRENT_TIMESTAMP(), NULL),
# MAGIC ('35200000013', 'IssueCommentEvent', '{"id":"35200000013","type":"IssueCommentEvent","actor":{"id":12345,"login":"dev_alice","avatar_url":"https://avatars.githubusercontent.com/u/12345"},"repo":{"id":200001,"name":"startup/api","url":"https://api.github.com/repos/startup/api"},"payload":{"action":"created","issue":{"id":8000001,"number":1},"comment":{"id":9000001,"body":"Fixed in v1.1"}},"public":true,"created_at":"2024-02-12T16:00:00Z"}', '2024-02-12-16.json.gz', CURRENT_TIMESTAMP(), NULL),
# MAGIC ('35200000014', 'PushEvent', '{"id":"35200000014","type":"PushEvent","actor":{"id":12345,"login":"dev_alice","avatar_url":"https://avatars.githubusercontent.com/u/12345"},"repo":{"id":200001,"name":"startup/api","url":"https://api.github.com/repos/startup/api"},"payload":{"push_id":16000000003,"size":7,"distinct_size":7,"ref":"refs/heads/main","commits":[{"sha":"ccc333","message":"v1.2 release"}]},"public":true,"created_at":"2024-02-15T10:00:00Z"}', '2024-02-15-10.json.gz', CURRENT_TIMESTAMP(), NULL),
# MAGIC -- Week 4 February (high activity)
# MAGIC ('35200000015', 'WatchEvent', '{"id":"35200000015","type":"WatchEvent","actor":{"id":90123,"login":"fan_ivy","avatar_url":"https://avatars.githubusercontent.com/u/90123"},"repo":{"id":200001,"name":"startup/api","url":"https://api.github.com/repos/startup/api"},"payload":{"action":"started"},"public":true,"created_at":"2024-02-20T08:00:00Z"}', '2024-02-20-8.json.gz', CURRENT_TIMESTAMP(), NULL),
# MAGIC ('35200000016', 'WatchEvent', '{"id":"35200000016","type":"WatchEvent","actor":{"id":11111,"login":"fan_jack","avatar_url":"https://avatars.githubusercontent.com/u/11111"},"repo":{"id":200001,"name":"startup/api","url":"https://api.github.com/repos/startup/api"},"payload":{"action":"started"},"public":true,"created_at":"2024-02-22T09:00:00Z"}', '2024-02-22-9.json.gz', CURRENT_TIMESTAMP(), NULL),
# MAGIC ('35200000017', 'PullRequestEvent', '{"id":"35200000017","type":"PullRequestEvent","actor":{"id":12345,"login":"dev_alice","avatar_url":"https://avatars.githubusercontent.com/u/12345"},"repo":{"id":200001,"name":"startup/api","url":"https://api.github.com/repos/startup/api"},"payload":{"action":"closed","number":2,"pull_request":{"id":7000002,"title":"Add rate limiting","state":"closed","merged":true}},"public":true,"created_at":"2024-02-25T15:00:00Z"}', '2024-02-25-15.json.gz', CURRENT_TIMESTAMP(), NULL)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Generate sample data for March 2024
# MAGIC INSERT INTO main.github_bronze.raw_events (event_id, event_type, raw_json, source_file, ingested_at, _rescued_data)
# MAGIC VALUES
# MAGIC -- Week 1 March (continued growth)
# MAGIC ('35200000018', 'WatchEvent', '{"id":"35200000018","type":"WatchEvent","actor":{"id":22222,"login":"fan_kate","avatar_url":"https://avatars.githubusercontent.com/u/22222"},"repo":{"id":200001,"name":"startup/api","url":"https://api.github.com/repos/startup/api"},"payload":{"action":"started"},"public":true,"created_at":"2024-03-01T09:00:00Z"}', '2024-03-01-9.json.gz', CURRENT_TIMESTAMP(), NULL),
# MAGIC ('35200000019', 'ForkEvent', '{"id":"35200000019","type":"ForkEvent","actor":{"id":33333,"login":"contributor_leo","avatar_url":"https://avatars.githubusercontent.com/u/33333"},"repo":{"id":200001,"name":"startup/api","url":"https://api.github.com/repos/startup/api"},"payload":{"forkee":{"id":300003,"name":"api-fork-3","full_name":"contributor_leo/api-fork-3"}},"public":true,"created_at":"2024-03-03T14:00:00Z"}', '2024-03-03-14.json.gz', CURRENT_TIMESTAMP(), NULL),
# MAGIC ('35200000020', 'PullRequestEvent', '{"id":"35200000020","type":"PullRequestEvent","actor":{"id":33333,"login":"contributor_leo","avatar_url":"https://avatars.githubusercontent.com/u/33333"},"repo":{"id":200001,"name":"startup/api","url":"https://api.github.com/repos/startup/api"},"payload":{"action":"opened","number":3,"pull_request":{"id":7000003,"title":"Add caching layer","state":"open","merged":false}},"public":true,"created_at":"2024-03-05T11:00:00Z"}', '2024-03-05-11.json.gz', CURRENT_TIMESTAMP(), NULL),
# MAGIC -- Week 2-3 March
# MAGIC ('35200000021', 'IssuesEvent', '{"id":"35200000021","type":"IssuesEvent","actor":{"id":44444,"login":"user_mike","avatar_url":"https://avatars.githubusercontent.com/u/44444"},"repo":{"id":200001,"name":"startup/api","url":"https://api.github.com/repos/startup/api"},"payload":{"action":"opened","issue":{"id":8000002,"number":2,"title":"Feature request: WebSocket support","state":"open"}},"public":true,"created_at":"2024-03-08T16:00:00Z"}', '2024-03-08-16.json.gz', CURRENT_TIMESTAMP(), NULL),
# MAGIC ('35200000022', 'PushEvent', '{"id":"35200000022","type":"PushEvent","actor":{"id":12345,"login":"dev_alice","avatar_url":"https://avatars.githubusercontent.com/u/12345"},"repo":{"id":200001,"name":"startup/api","url":"https://api.github.com/repos/startup/api"},"payload":{"push_id":16000000004,"size":10,"distinct_size":10,"ref":"refs/heads/main","commits":[{"sha":"ddd444","message":"v2.0 major release"}]},"public":true,"created_at":"2024-03-10T10:00:00Z"}', '2024-03-10-10.json.gz', CURRENT_TIMESTAMP(), NULL),
# MAGIC ('35200000023', 'WatchEvent', '{"id":"35200000023","type":"WatchEvent","actor":{"id":55555,"login":"fan_nancy","avatar_url":"https://avatars.githubusercontent.com/u/55555"},"repo":{"id":200001,"name":"startup/api","url":"https://api.github.com/repos/startup/api"},"payload":{"action":"started"},"public":true,"created_at":"2024-03-12T08:00:00Z"}', '2024-03-12-8.json.gz', CURRENT_TIMESTAMP(), NULL),
# MAGIC -- Week 4 March
# MAGIC ('35200000024', 'PullRequestEvent', '{"id":"35200000024","type":"PullRequestEvent","actor":{"id":12345,"login":"dev_alice","avatar_url":"https://avatars.githubusercontent.com/u/12345"},"repo":{"id":200001,"name":"startup/api","url":"https://api.github.com/repos/startup/api"},"payload":{"action":"closed","number":3,"pull_request":{"id":7000003,"title":"Add caching layer","state":"closed","merged":true}},"public":true,"created_at":"2024-03-20T15:00:00Z"}', '2024-03-20-15.json.gz', CURRENT_TIMESTAMP(), NULL),
# MAGIC ('35200000025', 'WatchEvent', '{"id":"35200000025","type":"WatchEvent","actor":{"id":66666,"login":"fan_oliver","avatar_url":"https://avatars.githubusercontent.com/u/66666"},"repo":{"id":200001,"name":"startup/api","url":"https://api.github.com/repos/startup/api"},"payload":{"action":"started"},"public":true,"created_at":"2024-03-25T09:00:00Z"}', '2024-03-25-9.json.gz', CURRENT_TIMESTAMP(), NULL),
# MAGIC ('35200000026', 'ForkEvent', '{"id":"35200000026","type":"ForkEvent","actor":{"id":77777,"login":"contributor_pat","avatar_url":"https://avatars.githubusercontent.com/u/77777"},"repo":{"id":200001,"name":"startup/api","url":"https://api.github.com/repos/startup/api"},"payload":{"forkee":{"id":300004,"name":"api-fork-4","full_name":"contributor_pat/api-fork-4"}},"public":true,"created_at":"2024-03-28T14:00:00Z"}', '2024-03-28-14.json.gz', CURRENT_TIMESTAMP(), NULL)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Add a second repo with different activity pattern (declining)
# MAGIC INSERT INTO main.github_bronze.raw_events (event_id, event_type, raw_json, source_file, ingested_at, _rescued_data)
# MAGIC VALUES
# MAGIC -- January: High activity
# MAGIC ('35300000001', 'PushEvent', '{"id":"35300000001","type":"PushEvent","actor":{"id":99999,"login":"old_dev","avatar_url":"https://avatars.githubusercontent.com/u/99999"},"repo":{"id":200002,"name":"legacy/project","url":"https://api.github.com/repos/legacy/project"},"payload":{"push_id":17000000001,"size":5,"ref":"refs/heads/main","commits":[{"sha":"eee555","message":"Update deps"}]},"public":true,"created_at":"2024-01-05T09:00:00Z"}', '2024-01-05-9.json.gz', CURRENT_TIMESTAMP(), NULL),
# MAGIC ('35300000002', 'WatchEvent', '{"id":"35300000002","type":"WatchEvent","actor":{"id":88888,"login":"fan_old1","avatar_url":"https://avatars.githubusercontent.com/u/88888"},"repo":{"id":200002,"name":"legacy/project","url":"https://api.github.com/repos/legacy/project"},"payload":{"action":"started"},"public":true,"created_at":"2024-01-10T10:00:00Z"}', '2024-01-10-10.json.gz', CURRENT_TIMESTAMP(), NULL),
# MAGIC ('35300000003', 'WatchEvent', '{"id":"35300000003","type":"WatchEvent","actor":{"id":77788,"login":"fan_old2","avatar_url":"https://avatars.githubusercontent.com/u/77788"},"repo":{"id":200002,"name":"legacy/project","url":"https://api.github.com/repos/legacy/project"},"payload":{"action":"started"},"public":true,"created_at":"2024-01-15T11:00:00Z"}', '2024-01-15-11.json.gz', CURRENT_TIMESTAMP(), NULL),
# MAGIC -- February: Less activity
# MAGIC ('35300000004', 'PushEvent', '{"id":"35300000004","type":"PushEvent","actor":{"id":99999,"login":"old_dev","avatar_url":"https://avatars.githubusercontent.com/u/99999"},"repo":{"id":200002,"name":"legacy/project","url":"https://api.github.com/repos/legacy/project"},"payload":{"push_id":17000000002,"size":1,"ref":"refs/heads/main","commits":[{"sha":"fff666","message":"Minor fix"}]},"public":true,"created_at":"2024-02-15T09:00:00Z"}', '2024-02-15-9.json.gz', CURRENT_TIMESTAMP(), NULL),
# MAGIC -- March: Almost no activity (dormant)
# MAGIC ('35300000005', 'IssuesEvent', '{"id":"35300000005","type":"IssuesEvent","actor":{"id":66677,"login":"user_reporter","avatar_url":"https://avatars.githubusercontent.com/u/66677"},"repo":{"id":200002,"name":"legacy/project","url":"https://api.github.com/repos/legacy/project"},"payload":{"action":"opened","issue":{"id":8000003,"number":1,"title":"Is this project maintained?","state":"open"}},"public":true,"created_at":"2024-03-01T16:00:00Z"}', '2024-03-01-16.json.gz', CURRENT_TIMESTAMP(), NULL)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validate 3-Month Data Load

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify date range covered
# MAGIC SELECT
# MAGIC     MIN(TO_TIMESTAMP(GET_JSON_OBJECT(raw_json, '$.created_at'))) as earliest_event,
# MAGIC     MAX(TO_TIMESTAMP(GET_JSON_OBJECT(raw_json, '$.created_at'))) as latest_event,
# MAGIC     DATEDIFF(
# MAGIC         MAX(TO_TIMESTAMP(GET_JSON_OBJECT(raw_json, '$.created_at'))),
# MAGIC         MIN(TO_TIMESTAMP(GET_JSON_OBJECT(raw_json, '$.created_at')))
# MAGIC     ) as days_covered,
# MAGIC     COUNT(*) as total_events
# MAGIC FROM main.github_bronze.raw_events

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Events by month
# MAGIC SELECT
# MAGIC     DATE_FORMAT(TO_TIMESTAMP(GET_JSON_OBJECT(raw_json, '$.created_at')), 'yyyy-MM') as month,
# MAGIC     COUNT(*) as events,
# MAGIC     COUNT(DISTINCT GET_JSON_OBJECT(raw_json, '$.repo.name')) as unique_repos,
# MAGIC     COUNT(DISTINCT GET_JSON_OBJECT(raw_json, '$.actor.login')) as unique_actors
# MAGIC FROM main.github_bronze.raw_events
# MAGIC GROUP BY 1
# MAGIC ORDER BY 1

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Event type distribution
# MAGIC SELECT
# MAGIC     event_type,
# MAGIC     COUNT(*) as count,
# MAGIC     ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as pct
# MAGIC FROM main.github_bronze.raw_events
# MAGIC GROUP BY event_type
# MAGIC ORDER BY count DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Repos with activity spanning multiple months
# MAGIC SELECT
# MAGIC     GET_JSON_OBJECT(raw_json, '$.repo.name') as repo_name,
# MAGIC     COUNT(DISTINCT DATE_FORMAT(TO_TIMESTAMP(GET_JSON_OBJECT(raw_json, '$.created_at')), 'yyyy-MM')) as months_active,
# MAGIC     MIN(TO_TIMESTAMP(GET_JSON_OBJECT(raw_json, '$.created_at'))) as first_activity,
# MAGIC     MAX(TO_TIMESTAMP(GET_JSON_OBJECT(raw_json, '$.created_at'))) as last_activity,
# MAGIC     COUNT(*) as total_events
# MAGIC FROM main.github_bronze.raw_events
# MAGIC GROUP BY GET_JSON_OBJECT(raw_json, '$.repo.name')
# MAGIC ORDER BY months_active DESC, total_events DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC **3-Month Data Load Complete:**
# MAGIC - Sample data spanning January - March 2024
# MAGIC - Multiple repos with different activity patterns
# MAGIC - `startup/api`: Growing engagement (external stars, forks, PRs)
# MAGIC - `legacy/project`: Declining activity (dormant pattern)
# MAGIC
# MAGIC **Next Steps:**
# MAGIC 1. Run `04_silver_transform` to populate Silver layer
# MAGIC 2. Run `05_gold_repo_monthly` for monthly aggregations
# MAGIC 3. Run `06_gold_repo_lifespan` for health indicators
# MAGIC 4. Run `07_streaming_24h_activity` for real-time dashboard
# MAGIC
# MAGIC **Expected Results:**
# MAGIC - `startup/api` should show `engagement_trend = 'growing'`
# MAGIC - `legacy/project` should show `engagement_trend = 'declining'` or `'dormant'`
