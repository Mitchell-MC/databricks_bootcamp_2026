# Databricks Bootcamp 2026

Welcome to the **Databricks Bootcamp** by **Data With Baraa**.

This repository contains bootcamp materials plus a hands-on **GitHub Archive Lakehouse** project demonstrating production-grade data engineering patterns on Databricks.

---

## What You'll Learn
- How Databricks works at a high level
- How data analysts use Databricks SQL and dashboards
- How data engineers build data pipelines
- How analytics and engineering connect in real workflows

---

## GitHub Archive Lakehouse Project

A real-world data engineering project using GitHub Archive data (3B+ events) to build a medallion architecture lakehouse with streaming capabilities.

### Architecture

```
GH Archive (JSON.gz) → Bronze (Raw) → Silver (Cleansed) → Gold (Analytics)
                         ↑                                      ↓
                    Auto Loader                          Streaming Aggregates
                    (Batch/Stream)                       (Real-time metrics)
```

### Project Structure

```
src/github_lakehouse/
├── config.py              # Pydantic configuration models
├── bronze/                # Raw data ingestion layer
│   ├── schemas.py         # Bronze table schemas
│   └── ingestion_job.py   # Ingestion logic
├── ingestion/             # Data download utilities
│   └── downloader.py      # GH Archive file downloader
├── silver/                # Data cleansing layer (TODO)
├── gold/                  # Business aggregates (TODO)
└── streaming/             # Real-time processing (TODO)
```

### Quick Start

```bash
# Install dependencies
uv sync

# Run tests
uv run pytest src/github_lakehouse/ -v

# Format code
uv run ruff format .

# Lint code
uv run ruff check .
```

### Key Features

- **Delta Lake**: ACID transactions, time travel, schema evolution
- **Medallion Architecture**: Bronze/Silver/Gold layers
- **Structured Streaming**: Real-time event processing with watermarks
- **Auto Loader**: Incremental file ingestion from cloud storage
- **Pydantic Models**: Type-safe configuration and data validation

### Data Source

**GitHub Archive** (https://www.gharchive.org/)
- 3+ billion events since 2011
- Hourly JSON archives (gzip compressed)
- Event types: PushEvent, PullRequestEvent, WatchEvent, ForkEvent, and 30+ more

---

## Bootcamp Materials

### Datasets
- `datasets/analysts/` - Clean dimensional tables (star schema) for SQL analysis
- `datasets/engineering/` - Raw source data (CRM/ERP) with data quality issues

### Notebooks
- `code/EDA Sales Project.ipynb` - Exploratory data analysis example

---

## How to Use This Repository
- Follow along with the bootcamp sessions
- Materials are organized by module
- Clone or download the repository
- Run the GitHub Lakehouse project for hands-on practice

---

## Prerequisites
- Basic SQL knowledge
- No prior Databricks experience required
- Python 3.10+ (for local development)

---

## Development Setup

```bash
# Create virtual environment with uv
uv venv
source .venv/bin/activate  # Unix/macOS
# .venv\Scripts\activate   # Windows

# Install dependencies
uv sync

# Install in development mode
uv pip install -e .
```

---

## Notes
This repository is shared only with bootcamp participants.
Please do not redistribute the content.
