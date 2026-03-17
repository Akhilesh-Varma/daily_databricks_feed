# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Ingestion — PySpark 4.0 Custom Streaming Data Sources
# MAGIC
# MAGIC All news sources are implemented as **PySpark 4.0 Custom Data Sources** and
# MAGIC read via Structured Streaming with an **`availableNow`** trigger.
# MAGIC
# MAGIC | Source | Auth required | Format name |
# MAGIC |--------|--------------|-------------|
# MAGIC | Hacker News (Algolia) | None | `hacker_news_news` |
# MAGIC | RSS feeds (blogs, Medium, arXiv) | None | `rss_news` |
# MAGIC | GitHub Releases | Optional `GITHUB_TOKEN` | `github_releases_news` |
# MAGIC | Databricks Community (Discourse) | None | `discourse_news` |
# MAGIC | Dev.to articles | None | `devto_news` |
# MAGIC | PyPI package releases | None | `pypi_releases_news` |
# MAGIC | Reddit | `REDDIT_CLIENT_ID` + `SECRET` | `reddit_news` |
# MAGIC | YouTube | `YOUTUBE_API_KEY` | `youtube_news` |
# MAGIC | Stack Overflow | Optional `STACK_EXCHANGE_API_KEY` | `stackoverflow_news` |
# MAGIC
# MAGIC ### How it works
# MAGIC | Step | What happens |
# MAGIC |------|-------------|
# MAGIC | First run | `initialOffset` goes back `days_back` days; all matching content is fetched |
# MAGIC | Subsequent runs | Checkpoint stores the previous end-epoch; only content **since that epoch** is fetched |
# MAGIC | Write | All records appended to `bronze_raw_landing` (Delta, Unity Catalog) |
# MAGIC | JSON export | Run's records merged into `bronze_news_raw.json` for downstream notebooks |

# COMMAND ----------

# MAGIC %pip install requests praw feedparser python-dotenv

# COMMAND ----------

import json
import logging
import os
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pyspark.sql.functions as F
from pyspark.sql import SparkSession

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

project_root = Path(os.getcwd()).parent
if str(project_root / "src") not in sys.path:
    sys.path.insert(0, str(project_root / "src"))

# COMMAND ----------

from daily_databricks_feed.utils.secrets import SecretsManager, load_dotenv

env_file = project_root / ".env"
if env_file.exists():
    load_dotenv(str(env_file))
    logger.info("Loaded environment from .env file")

secrets = SecretsManager()
secrets.print_status()

# COMMAND ----------

# Configuration
DATA_PATH = os.environ.get("DATA_PATH", str(project_root / "data"))
try:
    DAYS_BACK = int(dbutils.widgets.get("DAYS_BACK"))
except Exception:
    DAYS_BACK = int(os.environ.get("DAYS_BACK", "1"))
CHECKPOINT_PATH = os.environ.get(
    "CHECKPOINT_PATH",
    f"{DATA_PATH}/checkpoints/bronze_ingestion",
)

logger.info("Data path:       %s", DATA_PATH)
logger.info("Days back:       %d", DAYS_BACK)
logger.info("Checkpoint path: %s", CHECKPOINT_PATH)

Path(DATA_PATH).mkdir(parents=True, exist_ok=True)

# COMMAND ----------

# Run tracking — same ID across all tasks in one Databricks job run
def _get_run_id() -> str:
    try:
        return (
            dbutils.notebook.entry_point
                   .getDbutils().notebook().getContext()
                   .tags().apply("jobRunId")
        )
    except Exception:
        return f"local-{uuid.uuid4().hex[:8]}"

_run_id          = _get_run_id()
_pipeline_run_at = datetime.now(timezone.utc).isoformat()

logger.info("Run ID: %s", _run_id)

# COMMAND ----------

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Register PySpark 4.0 Custom Data Sources

# COMMAND ----------

from daily_databricks_feed.data_sources.pyspark_sources import (
    DevToDataSource,
    DiscourseDataSource,
    GitHubReleasesDataSource,
    HackerNewsDataSource,
    PyPIDataSource,
    RedditDataSource,
    RSSFeedDataSource,
    StackOverflowDataSource,
    YouTubeDataSource,
)

spark.dataSource.register(HackerNewsDataSource)
spark.dataSource.register(RSSFeedDataSource)
spark.dataSource.register(GitHubReleasesDataSource)
spark.dataSource.register(DiscourseDataSource)
spark.dataSource.register(DevToDataSource)
spark.dataSource.register(PyPIDataSource)
spark.dataSource.register(RedditDataSource)
spark.dataSource.register(YouTubeDataSource)
spark.dataSource.register(StackOverflowDataSource)

logger.info("Registered all 9 PySpark 4.0 Custom Data Sources")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build Streaming Sources

# COMMAND ----------

streams = []  # list of (source_name, streaming_DataFrame)

# ── Hacker News (public API — no credentials required) ───────────────────────
hn_stream = (
    spark.readStream
         .format("hacker_news_news")
         .option("days_back",         str(DAYS_BACK))
         .option("min_points",        "5")
         .option("limit",             "50")
         .option("filter_databricks", "true")
         .load()
)
streams.append(("hacker_news", hn_stream))
logger.info("Built Hacker News stream")

# ── RSS Feeds (blogs, Medium, arXiv newsletters — no credentials) ─────────────
rss_stream = (
    spark.readStream
         .format("rss_news")
         .option("days_back",         str(max(DAYS_BACK, 7)))
         .option("limit",             "100")
         .option("filter_databricks", "true")
         .load()
)
streams.append(("rss_feeds", rss_stream))
logger.info("Built RSS stream")

# ── GitHub Releases (works without token; GITHUB_TOKEN gives 5000 req/hr) ────
github_stream = (
    spark.readStream
         .format("github_releases_news")
         .option("days_back",          str(max(DAYS_BACK, 7)))
         .option("limit",              "50")
         .option("include_prereleases","false")
         .load()
)
streams.append(("github_releases", github_stream))
logger.info("Built GitHub Releases stream")

# ── Databricks Community Forum (public Discourse API — no credentials) ────────
discourse_stream = (
    spark.readStream
         .format("discourse_news")
         .option("days_back", str(DAYS_BACK))
         .option("limit",     "50")
         .load()
)
streams.append(("databricks_community", discourse_stream))
logger.info("Built Databricks Community stream")

# ── Dev.to articles (public Forem API — no credentials) ───────────────────────
devto_stream = (
    spark.readStream
         .format("devto_news")
         .option("days_back",         str(DAYS_BACK))
         .option("limit",             "50")
         .option("filter_databricks", "true")
         .load()
)
streams.append(("devto", devto_stream))
logger.info("Built Dev.to stream")

# ── PyPI Releases (public JSON API — no credentials) ──────────────────────────
pypi_stream = (
    spark.readStream
         .format("pypi_releases_news")
         .option("days_back", str(max(DAYS_BACK, 7)))
         .option("limit",     "50")
         .load()
)
streams.append(("pypi_releases", pypi_stream))
logger.info("Built PyPI Releases stream")

# ── Stack Overflow (300 req/day free; STACK_EXCHANGE_API_KEY → 10,000/day) ────
so_stream = (
    spark.readStream
         .format("stackoverflow_news")
         .option("days_back",         str(DAYS_BACK))
         .option("limit",             "30")
         .option("min_answers",       "1")
         .option("min_score",         "2")
         .option("filter_databricks", "true")
         .load()
)
streams.append(("stackoverflow", so_stream))
logger.info("Built Stack Overflow stream")

# ── Reddit (requires REDDIT_CLIENT_ID + REDDIT_CLIENT_SECRET) ────────────────
if os.environ.get("REDDIT_CLIENT_ID"):
    reddit_stream = (
        spark.readStream
             .format("reddit_news")
             .option("days_back",         str(DAYS_BACK))
             .option("min_score",         "3")
             .option("limit",             "50")
             .option("filter_databricks", "true")
             .load()
    )
    streams.append(("reddit", reddit_stream))
    logger.info("Built Reddit stream")
else:
    logger.warning("REDDIT_CLIENT_ID not set — Reddit stream skipped")

# ── YouTube (requires YOUTUBE_API_KEY; minimum 7-day lookback) ───────────────
if os.environ.get("YOUTUBE_API_KEY"):
    youtube_stream = (
        spark.readStream
             .format("youtube_news")
             .option("days_back",         str(max(DAYS_BACK, 7)))
             .option("limit",             "30")
             .option("filter_databricks", "true")
             .load()
    )
    streams.append(("youtube", youtube_stream))
    logger.info("Built YouTube stream")
else:
    logger.warning("YOUTUBE_API_KEY not set — YouTube stream skipped")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Union All Sources and Attach Run Metadata

# COMMAND ----------

# Union all active streams (all share BRONZE_SCHEMA — safe to union directly)
unified = streams[0][1]
for _, s in streams[1:]:
    unified = unified.union(s)

# Attach run-level columns as literals so every row is traceable to this run
all_stream = (
    unified
    .withColumn("_run_id",          F.lit(_run_id))
    .withColumn("_pipeline_run_at", F.lit(_pipeline_run_at))
)

logger.info(
    "Unified %d stream(s): %s",
    len(streams),
    [name for name, _ in streams],
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Delta — `availableNow` Trigger

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS news_pipeline.daily_databricks_feed")

query = (
    all_stream
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .option("mergeSchema",        "true")
    .trigger(availableNow=True)          # process all available data, then stop
    .toTable("news_pipeline.daily_databricks_feed.bronze_raw_landing")
)

logger.info("Streaming ingestion started (availableNow) ...")
query.awaitTermination()
logger.info("Streaming ingestion complete")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Export to JSON for Downstream Notebooks

# COMMAND ----------

# Read what was written in this run and save to JSON.
# Notebooks 02–06 still read from bronze_news_raw.json, so we keep that file
# up-to-date as a snapshot that accumulates new records across runs.

run_df = (
    spark.table("news_pipeline.daily_databricks_feed.bronze_raw_landing")
         .filter(F.col("_run_id") == _run_id)
)

new_records_count = run_df.count()
logger.info("Records written this run: %d", new_records_count)

bronze_records = run_df.toPandas().to_dict(orient="records")

bronze_file = Path(DATA_PATH) / "bronze_news_raw.json"

existing_records: list = []
if bronze_file.exists():
    try:
        with open(bronze_file, "r") as f:
            existing_records = json.load(f)
    except Exception as exc:
        logger.warning("Could not load existing JSON: %s", exc)

existing_ids = {r["id"] for r in existing_records}
new_for_json = [r for r in bronze_records if r["id"] not in existing_ids]
all_records  = existing_records + new_for_json

with open(bronze_file, "w") as f:
    json.dump(all_records, f, indent=2, default=str)

logger.info(
    "JSON snapshot: %d new / %d total → %s",
    len(new_for_json),
    len(all_records),
    bronze_file,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

source_counts: dict = {}
for r in bronze_records:
    src = r.get("source", "unknown")
    source_counts[src] = source_counts.get(src, 0) + 1

print("\n" + "=" * 60)
print("BRONZE INGESTION COMPLETE")
print("=" * 60)
print(f"Run ID:          {_run_id}")
print(f"Date:            {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")
print(f"New records:     {new_records_count}")
print(f"Total in JSON:   {len(all_records)}")
print(f"Active streams:  {[name for name, _ in streams]}")
print("\nSource breakdown:")
for source, count in sorted(source_counts.items(), key=lambda x: x[1], reverse=True):
    print(f"  {source}: {count}")
print("=" * 60)

dbutils.notebook.exit(json.dumps({
    "run_id":        _run_id,
    "new_records":   new_records_count,
    "total_records": len(all_records),
    "sources":       source_counts,
})) if "dbutils" in dir() else None
