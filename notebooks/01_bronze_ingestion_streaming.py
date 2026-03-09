# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Ingestion (Streaming) - PySpark Data Source API
# MAGIC
# MAGIC This notebook uses the PySpark Python Data Source Streaming API to fetch news
# MAGIC from multiple sources using Spark Structured Streaming.
# MAGIC
# MAGIC **Requirements:**
# MAGIC - PySpark 4.0+ (or Databricks Runtime 15.0+)
# MAGIC - Required API credentials in environment variables
# MAGIC
# MAGIC **Data Sources:**
# MAGIC - Hacker News (Algolia API) - No auth required
# MAGIC - Reddit (PRAW) - Requires REDDIT_CLIENT_ID, REDDIT_CLIENT_SECRET
# MAGIC - YouTube (Data API v3) - Requires YOUTUBE_API_KEY
# MAGIC - RSS Feeds (feedparser) - No auth required

# COMMAND ----------

# MAGIC %pip install requests praw feedparser pyspark-data-sources

# COMMAND ----------

import os
import sys
from pathlib import Path

# Add src to path
project_root = Path(os.getcwd()).parent
if str(project_root / "src") not in sys.path:
    sys.path.insert(0, str(project_root / "src"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Load environment variables
from daily_databricks_feed.utils.secrets import load_dotenv, SecretsManager

env_file = project_root / ".env"
if env_file.exists():
    load_dotenv(str(env_file))

secrets = SecretsManager()
secrets.print_status()

# COMMAND ----------

# Configuration
DATA_PATH = os.environ.get("DATA_PATH", str(project_root / "data"))
CHECKPOINT_PATH = os.environ.get("CHECKPOINT_PATH", str(project_root / "checkpoints"))

# Create directories
Path(DATA_PATH).mkdir(parents=True, exist_ok=True)
Path(CHECKPOINT_PATH).mkdir(parents=True, exist_ok=True)

print(f"Data path: {DATA_PATH}")
print(f"Checkpoint path: {CHECKPOINT_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Register PySpark Data Sources

# COMMAND ----------

from pyspark.sql import SparkSession

# Get or create Spark session
spark = SparkSession.builder \
    .appName("DailyDatabricksFeed-BronzeIngestion") \
    .config("spark.sql.streaming.schemaInference", "true") \
    .getOrCreate()

print(f"Spark version: {spark.version}")

# COMMAND ----------

# Register custom data sources
from daily_databricks_feed.data_sources.pyspark_sources import (
    HackerNewsDataSource,
    RedditDataSource,
    YouTubeDataSource,
    RSSFeedDataSource,
)

spark.dataSource.register(HackerNewsDataSource)
spark.dataSource.register(RSSFeedDataSource)

# Register sources that require credentials only if available
if secrets.get("reddit_client_id"):
    spark.dataSource.register(RedditDataSource)
    print("Registered: RedditDataSource")

if secrets.get("youtube_api_key"):
    spark.dataSource.register(YouTubeDataSource)
    print("Registered: YouTubeDataSource")

print("Registered: HackerNewsDataSource")
print("Registered: RSSFeedDataSource")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Option 1: Batch Read (One-time fetch)
# MAGIC
# MAGIC Use this for a single batch fetch of current news.

# COMMAND ----------

# Batch read from Hacker News
df_hn = spark.read.format("hackernews") \
    .option("query", "databricks") \
    .option("days_back", "1") \
    .option("min_points", "5") \
    .option("limit", "50") \
    .load()

print(f"Hacker News items: {df_hn.count()}")
df_hn.select("id", "title", "score", "source").show(5, truncate=50)

# COMMAND ----------

# Batch read from RSS feeds
df_rss = spark.read.format("rss") \
    .option("days_back", "7") \
    .option("limit", "50") \
    .load()

print(f"RSS Feed items: {df_rss.count()}")
df_rss.select("id", "title", "source", "published_at").show(5, truncate=50)

# COMMAND ----------

# Batch read from YouTube (if API key available)
if secrets.get("youtube_api_key"):
    df_yt = spark.read.format("youtube") \
        .option("query", "databricks tutorial") \
        .option("days_back", "7") \
        .option("limit", "30") \
        .load()

    print(f"YouTube items: {df_yt.count()}")
    df_yt.select("id", "title", "author", "score").show(5, truncate=50)

# COMMAND ----------

# Batch read from Reddit (if credentials available)
if secrets.get("reddit_client_id"):
    df_reddit = spark.read.format("reddit") \
        .option("subreddits", "databricks,dataengineering") \
        .option("days_back", "1") \
        .option("min_score", "5") \
        .option("limit", "50") \
        .load()

    print(f"Reddit items: {df_reddit.count()}")
    df_reddit.select("id", "title", "score", "source").show(5, truncate=50)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Option 2: Streaming Read (Continuous ingestion)
# MAGIC
# MAGIC Use this for continuous streaming ingestion with checkpointing.

# COMMAND ----------

# Streaming read from Hacker News
stream_hn = spark.readStream.format("hackernews") \
    .option("query", "databricks") \
    .option("min_points", "3") \
    .option("limit", "25") \
    .load()

# Write to Delta table with checkpointing
query_hn = stream_hn.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", f"{CHECKPOINT_PATH}/hackernews") \
    .option("path", f"{DATA_PATH}/bronze_hackernews") \
    .trigger(availableNow=True) \
    .start()

# Wait for completion (availableNow processes one batch then stops)
query_hn.awaitTermination()

print("Hacker News streaming batch complete")

# COMMAND ----------

# Streaming read from RSS feeds
stream_rss = spark.readStream.format("rss") \
    .option("days_back", "3") \
    .option("limit", "30") \
    .load()

query_rss = stream_rss.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", f"{CHECKPOINT_PATH}/rss") \
    .option("path", f"{DATA_PATH}/bronze_rss") \
    .trigger(availableNow=True) \
    .start()

query_rss.awaitTermination()

print("RSS streaming batch complete")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Combine All Sources into Bronze Table

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# Read all bronze tables
bronze_dfs = []

# Hacker News
if Path(f"{DATA_PATH}/bronze_hackernews").exists():
    df = spark.read.format("delta").load(f"{DATA_PATH}/bronze_hackernews")
    bronze_dfs.append(df)
    print(f"Loaded {df.count()} rows from bronze_hackernews")

# RSS
if Path(f"{DATA_PATH}/bronze_rss").exists():
    df = spark.read.format("delta").load(f"{DATA_PATH}/bronze_rss")
    bronze_dfs.append(df)
    print(f"Loaded {df.count()} rows from bronze_rss")

# Combine all sources
if bronze_dfs:
    from functools import reduce
    bronze_combined = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), bronze_dfs)

    # Add ingestion metadata
    bronze_combined = bronze_combined \
        .withColumn("_ingested_at", current_timestamp()) \
        .withColumn("_ingestion_date", lit(str(datetime.now().date())))

    # Write combined bronze table
    bronze_combined.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save(f"{DATA_PATH}/bronze_news_combined")

    print(f"\nTotal bronze records: {bronze_combined.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preview Bronze Data

# COMMAND ----------

from datetime import datetime

# Read combined bronze table
if Path(f"{DATA_PATH}/bronze_news_combined").exists():
    bronze_df = spark.read.format("delta").load(f"{DATA_PATH}/bronze_news_combined")

    print(f"Total records: {bronze_df.count()}")
    print("\nSource distribution:")
    bronze_df.groupBy("source").count().show()

    print("\nSample records:")
    bronze_df.select("id", "source", "title", "score", "published_at") \
        .orderBy("published_at", ascending=False) \
        .show(10, truncate=50)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Export to JSON (for non-Spark workflows)

# COMMAND ----------

import json

# Export to JSON for compatibility with non-Spark pipeline steps
if Path(f"{DATA_PATH}/bronze_news_combined").exists():
    bronze_df = spark.read.format("delta").load(f"{DATA_PATH}/bronze_news_combined")

    # Convert to Python dicts
    records = bronze_df.toPandas().to_dict(orient="records")

    # Handle datetime serialization
    for record in records:
        for key, value in record.items():
            if hasattr(value, 'isoformat'):
                record[key] = value.isoformat()

    # Save to JSON
    json_file = Path(DATA_PATH) / "bronze_news_raw.json"
    with open(json_file, "w") as f:
        json.dump(records, f, indent=2, default=str)

    print(f"Exported {len(records)} records to {json_file}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("\n" + "=" * 60)
print("BRONZE INGESTION (STREAMING) COMPLETE")
print("=" * 60)
print(f"Data path: {DATA_PATH}")
print(f"Checkpoint path: {CHECKPOINT_PATH}")
if Path(f"{DATA_PATH}/bronze_news_combined").exists():
    bronze_df = spark.read.format("delta").load(f"{DATA_PATH}/bronze_news_combined")
    print(f"Total records: {bronze_df.count()}")
print("=" * 60)
