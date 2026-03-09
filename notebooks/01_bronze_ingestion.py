# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Ingestion - Fetch News from All Sources
# MAGIC
# MAGIC This notebook fetches news from multiple sources:
# MAGIC - Hacker News (Algolia API)
# MAGIC - Reddit (PRAW)
# MAGIC - YouTube (Data API v3)
# MAGIC - RSS Feeds (feedparser)
# MAGIC
# MAGIC Data is saved to the bronze layer as raw records.

# COMMAND ----------

# MAGIC %pip install requests praw feedparser python-dotenv

# COMMAND ----------

import os
import sys
import json
import logging
from datetime import datetime, timezone
from pathlib import Path

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Add src to path for local development
project_root = Path(os.getcwd()).parent
if str(project_root / "src") not in sys.path:
    sys.path.insert(0, str(project_root / "src"))

# COMMAND ----------

# Load environment variables from .env file if running locally
from daily_databricks_feed.utils.secrets import load_dotenv, SecretsManager

env_file = project_root / ".env"
if env_file.exists():
    load_dotenv(str(env_file))
    logger.info("Loaded environment from .env file")

# Initialize secrets manager
secrets = SecretsManager()
secrets.print_status()

# COMMAND ----------

# Configuration
DATA_PATH = os.environ.get("DATA_PATH", str(project_root / "data"))
DAYS_BACK = int(os.environ.get("DAYS_BACK", "1"))

logger.info(f"Data path: {DATA_PATH}")
logger.info(f"Fetching news from the last {DAYS_BACK} days")

# Create data directory if it doesn't exist
Path(DATA_PATH).mkdir(parents=True, exist_ok=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fetch from Hacker News

# COMMAND ----------

from daily_databricks_feed.data_sources.hacker_news import HackerNewsSource

hn_source = HackerNewsSource()

try:
    hn_items = hn_source.fetch_with_retry(
        days_back=DAYS_BACK,
        min_points=5,
        limit=50,
        filter_databricks=True,
    )
    logger.info(f"Fetched {len(hn_items)} items from Hacker News")
except Exception as e:
    logger.error(f"Error fetching from Hacker News: {e}")
    hn_items = []

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fetch from Reddit

# COMMAND ----------

from daily_databricks_feed.data_sources.reddit import RedditSource

reddit_source = RedditSource()

if reddit_source.is_available():
    try:
        reddit_items = reddit_source.fetch_with_retry(
            days_back=DAYS_BACK,
            min_score=3,
            limit=50,
            filter_databricks=True,
        )
        logger.info(f"Fetched {len(reddit_items)} items from Reddit")
    except Exception as e:
        logger.error(f"Error fetching from Reddit: {e}")
        reddit_items = []
else:
    logger.warning("Reddit API not configured, skipping")
    reddit_items = []

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fetch from YouTube

# COMMAND ----------

from daily_databricks_feed.data_sources.youtube import YouTubeSource

youtube_source = YouTubeSource()

if youtube_source.is_available():
    try:
        youtube_items = youtube_source.fetch_with_retry(
            days_back=max(DAYS_BACK, 7),  # YouTube needs longer lookback
            limit=30,
            filter_databricks=True,
        )
        logger.info(f"Fetched {len(youtube_items)} items from YouTube")
    except Exception as e:
        logger.error(f"Error fetching from YouTube: {e}")
        youtube_items = []
else:
    logger.warning("YouTube API not configured, skipping")
    youtube_items = []

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fetch from RSS Feeds

# COMMAND ----------

from daily_databricks_feed.data_sources.rss_feeds import RSSFeedSource

rss_source = RSSFeedSource()

if rss_source.is_available():
    try:
        rss_items = rss_source.fetch_with_retry(
            days_back=max(DAYS_BACK, 7),
            limit=50,
            filter_databricks=True,
        )
        logger.info(f"Fetched {len(rss_items)} items from RSS feeds")
    except Exception as e:
        logger.error(f"Error fetching from RSS feeds: {e}")
        rss_items = []
else:
    logger.warning("RSS parsing not available, skipping")
    rss_items = []

# COMMAND ----------

# MAGIC %md
# MAGIC ## Combine and Save to Bronze Layer

# COMMAND ----------

# Combine all items
all_items = hn_items + reddit_items + youtube_items + rss_items

logger.info(f"Total items fetched: {len(all_items)}")
logger.info(f"  - Hacker News: {len(hn_items)}")
logger.info(f"  - Reddit: {len(reddit_items)}")
logger.info(f"  - YouTube: {len(youtube_items)}")
logger.info(f"  - RSS Feeds: {len(rss_items)}")

# COMMAND ----------

# Convert to dictionaries for storage
bronze_records = [item.to_dict() for item in all_items]

# Add ingestion metadata
ingestion_time = datetime.now(timezone.utc).isoformat()
for record in bronze_records:
    record["_ingested_at"] = ingestion_time
    record["_ingestion_date"] = datetime.now(timezone.utc).strftime("%Y-%m-%d")

# COMMAND ----------

# Save to JSON for simplicity (can use Delta Lake with Spark if available)
bronze_file = Path(DATA_PATH) / "bronze_news_raw.json"

# Load existing data if any
existing_records = []
if bronze_file.exists():
    try:
        with open(bronze_file, "r") as f:
            existing_records = json.load(f)
    except Exception as e:
        logger.warning(f"Error loading existing bronze data: {e}")

# Deduplicate by ID
existing_ids = {r["id"] for r in existing_records}
new_records = [r for r in bronze_records if r["id"] not in existing_ids]

# Combine and save
all_records = existing_records + new_records

with open(bronze_file, "w") as f:
    json.dump(all_records, f, indent=2, default=str)

logger.info(f"Saved {len(new_records)} new records to bronze layer")
logger.info(f"Total records in bronze: {len(all_records)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

# Print summary
print("\n" + "=" * 60)
print("BRONZE INGESTION COMPLETE")
print("=" * 60)
print(f"Date: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")
print(f"New records: {len(new_records)}")
print(f"Total records: {len(all_records)}")
print(f"Output file: {bronze_file}")
print("=" * 60)

# Return count for workflow
dbutils.notebook.exit(json.dumps({
    "new_records": len(new_records),
    "total_records": len(all_records),
    "sources": {
        "hacker_news": len(hn_items),
        "reddit": len(reddit_items),
        "youtube": len(youtube_items),
        "rss_feeds": len(rss_items),
    }
})) if "dbutils" in dir() else None
