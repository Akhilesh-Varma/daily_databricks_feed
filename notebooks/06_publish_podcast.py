# Databricks notebook source
# MAGIC %md
# MAGIC # Publish Podcast - Upload Audio and Update RSS Feed
# MAGIC
# MAGIC This notebook publishes the podcast:
# MAGIC - Uploads audio to Google Cloud Storage
# MAGIC - Generates RSS 2.0 feed with iTunes extensions
# MAGIC - Updates feed.xml for podcast apps

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

project_root = Path(os.getcwd()).parent

# COMMAND ----------

# Load environment variables
from daily_databricks_feed.utils.secrets import load_dotenv, SecretsManager

env_file = project_root / ".env"
if env_file.exists():
    load_dotenv(str(env_file))

_WIDGET_KEYS = [
    "GCP_SERVICE_ACCOUNT_JSON",
    "GROQ_API_KEY",
    "CLAUDE_API_KEY",
    "GOOGLE_API_KEY",
    "REDDIT_CLIENT_ID",
    "REDDIT_CLIENT_SECRET",
    "YOUTUBE_API_KEY",
    "GCS_BUCKET_NAME",
    "AUDIO_BASE_URL",
]
for _k in _WIDGET_KEYS:
    try:
        _v = dbutils.widgets.getArgument(_k, "")
        if _v:
            os.environ[_k] = _v
    except Exception:
        pass
secrets = SecretsManager()

# COMMAND ----------

# Configuration
try:
    DATA_PATH = dbutils.widgets.get("DATA_PATH")
except Exception:
    DATA_PATH = os.environ.get("DATA_PATH", "/Volumes/news_pipeline/default/podcast_data")
GCS_BUCKET = secrets.get("gcs_bucket_name", "")
AUDIO_BASE_URL = secrets.get("audio_base_url", "")
PODCAST_AUTHOR = os.environ.get("PODCAST_AUTHOR", "Daily Databricks Team")
PODCAST_EMAIL = os.environ.get("PODCAST_EMAIL", "podcast@example.com")
DRY_RUN = os.environ.get("DRY_RUN", "false").lower() == "true"

logger.info(f"Data path: {DATA_PATH}")
logger.info(f"GCS bucket: {GCS_BUCKET or 'Not configured'}")
logger.info(f"Audio base URL: {AUDIO_BASE_URL or 'Not configured'}")
logger.info(f"Dry run: {DRY_RUN}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Episode Data

# COMMAND ----------

# Find the latest audio metadata
date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
metadata_file = Path(DATA_PATH) / f"audio_metadata_{date_str}.json"

# If today's metadata doesn't exist, find the most recent one
if not metadata_file.exists():
    metadata_files = list(Path(DATA_PATH).glob("audio_metadata_*.json"))
    if metadata_files:
        metadata_file = max(metadata_files, key=lambda p: p.name)
        date_str = metadata_file.stem.replace("audio_metadata_", "")
    else:
        raise FileNotFoundError("No audio metadata files found")

logger.info(f"Loading metadata from: {metadata_file}")

with open(metadata_file, "r") as f:
    episode_metadata = json.load(f)

logger.info(f"Loaded episode: {episode_metadata.get('title')}")

# COMMAND ----------

# Load script for description
script_file = Path(DATA_PATH) / f"script_{date_str}.json"

episode_description = "Today's episode of Daily Databricks Digest."
if script_file.exists():
    with open(script_file, "r") as f:
        script_data = json.load(f)
    # Create description from intro and story titles
    intro = script_data.get("intro", "")[:200]
    stories = script_data.get("stories", [])
    story_titles = [s.get("title", "")[:50] for s in stories[:5]]

    episode_description = f"{intro}\n\nIn this episode:\n"
    for i, title in enumerate(story_titles, 1):
        episode_description += f"- {title}\n"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Initialize Publisher

# COMMAND ----------

from daily_databricks_feed.podcast.rss_publisher import (
    RSSPublisher,
    PodcastFeed,
    PodcastEpisode,
    create_default_feed,
)

# Initialize publisher
publisher = RSSPublisher(
    bucket_name=GCS_BUCKET,
    credentials_json=secrets.get("gcp_service_account"),
    base_url=AUDIO_BASE_URL,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Upload Audio (if GCS configured)

# COMMAND ----------

audio_file = Path(episode_metadata.get("filepath", ""))
audio_url = ""

if not audio_file.exists():
    # Try to find it in data path
    audio_file = Path(DATA_PATH) / episode_metadata.get("filename", "")

if audio_file.exists():
    logger.info(f"Audio file: {audio_file}")
    logger.info(f"File size: {audio_file.stat().st_size / 1024:.1f} KB")

    if GCS_BUCKET and not DRY_RUN:
        try:
            with open(audio_file, "rb") as f:
                audio_data = f.read()

            audio_url = publisher.upload_audio(
                audio_data=audio_data,
                filename=episode_metadata.get("filename"),
            )
            logger.info(f"Uploaded audio to: {audio_url}")

        except Exception as e:
            logger.error(f"Error uploading audio: {e}")
            # Fall back to local URL for testing
            audio_url = f"file://{audio_file}"
    else:
        # Use local file path or configured base URL
        if AUDIO_BASE_URL:
            audio_url = f"{AUDIO_BASE_URL}/{episode_metadata.get('filename')}"
        else:
            audio_url = f"file://{audio_file}"
        logger.info(f"Audio URL (local/configured): {audio_url}")
else:
    logger.error(f"Audio file not found: {audio_file}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Episode

# COMMAND ----------

# Create episode object
episode_date = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)

episode = PodcastEpisode(
    guid=f"daily-databricks-{date_str}",
    title=episode_metadata.get("title", f"Daily Databricks Digest - {date_str}"),
    description=episode_description,
    audio_url=audio_url,
    audio_size_bytes=episode_metadata.get("file_size_bytes", 0),
    duration_seconds=episode_metadata.get("duration_seconds", 0),
    published_at=episode_date,
    episode_number=0,  # Will be set based on episode count
    keywords=["databricks", "data engineering", "spark", "delta lake", "lakehouse"],
)

logger.info(f"Created episode: {episode.title}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load or Create Feed

# COMMAND ----------

# Load existing feed or create new one
feed_file = Path(DATA_PATH) / "podcast_feed.json"

if feed_file.exists():
    with open(feed_file, "r") as f:
        feed_data = json.load(f)

    feed = PodcastFeed(
        title=feed_data.get("title", "Daily Databricks Digest"),
        description=feed_data.get("description", ""),
        author=feed_data.get("author", PODCAST_AUTHOR),
        email=feed_data.get("email", PODCAST_EMAIL),
        website_url=feed_data.get("website_url", AUDIO_BASE_URL),
        feed_url=feed_data.get("feed_url", f"{AUDIO_BASE_URL}/feed.xml"),
        image_url=feed_data.get("image_url", f"{AUDIO_BASE_URL}/cover.jpg"),
        language=feed_data.get("language", "en-us"),
        category=feed_data.get("category", "Technology"),
        subcategory=feed_data.get("subcategory", "Tech News"),
        episodes=[],
    )

    # Load existing episodes
    for ep_data in feed_data.get("episodes", []):
        try:
            pub_date = datetime.fromisoformat(ep_data["published_at"].replace("Z", "+00:00"))
            ep = PodcastEpisode(
                guid=ep_data["guid"],
                title=ep_data["title"],
                description=ep_data["description"],
                audio_url=ep_data["audio_url"],
                audio_size_bytes=ep_data["audio_size_bytes"],
                duration_seconds=ep_data["duration_seconds"],
                published_at=pub_date,
                episode_number=ep_data.get("episode_number", 0),
                keywords=ep_data.get("keywords", []),
            )
            feed.episodes.append(ep)
        except Exception as e:
            logger.warning(f"Error loading episode: {e}")

    logger.info(f"Loaded feed with {len(feed.episodes)} existing episodes")

else:
    # Create new feed
    feed = create_default_feed(
        base_url=AUDIO_BASE_URL or "https://example.com/podcast",
        author=PODCAST_AUTHOR,
        email=PODCAST_EMAIL,
    )
    logger.info("Created new feed")

# COMMAND ----------

# Add new episode (avoid duplicates)
existing_guids = {ep.guid for ep in feed.episodes}

if episode.guid not in existing_guids:
    # Set episode number
    episode.episode_number = len(feed.episodes) + 1
    feed.episodes.insert(0, episode)  # Add to front
    logger.info(f"Added episode #{episode.episode_number}")
else:
    # Update existing episode
    for i, ep in enumerate(feed.episodes):
        if ep.guid == episode.guid:
            episode.episode_number = ep.episode_number
            feed.episodes[i] = episode
            logger.info(f"Updated existing episode #{episode.episode_number}")
            break

# Keep only last 100 episodes
feed.episodes = feed.episodes[:100]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate RSS Feed

# COMMAND ----------

# Generate RSS XML
rss_xml = publisher.generate_rss(feed)

logger.info(f"Generated RSS feed ({len(rss_xml)} chars)")

# Preview
print("\nRSS Feed Preview (first 1000 chars):")
print("-" * 60)
print(rss_xml[:1000])
print("...")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save and Upload Feed

# COMMAND ----------

# Save feed XML locally
rss_file = Path(DATA_PATH) / "feed.xml"
publisher.save_feed_locally(rss_xml, str(rss_file))
logger.info(f"Saved RSS feed to: {rss_file}")

# Save feed data as JSON for persistence
feed_data = {
    "title": feed.title,
    "description": feed.description,
    "author": feed.author,
    "email": feed.email,
    "website_url": feed.website_url,
    "feed_url": feed.feed_url,
    "image_url": feed.image_url,
    "language": feed.language,
    "category": feed.category,
    "subcategory": feed.subcategory,
    "episodes": [ep.to_dict() for ep in feed.episodes],
}

with open(feed_file, "w") as f:
    json.dump(feed_data, f, indent=2, default=str)

logger.info(f"Saved feed data to: {feed_file}")

# COMMAND ----------

# Upload to GCS if configured
feed_url = ""

if GCS_BUCKET and not DRY_RUN:
    try:
        feed_url = publisher.upload_feed(rss_xml, "feed.xml")
        logger.info(f"Uploaded RSS feed to: {feed_url}")
    except Exception as e:
        logger.error(f"Error uploading feed: {e}")
        feed_url = str(rss_file)
else:
    feed_url = str(rss_file)
    logger.info(f"Feed saved locally: {feed_url}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Delta Table (Feed History)

# COMMAND ----------

import uuid
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()


def _get_run_id():
    try:
        return (
            dbutils.notebook.entry_point.getDbutils()
            .notebook()
            .getContext()
            .tags()
            .apply("jobRunId")
        )
    except Exception:
        return f"local-{uuid.uuid4().hex[:8]}"


_run_id = _get_run_id()

spark.sql("CREATE SCHEMA IF NOT EXISTS news_pipeline.daily_databricks_feed")

feed_row = {
    "episode_date": date_str,
    "episode_guid": episode.guid,
    "episode_title": episode.title,
    "episode_number": episode.episode_number,
    "audio_url": audio_url,
    "duration_seconds": episode.duration_seconds,
    "feed_url": feed_url,
    "total_episodes_in_feed": len(feed.episodes),
    "feed_xml_length": len(rss_xml),
    "gcs_uploaded": bool(GCS_BUCKET and not DRY_RUN),
    "_run_id": _run_id,
    "_pipeline_run_at": datetime.now(timezone.utc).isoformat(),
}

df_feed = spark.createDataFrame([feed_row])
(
    df_feed.write.format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .saveAsTable("news_pipeline.daily_databricks_feed.podcast_feed_history")
)
logger.info(f"Wrote feed history to Delta podcast_feed_history (run_id={_run_id})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

# Print summary
print("\n" + "=" * 60)
print("PODCAST PUBLISH COMPLETE")
print("=" * 60)
print(f"Date: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")
print(f"Episode: {episode.title}")
print(f"Episode #: {episode.episode_number}")
print(f"Duration: {episode.duration_seconds // 60}:{episode.duration_seconds % 60:02d}")
print(f"Total episodes: {len(feed.episodes)}")
print(f"\nURLs:")
print(f"  Audio: {audio_url}")
print(f"  Feed: {feed_url}")
print(f"\nLocal files:")
print(f"  - {rss_file}")
print(f"  - {feed_file}")
if GCS_BUCKET:
    print(f"\nGCS bucket: {GCS_BUCKET}")
print("=" * 60)

# Instructions for podcast submission
print("\n" + "=" * 60)
print("NEXT STEPS FOR DISTRIBUTION")
print("=" * 60)
print(
    """
To distribute your podcast:

1. **Spotify for Podcasters** (https://podcasters.spotify.com)
   - Sign up and submit your RSS feed URL
   - Automatically distributes to Spotify

2. **Apple Podcasts Connect** (https://podcastsconnect.apple.com)
   - Sign in with Apple ID
   - Submit RSS feed for review

3. **Google Podcasts**
   - Submit via Google Podcasts Manager
   - Uses same RSS feed

4. **Other platforms**
   - Most platforms accept standard RSS feeds
   - Submit to: Overcast, Pocket Casts, etc.

Your RSS feed URL: {feed_url}
""".format(
        feed_url=(
            feed_url if feed_url.startswith("http") else "Configure AUDIO_BASE_URL for public URL"
        )
    )
)
print("=" * 60)

# Return results for workflow
(
    dbutils.notebook.exit(
        json.dumps(
            {
                "date": date_str,
                "episode_number": episode.episode_number,
                "total_episodes": len(feed.episodes),
                "audio_url": audio_url,
                "feed_url": feed_url,
            }
        )
    )
    if "dbutils" in dir()
    else None
)
