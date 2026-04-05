# Databricks notebook source
# MAGIC %md
# MAGIC # Audio Generation - Convert Script to Audio using TTS
# MAGIC
# MAGIC This notebook generates podcast audio:
# MAGIC - Uses Google Cloud Text-to-Speech API
# MAGIC - Alternates between male and female voices
# MAGIC - Stitches segments together
# MAGIC - Outputs MP3 file

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
# Source 1: notebook widgets (job base_parameters) — try both APIs
for _k in _WIDGET_KEYS:
    if not os.environ.get(_k):
        try:
            _v = dbutils.widgets.get(_k)
            if _v:
                os.environ[_k] = _v
        except Exception:
            try:
                _v = dbutils.widgets.getArgument(_k, "")
                if _v:
                    os.environ[_k] = _v
            except Exception:
                pass
# Source 2: Databricks Secret Scope (most reliable for large/complex credentials)
for _k in _WIDGET_KEYS:
    if not os.environ.get(_k):
        try:
            _v = dbutils.secrets.get(scope="daily-podcast", key=_k)
            if _v:
                os.environ[_k] = _v
        except Exception:
            pass
secrets = SecretsManager()
secrets.print_status()  # TODO: remove after verifying GCP credentials are loaded

# COMMAND ----------

# Configuration
try:
    DATA_PATH = dbutils.widgets.get("DATA_PATH")
except Exception:
    DATA_PATH = os.environ.get("DATA_PATH", "/Volumes/news_pipeline/default/podcast_data")
USE_MOCK_TTS = os.environ.get("USE_MOCK_TTS", "false").lower() == "true"

logger.info(f"Data path: {DATA_PATH}")
logger.info(f"Use mock TTS: {USE_MOCK_TTS}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Script

# COMMAND ----------

# Find the latest script file
date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
script_file = Path(DATA_PATH) / f"script_{date_str}.json"

# If today's script doesn't exist, find the most recent one
if not script_file.exists():
    script_files = list(Path(DATA_PATH).glob("script_*.json"))
    if script_files:
        script_file = max(script_files, key=lambda p: p.name)
        date_str = script_file.stem.replace("script_", "")
    else:
        raise FileNotFoundError("No script files found")

logger.info(f"Loading script from: {script_file}")

with open(script_file, "r") as f:
    script_data = json.load(f)

logger.info(f"Loaded script: {script_data.get('title')}")
logger.info(f"Word count: {script_data.get('word_count')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Initialize TTS Generator

# COMMAND ----------

from daily_databricks_feed.podcast.tts_generator import TTSGenerator, MockTTSGenerator

# Check if GCP credentials are available
gcp_creds = secrets.get("gcp_service_account")

if USE_MOCK_TTS or not gcp_creds:
    logger.info("Using MockTTSGenerator (no GCP credentials or mock mode enabled)")
    tts = MockTTSGenerator()
else:
    logger.info("Using Google Cloud TTS")
    tts = TTSGenerator(credentials_json=gcp_creds)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Audio

# COMMAND ----------

# Check TTS availability
if not tts.is_available():
    logger.warning("TTS not available - using mock generator")
    tts = MockTTSGenerator()

# Generate podcast audio
logger.info("Starting audio generation...")

try:
    podcast_audio = tts.generate_podcast(script_data)

    logger.info(f"Audio generated successfully!")
    logger.info(
        f"Duration: {podcast_audio.duration_seconds // 60}:{podcast_audio.duration_seconds % 60:02d}"
    )
    logger.info(f"File size: {podcast_audio.file_size_bytes / 1024:.1f} KB")
    logger.info(f"Segments: {len(podcast_audio.segments)}")

except Exception as e:
    logger.error(f"Error generating audio: {e}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preview Audio Segments

# COMMAND ----------

print("\nAudio Segments:")
print("-" * 60)

for seg in podcast_audio.segments:
    duration_sec = seg.get("duration_ms", 0) / 1000
    print(f"  [{seg.get('type'):6s}] {seg.get('voice'):20s} - {duration_sec:.1f}s")

total_duration = podcast_audio.duration_seconds
print("-" * 60)
print(f"Total Duration: {total_duration // 60}:{total_duration % 60:02d}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save Audio File

# COMMAND ----------

# Save audio file
audio_filename = f"daily-databricks-{date_str}.mp3"
audio_file = Path(DATA_PATH) / audio_filename

podcast_audio.save(str(audio_file))

logger.info(f"Saved audio to: {audio_file}")
logger.info(f"File size: {audio_file.stat().st_size / 1024:.1f} KB")

# COMMAND ----------

# Save audio metadata
metadata_file = Path(DATA_PATH) / f"audio_metadata_{date_str}.json"

audio_metadata = {
    "episode_date": podcast_audio.episode_date,
    "title": podcast_audio.title,
    "format": podcast_audio.format,
    "duration_seconds": podcast_audio.duration_seconds,
    "file_size_bytes": podcast_audio.file_size_bytes,
    "filename": audio_filename,
    "filepath": str(audio_file),
    "segments": podcast_audio.segments,
    "metadata": podcast_audio.metadata,
}

with open(metadata_file, "w") as f:
    json.dump(audio_metadata, f, indent=2)

logger.info(f"Saved metadata to: {metadata_file}")

# COMMAND ----------

# Update gold episodes table
gold_episodes_file = Path(DATA_PATH) / "gold_episodes.json"

# Load existing episodes
existing_episodes = []
if gold_episodes_file.exists():
    try:
        with open(gold_episodes_file, "r") as f:
            existing_episodes = json.load(f)
    except:
        existing_episodes = []

# Add new episode (update if exists)
existing_dates = {e.get("episode_date") for e in existing_episodes}
if date_str in existing_dates:
    existing_episodes = [e for e in existing_episodes if e.get("episode_date") != date_str]

existing_episodes.append(audio_metadata)

# Sort by date descending and keep last 30
existing_episodes.sort(key=lambda x: x.get("episode_date", ""), reverse=True)
existing_episodes = existing_episodes[:30]

with open(gold_episodes_file, "w") as f:
    json.dump(existing_episodes, f, indent=2)

logger.info(f"Updated gold episodes at {gold_episodes_file}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Delta Table (Audio Episodes)

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

episode_row = {
    "episode_date": date_str,
    "title": podcast_audio.title,
    "format": podcast_audio.format,
    "duration_seconds": podcast_audio.duration_seconds,
    "file_size_bytes": podcast_audio.file_size_bytes,
    "filename": audio_filename,
    "filepath": str(audio_file),
    "segment_count": len(podcast_audio.segments),
    "segments_json": json.dumps(podcast_audio.segments),
    "_run_id": _run_id,
    "_pipeline_run_at": datetime.now(timezone.utc).isoformat(),
}

df_episode = spark.createDataFrame([episode_row])
(
    df_episode.write.format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .saveAsTable("news_pipeline.daily_databricks_feed.audio_episodes")
)
logger.info(f"Wrote episode metadata to Delta audio_episodes (run_id={_run_id})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

# Print summary
print("\n" + "=" * 60)
print("AUDIO GENERATION COMPLETE")
print("=" * 60)
print(f"Date: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")
print(f"Episode: {podcast_audio.title}")
print(f"Duration: {podcast_audio.duration_seconds // 60}:{podcast_audio.duration_seconds % 60:02d}")
print(f"File size: {podcast_audio.file_size_bytes / 1024:.1f} KB")
print(f"Segments: {len(podcast_audio.segments)}")
print(f"\nOutput files:")
print(f"  - {audio_file}")
print(f"  - {metadata_file}")
print("=" * 60)

# Return results for workflow
(
    dbutils.notebook.exit(
        json.dumps(
            {
                "date": date_str,
                "duration_seconds": podcast_audio.duration_seconds,
                "file_size_bytes": podcast_audio.file_size_bytes,
                "segment_count": len(podcast_audio.segments),
                "audio_file": str(audio_file),
            }
        )
    )
    if "dbutils" in dir()
    else None
)
