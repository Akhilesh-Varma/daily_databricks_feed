# Databricks notebook source
# MAGIC %md
# MAGIC # Script Generation - Generate Podcast Script using LLM
# MAGIC
# MAGIC This notebook generates the podcast script:
# MAGIC - Uses Claude (Anthropic) as primary LLM — key read from CLAUDE_API_KEY env var
# MAGIC - Falls back to Gemini, then template-based generation
# MAGIC - Creates intro, stories, and outro segments

# COMMAND ----------

# MAGIC %pip install requests

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

# Load environment variables
from daily_databricks_feed.utils.secrets import load_dotenv, SecretsManager

env_file = project_root / ".env"
if env_file.exists():
    load_dotenv(str(env_file))

secrets = SecretsManager()

# COMMAND ----------

# Configuration
DATA_PATH = os.environ.get("DATA_PATH", str(project_root / "data"))
PODCAST_NAME = os.environ.get("PODCAST_NAME", "Daily Databricks Digest")

logger.info(f"Data path: {DATA_PATH}")
logger.info(f"Podcast name: {PODCAST_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Script Input

# COMMAND ----------

script_input_file = Path(DATA_PATH) / "script_input.json"

if not script_input_file.exists():
    logger.error(f"Script input file not found: {script_input_file}")
    raise FileNotFoundError(f"Script input file not found: {script_input_file}")

with open(script_input_file, "r") as f:
    script_input = json.load(f)

stories = script_input.get("stories", [])
date_str = script_input.get("date", datetime.now().strftime("%Y-%m-%d"))

logger.info(f"Loaded {len(stories)} stories for {date_str}")

# COMMAND ----------

# Preview stories
print("\nStories for Script Generation:")
print("-" * 80)

for i, story in enumerate(stories[:7], 1):
    print(f"\n{i}. [{story.get('source')}] {story.get('title')[:60]}...")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Initialize Script Generator

# COMMAND ----------

from daily_databricks_feed.aggregation.script_generator import ScriptGenerator

# Initialize — Claude key is read from CLAUDE_API_KEY env var (injected by Databricks job)
generator = ScriptGenerator(
    anthropic_api_key=secrets.get("claude_api_key"),
    google_api_key=secrets.get("google_api_key"),
)

# Check which provider will be used
provider = generator.get_available_provider()
logger.info(f"Using LLM provider: {type(provider).__name__}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Script

# COMMAND ----------

# Generate the podcast script
episode_date = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)

script = generator.generate_script(
    stories=stories,
    date=episode_date,
    podcast_name=PODCAST_NAME,
)

logger.info(f"Generated script with {script.word_count} words")
logger.info(f"Estimated duration: {script.estimated_duration_seconds // 60}:{script.estimated_duration_seconds % 60:02d}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preview Script

# COMMAND ----------

print("\n" + "=" * 80)
print("GENERATED PODCAST SCRIPT")
print("=" * 80)
print(f"\nTitle: {script.title}")
print(f"Date: {script.episode_date}")
print(f"Word Count: {script.word_count}")
print(f"Est. Duration: {script.estimated_duration_seconds // 60}:{script.estimated_duration_seconds % 60:02d}")
print("\n" + "-" * 80)

# Show intro
print("\n[INTRO]")
print(script.intro[:500] + "..." if len(script.intro) > 500 else script.intro)

# Show first 2 stories
print("\n[STORIES]")
for i, story in enumerate(script.stories[:2], 1):
    print(f"\n--- Story {i}: {story.get('title', 'Untitled')[:50]}... ---")
    content = story.get('content', '')[:300]
    print(content + "..." if len(story.get('content', '')) > 300 else content)

if len(script.stories) > 2:
    print(f"\n... and {len(script.stories) - 2} more stories ...")

# Show outro
print("\n[OUTRO]")
print(script.outro)

print("\n" + "=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate SSML

# COMMAND ----------

# Generate SSML for TTS
ssml_script = generator.generate_ssml(script)

logger.info(f"Generated SSML with {len(ssml_script)} characters")

# Preview SSML
print("\nSSML Preview (first 500 chars):")
print("-" * 40)
print(ssml_script[:500] + "...")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save Script

# COMMAND ----------

# Save script as JSON
script_output_file = Path(DATA_PATH) / f"script_{date_str}.json"

with open(script_output_file, "w") as f:
    json.dump(script.to_dict(), f, indent=2)

logger.info(f"Saved script to {script_output_file}")

# COMMAND ----------

# Save SSML
ssml_output_file = Path(DATA_PATH) / f"script_{date_str}.ssml"

with open(ssml_output_file, "w") as f:
    f.write(ssml_script)

logger.info(f"Saved SSML to {ssml_output_file}")

# COMMAND ----------

# Save to gold layer for tracking
gold_scripts_file = Path(DATA_PATH) / "gold_podcast_scripts.json"

# Load existing scripts
existing_scripts = []
if gold_scripts_file.exists():
    try:
        with open(gold_scripts_file, "r") as f:
            existing_scripts = json.load(f)
    except:
        existing_scripts = []

# Add new script (update if exists for today)
existing_dates = {s.get("episode_date") for s in existing_scripts}
if date_str in existing_dates:
    existing_scripts = [s for s in existing_scripts if s.get("episode_date") != date_str]

existing_scripts.append(script.to_dict())

# Sort by date descending and keep last 30
existing_scripts.sort(key=lambda x: x.get("episode_date", ""), reverse=True)
existing_scripts = existing_scripts[:30]

with open(gold_scripts_file, "w") as f:
    json.dump(existing_scripts, f, indent=2)

logger.info(f"Updated gold scripts at {gold_scripts_file}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Delta Table (Scripts)

# COMMAND ----------

import uuid
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

def _get_run_id():
    try:
        return dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply("jobRunId")
    except Exception:
        return f"local-{uuid.uuid4().hex[:8]}"

_run_id = _get_run_id()

spark.sql("CREATE SCHEMA IF NOT EXISTS news_pipeline.daily_databricks_feed")

script_dict = script.to_dict()

script_row = {
    "episode_date":        date_str,
    "title":               script_dict.get("title", ""),
    "word_count":          int(script_dict.get("word_count", 0) or 0),
    "duration_seconds":    int(script_dict.get("estimated_duration_seconds", 0) or 0),
    "intro":               script_dict.get("intro", ""),
    "outro":               script_dict.get("outro", ""),
    "stories_json":        json.dumps(script_dict.get("stories", [])),
    "story_count":         len(script_dict.get("stories", [])),
    "ssml_length":         len(ssml_script),
    "provider":            script_dict.get("metadata", {}).get("provider", "unknown"),
    "_run_id":             _run_id,
    "_pipeline_run_at":    datetime.now(timezone.utc).isoformat(),
}

df_script = spark.createDataFrame([script_row])
(
    df_script.write
             .format("delta")
             .mode("append")
             .option("mergeSchema", "true")
             .saveAsTable("news_pipeline.daily_databricks_feed.podcast_scripts")
)
logger.info(f"Wrote script metadata to Delta podcast_scripts (run_id={_run_id})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

# Print summary
print("\n" + "=" * 60)
print("SCRIPT GENERATION COMPLETE")
print("=" * 60)
print(f"Date: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")
print(f"Episode: {script.title}")
print(f"Word count: {script.word_count}")
print(f"Duration: {script.estimated_duration_seconds // 60}:{script.estimated_duration_seconds % 60:02d}")
print(f"Stories: {len(script.stories)}")
print(f"Provider: {script.metadata.get('provider', 'Unknown')}")
print(f"\nOutput files:")
print(f"  - {script_output_file}")
print(f"  - {ssml_output_file}")
print("=" * 60)

# Return results for workflow
dbutils.notebook.exit(json.dumps({
    "date": date_str,
    "word_count": script.word_count,
    "duration_seconds": script.estimated_duration_seconds,
    "story_count": len(script.stories),
    "provider": script.metadata.get("provider", "Unknown"),
})) if "dbutils" in dir() else None
