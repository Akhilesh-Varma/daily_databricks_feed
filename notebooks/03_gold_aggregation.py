# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Aggregation - Select Top Stories for Podcast
# MAGIC
# MAGIC This notebook selects the top 10 stories for today's podcast:
# MAGIC - Prioritize by quality score
# MAGIC - Ensure source diversity
# MAGIC - Create daily summary

# COMMAND ----------

import os
import json
import logging
from datetime import datetime, timezone
from pathlib import Path

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

project_root = Path(os.getcwd()).parent

# COMMAND ----------

# Configuration
try:
    DATA_PATH = dbutils.widgets.get("DATA_PATH")
except Exception:
    DATA_PATH = os.environ.get("DATA_PATH", "/Volumes/news_pipeline/default/podcast_data")
try:
    MAX_STORIES = int(dbutils.widgets.get("MAX_STORIES"))
except Exception:
    MAX_STORIES = int(os.environ.get("MAX_STORIES", "10"))
DIVERSITY_WEIGHT = float(os.environ.get("DIVERSITY_WEIGHT", "0.3"))

logger.info(f"Data path: {DATA_PATH}")
logger.info(f"Max stories: {MAX_STORIES}")
logger.info(f"Diversity weight: {DIVERSITY_WEIGHT}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Silver Data

# COMMAND ----------

silver_file = Path(DATA_PATH) / "silver_news_cleaned.json"

if not silver_file.exists():
    logger.error(f"Silver file not found: {silver_file}")
    raise FileNotFoundError(f"Silver file not found: {silver_file}")

with open(silver_file, "r") as f:
    silver_records = json.load(f)

logger.info(f"Loaded {len(silver_records)} records from silver layer")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Select Top Stories with Diversity

# COMMAND ----------

from daily_databricks_feed.aggregation.script_generator import select_top_stories

# Select top stories
top_stories = select_top_stories(
    items=silver_records,
    max_stories=MAX_STORIES,
    diversity_weight=DIVERSITY_WEIGHT,
)

logger.info(f"Selected {len(top_stories)} top stories")

# COMMAND ----------

# Analyze selected stories
print("\nSelected Stories:")
print("-" * 80)

source_counts = {}
for i, story in enumerate(top_stories, 1):
    source = story.get("source", "unknown")
    source_counts[source] = source_counts.get(source, 0) + 1

    print(f"\n{i}. [{source}] {story.get('title_cleaned', story.get('title', 'Untitled'))[:60]}...")
    print(f"   Quality: {story.get('quality_score', 0):.3f}")
    print(f"   Score: {story.get('score', 0)} | Comments: {story.get('comments_count', 0)}")

print("\n\nSource Distribution in Selection:")
for source, count in sorted(source_counts.items(), key=lambda x: x[1], reverse=True):
    print(f"  - {source}: {count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Daily Summary

# COMMAND ----------

today = datetime.now(timezone.utc)
date_str = today.strftime("%Y-%m-%d")

# Create summary record
daily_summary = {
    "date": date_str,
    "created_at": today.isoformat(),
    "story_count": len(top_stories),
    "sources": source_counts,
    "stories": top_stories,
    "metadata": {
        "max_stories": MAX_STORIES,
        "diversity_weight": DIVERSITY_WEIGHT,
        "total_candidates": len(silver_records),
    },
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save Gold Layer

# COMMAND ----------

# Save daily summary
gold_summary_file = Path(DATA_PATH) / "gold_daily_summary.json"

# Load existing summaries
existing_summaries = []
if gold_summary_file.exists():
    try:
        with open(gold_summary_file, "r") as f:
            existing_summaries = json.load(f)
    except:
        existing_summaries = []

# Check if today's summary exists
existing_dates = {s.get("date") for s in existing_summaries}
if date_str in existing_dates:
    # Update existing
    existing_summaries = [s for s in existing_summaries if s.get("date") != date_str]

existing_summaries.append(daily_summary)

# Sort by date descending
existing_summaries.sort(key=lambda x: x.get("date", ""), reverse=True)

# Keep only last 30 days
existing_summaries = existing_summaries[:30]

with open(gold_summary_file, "w") as f:
    json.dump(existing_summaries, f, indent=2, default=str)

logger.info(f"Saved daily summary to {gold_summary_file}")

# COMMAND ----------

# Also save today's stories separately for easy access
gold_stories_file = Path(DATA_PATH) / f"gold_stories_{date_str}.json"

with open(gold_stories_file, "w") as f:
    json.dump(top_stories, f, indent=2, default=str)

logger.info(f"Saved today's stories to {gold_stories_file}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Delta Tables (Gold)

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
_pipeline_run_at = datetime.now(timezone.utc).isoformat()

spark.sql("CREATE SCHEMA IF NOT EXISTS news_pipeline.daily_databricks_feed")

# Write top stories
if top_stories:
    import pandas as pd

    stories_rows = []
    for rank_idx, story in enumerate(top_stories, 1):
        stories_rows.append(
            {
                "episode_date": date_str,
                "rank": rank_idx,
                "id": str(story.get("id", "")),
                "source": str(story.get("source", "")),
                "title": str(story.get("title_cleaned") or story.get("title", "")),
                "content": story.get("content_cleaned") or story.get("content"),
                "url": str(story.get("url", "")),
                "score": int(story.get("score", 0) or 0),
                "comments_count": int(story.get("comments_count", 0) or 0),
                "quality_score": float(story.get("quality_score", 0.0) or 0.0),
                "keywords": json.dumps(story.get("keywords", [])),
                "_run_id": _run_id,
                "_pipeline_run_at": _pipeline_run_at,
                "_aggregated_at": datetime.now(timezone.utc).isoformat(),
            }
        )

    df_stories = spark.createDataFrame(pd.DataFrame(stories_rows))
    (
        df_stories.write.format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable("news_pipeline.daily_databricks_feed.gold_top_stories_nb")
    )
    logger.info(
        f"Wrote {len(stories_rows)} top stories to Delta gold_top_stories_nb (run_id={_run_id})"
    )

# Write daily summary
summary_row = {
    "episode_date": date_str,
    "story_count": len(top_stories),
    "unique_source_count": len(source_counts),
    "source_distribution": json.dumps(source_counts),
    "total_candidates": len(silver_records),
    "max_stories_param": MAX_STORIES,
    "diversity_weight": DIVERSITY_WEIGHT,
    "_run_id": _run_id,
    "_pipeline_run_at": _pipeline_run_at,
    "_aggregated_at": datetime.now(timezone.utc).isoformat(),
}

df_summary = spark.createDataFrame([summary_row])
(
    df_summary.write.format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .saveAsTable("news_pipeline.daily_databricks_feed.gold_daily_summary_nb")
)
logger.info(f"Wrote daily summary to Delta gold_daily_summary_nb (run_id={_run_id})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Story Summaries for Script

# COMMAND ----------

# Prepare stories for script generation
stories_for_script = []

for story in top_stories:
    # Get the best available content
    content = (
        story.get("content_cleaned")
        or story.get("content")
        or story.get("title_cleaned")
        or story.get("title", "")
    )

    # Truncate long content
    if len(content) > 500:
        content = content[:500] + "..."

    stories_for_script.append(
        {
            "title": story.get("title_cleaned") or story.get("title", "Untitled"),
            "content": content,
            "source": story.get("source", "unknown"),
            "url": story.get("url", ""),
            "score": story.get("score", 0),
            "quality_score": story.get("quality_score", 0),
            "keywords": story.get("keywords", []),
        }
    )

# Save stories for script generation
script_input_file = Path(DATA_PATH) / "script_input.json"

with open(script_input_file, "w") as f:
    json.dump(
        {
            "date": date_str,
            "stories": stories_for_script,
        },
        f,
        indent=2,
    )

logger.info(f"Saved script input to {script_input_file}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

# Print summary
print("\n" + "=" * 60)
print("GOLD AGGREGATION COMPLETE")
print("=" * 60)
print(f"Date: {today.strftime('%Y-%m-%d %H:%M:%S UTC')}")
print(f"Stories selected: {len(top_stories)}")
print(f"Total candidates: {len(silver_records)}")
print(f"Sources represented: {len(source_counts)}")
print(f"\nOutput files:")
print(f"  - {gold_summary_file}")
print(f"  - {gold_stories_file}")
print(f"  - {script_input_file}")
print("=" * 60)

# Return results for workflow
(
    dbutils.notebook.exit(
        json.dumps(
            {
                "date": date_str,
                "stories_selected": len(top_stories),
                "total_candidates": len(silver_records),
                "sources": source_counts,
            }
        )
    )
    if "dbutils" in dir()
    else None
)
