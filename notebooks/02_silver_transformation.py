# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Transformation - Clean, Dedupe, Extract Keywords
# MAGIC
# MAGIC This notebook transforms bronze data to silver:
# MAGIC - Clean text (remove HTML, special chars)
# MAGIC - Normalize URLs
# MAGIC - Deduplicate content
# MAGIC - Extract keywords and entities
# MAGIC - Calculate quality scores

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

# Configuration
DATA_PATH = os.environ.get("DATA_PATH", str(project_root / "data"))

logger.info(f"Data path: {DATA_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Bronze Data

# COMMAND ----------

bronze_file = Path(DATA_PATH) / "bronze_news_raw.json"

if not bronze_file.exists():
    logger.error(f"Bronze file not found: {bronze_file}")
    raise FileNotFoundError(f"Bronze file not found: {bronze_file}")

with open(bronze_file, "r") as f:
    bronze_records = json.load(f)

logger.info(f"Loaded {len(bronze_records)} records from bronze layer")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transform to Silver

# COMMAND ----------

from daily_databricks_feed.data_sources.base import NewsItem
from daily_databricks_feed.transformations.bronze_to_silver import BronzeToSilverTransformer

# Convert bronze records back to NewsItem objects
news_items = []
for record in bronze_records:
    try:
        # Parse datetime fields
        published_at = None
        if record.get("published_at"):
            try:
                published_at = datetime.fromisoformat(record["published_at"].replace("Z", "+00:00"))
            except:
                pass

        fetched_at = datetime.now(timezone.utc)
        if record.get("fetched_at"):
            try:
                fetched_at = datetime.fromisoformat(record["fetched_at"].replace("Z", "+00:00"))
            except:
                pass

        item = NewsItem(
            id=record["id"],
            source=record["source"],
            title=record["title"],
            url=record["url"],
            content=record.get("content"),
            author=record.get("author"),
            published_at=published_at,
            fetched_at=fetched_at,
            score=record.get("score", 0),
            comments_count=record.get("comments_count", 0),
            tags=record.get("tags", []),
            metadata=record.get("metadata", {}),
        )
        news_items.append(item)
    except Exception as e:
        logger.warning(f"Error parsing record {record.get('id')}: {e}")

logger.info(f"Parsed {len(news_items)} news items")

# COMMAND ----------

# Apply transformations
transformer = BronzeToSilverTransformer()

silver_items = transformer.transform(news_items)

logger.info(f"Transformed {len(silver_items)} items to silver layer")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Analyze Transformations

# COMMAND ----------

# Count duplicates
duplicates = [item for item in silver_items if item.is_duplicate]
non_duplicates = [item for item in silver_items if not item.is_duplicate]

logger.info(f"Found {len(duplicates)} duplicate items")
logger.info(f"Unique items: {len(non_duplicates)}")

# COMMAND ----------

# Quality score distribution
quality_scores = [item.quality_score for item in silver_items]

if quality_scores:
    avg_quality = sum(quality_scores) / len(quality_scores)
    min_quality = min(quality_scores)
    max_quality = max(quality_scores)

    logger.info(f"Quality score distribution:")
    logger.info(f"  - Min: {min_quality:.3f}")
    logger.info(f"  - Max: {max_quality:.3f}")
    logger.info(f"  - Avg: {avg_quality:.3f}")

# COMMAND ----------

# Source distribution
source_counts = {}
for item in silver_items:
    source_counts[item.source] = source_counts.get(item.source, 0) + 1

logger.info("Source distribution:")
for source, count in sorted(source_counts.items(), key=lambda x: x[1], reverse=True):
    logger.info(f"  - {source}: {count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Filter and Rank

# COMMAND ----------

# Remove duplicates
clean_items = transformer.deduplicate(silver_items)
logger.info(f"After deduplication: {len(clean_items)} items")

# Filter by quality
quality_items = transformer.filter_by_quality(clean_items, min_quality=0.2)
logger.info(f"After quality filter: {len(quality_items)} items")

# Rank by quality
ranked_items = transformer.rank_items(quality_items)
logger.info(f"Ranked {len(ranked_items)} items")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save to Silver Layer

# COMMAND ----------

# Convert to dictionaries
silver_records = [item.to_dict() for item in ranked_items]

# Add transformation metadata
transform_time = datetime.now(timezone.utc).isoformat()
for record in silver_records:
    record["_transformed_at"] = transform_time
    record["_transform_date"] = datetime.now(timezone.utc).strftime("%Y-%m-%d")

# COMMAND ----------

# Save silver data
silver_file = Path(DATA_PATH) / "silver_news_cleaned.json"

with open(silver_file, "w") as f:
    json.dump(silver_records, f, indent=2, default=str)

logger.info(f"Saved {len(silver_records)} records to silver layer")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preview Top Items

# COMMAND ----------

# Show top 5 items
print("\nTop 5 Items by Quality Score:")
print("-" * 80)

for i, item in enumerate(ranked_items[:5], 1):
    print(f"\n{i}. [{item.source}] {item.title_cleaned[:60]}...")
    print(f"   Quality: {item.quality_score:.3f} | Score: {item.score} | Keywords: {', '.join(item.keywords[:3])}")
    print(f"   URL: {item.url[:60]}...")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

# Print summary
print("\n" + "=" * 60)
print("SILVER TRANSFORMATION COMPLETE")
print("=" * 60)
print(f"Date: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")
print(f"Input records: {len(bronze_records)}")
print(f"Output records: {len(silver_records)}")
print(f"Duplicates removed: {len(duplicates)}")
print(f"Output file: {silver_file}")
print("=" * 60)

# Return results for workflow
dbutils.notebook.exit(json.dumps({
    "input_records": len(bronze_records),
    "output_records": len(silver_records),
    "duplicates_removed": len(duplicates),
})) if "dbutils" in dir() else None
