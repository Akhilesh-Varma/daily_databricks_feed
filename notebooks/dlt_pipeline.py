# Databricks notebook source
# MAGIC %md
# MAGIC # DLT Pipeline: Bronze → Silver → Gold
# MAGIC
# MAGIC Spark Declarative Pipeline that reads new records from `bronze_raw_landing`
# MAGIC (populated by notebook 01) and produces versioned Silver and Gold tables.
# MAGIC Every run appends — nothing is ever overwritten.
# MAGIC
# MAGIC **Tables created:**
# MAGIC - `bronze_news`        – streaming table, raw records from landing table
# MAGIC - `silver_news`        – streaming table, cleaned + keyword-enriched
# MAGIC - `gold_top_stories`   – materialized view, top-10 per (_run_id, _ingestion_date)
# MAGIC - `gold_daily_summary` – materialized view, one row per (_run_id, _ingestion_date)

# COMMAND ----------

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType, DoubleType
from pyspark.sql.window import Window

# ── Keyword list for enrichment ────────────────────────────────────────────────
DATABRICKS_KEYWORDS = [
    "databricks", "delta lake", "delta", "apache spark", "spark", "lakehouse",
    "mlflow", "unity catalog", "delta sharing", "databricks sql", "pyspark",
    "structured streaming", "autoloader", "photon", "mosaic", "dolly", "dbrx",
    "data engineering", "feature store", "vector search", "genie", "lakeflow",
]

BRONZE_LANDING_TABLE = "news_pipeline.daily_databricks_feed.bronze_raw_landing"

# ── UDFs ───────────────────────────────────────────────────────────────────────

@F.udf(returnType=ArrayType(StringType()))
def extract_keywords(title, content):
    """Return Databricks keywords found in title + content."""
    text = f"{title or ''} {content or ''}".lower()
    return [kw for kw in DATABRICKS_KEYWORDS if kw in text]


@F.udf(returnType=DoubleType())
def quality_score_udf(score, comments_count, content, n_keywords):
    """Compute a 0–1 quality score from available signals."""
    try:
        s    = int(score or 0)
        c    = int(comments_count or 0)
        clen = len(content or "")
        nk   = int(n_keywords or 0)

        title_pts   = 0.2
        content_pts = 0.3 if clen > 150 else (0.15 if clen > 30 else 0.0)
        social_pts  = 0.2 if s > 10    else (0.1  if s  > 0  else 0.0)
        comment_pts = 0.1 if c > 5     else (0.05 if c  > 0  else 0.0)
        keyword_pts = 0.2 if nk > 2    else (0.1  if nk > 0  else 0.0)

        return min(1.0, title_pts + content_pts + social_pts + comment_pts + keyword_pts)
    except Exception:
        return 0.1


# ── BRONZE: stream from landing table ─────────────────────────────────────────

@dp.table(
    name="bronze_news",
    comment=(
        "Raw news streamed from bronze_raw_landing. "
        "One row per article per pipeline run, tagged with _run_id."
    ),
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true",
    },
)
def bronze_news():
    return (
        spark.readStream
             .format("delta")
             .table(BRONZE_LANDING_TABLE)
    )


# ── SILVER: clean + enrich ────────────────────────────────────────────────────

@dp.table(
    name="silver_news",
    comment=(
        "Cleaned, keyword-enriched articles. "
        "Streaming — each run appends its batch, full history retained."
    ),
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.managed": "true",
    },
)
@dp.expect_or_drop("has_title", "title IS NOT NULL AND length(title) > 5")
@dp.expect_or_drop("has_url",   "url IS NOT NULL AND length(url) > 10")
@dp.expect_or_drop("quality_above_floor", "quality_score >= 0.1")
def silver_news():
    def clean(col):
        return F.trim(
            F.regexp_replace(
                F.regexp_replace(col, r"<[^>]+>", " "),
                r"\s+", " ",
            )
        )

    kw_col = extract_keywords(F.col("title"), F.col("content"))

    return (
        spark.readStream.table("bronze_news")
        .withColumn("title_cleaned",   clean(F.col("title")))
        .withColumn("content_cleaned", clean(F.coalesce(F.col("content"), F.lit(""))))
        .withColumn("keywords", kw_col)
        .withColumn(
            "quality_score",
            quality_score_udf(
                F.col("score").cast("long"),
                F.col("comments_count").cast("long"),
                F.col("content"),
                F.size(kw_col),
            ),
        )
        .withColumn("_ingestion_date",  F.to_date(F.col("_pipeline_run_at")))
        .withColumn("_transformed_at", F.current_timestamp())
    )


# ── GOLD: top stories per run + date ─────────────────────────────────────────

@dp.materialized_view(
    name="gold_top_stories",
    comment=(
        "Top 10 quality stories per (_run_id, _ingestion_date). "
        "Query the latest _run_id per date to get current episode selection. "
        "All historical runs are preserved."
    ),
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true",
    },
)
def gold_top_stories():
    w = Window.partitionBy("_run_id", "_ingestion_date").orderBy(
        F.desc("quality_score"), F.desc("score")
    )
    return (
        spark.read.table("silver_news")
        .filter(F.col("quality_score") >= 0.2)
        .withColumn("rank", F.row_number().over(w))
        .filter(F.col("rank") <= 10)
        .withColumn("_aggregated_at", F.current_timestamp())
        .select(
            "_run_id", "_ingestion_date", "_pipeline_run_at",
            "rank", "id", "source",
            "title_cleaned", "content_cleaned", "url",
            "score", "comments_count", "quality_score", "keywords",
            "_aggregated_at",
        )
    )


@dp.materialized_view(
    name="gold_daily_summary",
    comment=(
        "One row per (_run_id, _ingestion_date) with story counts and source breakdown. "
        "Each pipeline run appends a new row — historical runs are never overwritten. "
        "Join with gold_top_stories on _run_id to get full story details."
    ),
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true",
    },
)
def gold_daily_summary():
    top = spark.read.table("gold_top_stories")

    # Per (run, date, source) counts
    src_counts = (
        top.groupBy("_run_id", "_ingestion_date", "source")
           .agg(F.count("*").alias("cnt"))
    )

    # Source distribution as JSON map
    src_dist = (
        src_counts
        .groupBy("_run_id", "_ingestion_date")
        .agg(
            F.to_json(
                F.map_from_entries(
                    F.collect_list(
                        F.struct(F.col("source").alias("key"), F.col("cnt").alias("value"))
                    )
                )
            ).alias("source_distribution")
        )
    )

    summary = (
        top.groupBy("_run_id", "_ingestion_date", "_pipeline_run_at")
           .agg(
               F.count("id").alias("story_count"),
               F.countDistinct("source").alias("unique_source_count"),
               F.max("quality_score").alias("max_quality_score"),
               F.avg("quality_score").alias("avg_quality_score"),
               F.current_timestamp().alias("_aggregated_at"),
           )
    )

    return summary.join(src_dist, ["_run_id", "_ingestion_date"])
