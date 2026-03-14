"""
PySpark Data Source for Hacker News using the Python Data Source API.

Supports both batch and streaming reads from the Hacker News Algolia API.

Usage:
    # Register the data source
    spark.dataSource.register(HackerNewsDataSource)

    # Batch read
    df = spark.read.format("hackernews").option("query", "databricks").load()

    # Streaming read
    df = spark.readStream.format("hackernews").option("query", "databricks").load()
"""

from pyspark.sql.datasource import (
    DataSource,
    DataSourceReader,
    SimpleDataSourceStreamReader,
    InputPartition,
)
from pyspark.sql.types import StructType
from typing import Iterator, Tuple, Dict, Any, List
import json


class HackerNewsDataSource(DataSource):
    """
    PySpark Data Source for Hacker News.

    Options:
        - query: Search query (default: "databricks")
        - days_back: Number of days to look back (default: 1)
        - min_points: Minimum points threshold (default: 5)
        - limit: Maximum items to fetch (default: 100)
        - filter_databricks: Filter for Databricks content (default: true)
    """

    @classmethod
    def name(cls) -> str:
        return "hackernews"

    def schema(self) -> str:
        return """
            id STRING,
            source STRING,
            title STRING,
            url STRING,
            content STRING,
            author STRING,
            published_at TIMESTAMP,
            fetched_at TIMESTAMP,
            score INT,
            comments_count INT,
            keywords ARRAY<STRING>,
            metadata STRING
        """

    def reader(self, schema: StructType) -> "HackerNewsReader":
        return HackerNewsReader(self.options)

    def simpleStreamReader(self, schema: StructType) -> "HackerNewsStreamReader":
        return HackerNewsStreamReader(self.options)


class HackerNewsReader(DataSourceReader):
    """Batch reader for Hacker News."""

    def __init__(self, options: Dict[str, str]):
        self.options = options

    def read(self, partition: InputPartition) -> Iterator[Tuple]:
        # Import inside method for pickle serialization
        import requests
        from datetime import datetime, timezone, timedelta

        from daily_databricks_feed.data_sources.pyspark_sources.base_source import (
            is_databricks_related,
            extract_keywords,
        )

        query = self.options.get("query", "databricks")
        days_back = int(self.options.get("days_back", "1"))
        min_points = int(self.options.get("min_points", "5"))
        limit = int(self.options.get("limit", "100"))
        filter_databricks = self.options.get("filter_databricks", "true").lower() == "true"

        # Calculate cutoff timestamp
        cutoff_time = datetime.now(timezone.utc) - timedelta(days=days_back)
        cutoff_timestamp = int(cutoff_time.timestamp())

        # Search queries
        search_queries = (
            [query]
            if query != "databricks"
            else ["databricks", "delta lake", "apache spark", "lakehouse", "mlflow"]
        )

        seen_ids = set()
        fetched_at = datetime.now(timezone.utc)

        for search_query in search_queries:
            try:
                params = {
                    "query": search_query,
                    "tags": "story",
                    "numericFilters": f"created_at_i>{cutoff_timestamp},points>{min_points}",
                    "hitsPerPage": min(limit // len(search_queries), 100),
                }

                response = requests.get(
                    "https://hn.algolia.com/api/v1/search",
                    params=params,
                    timeout=30,
                )
                response.raise_for_status()
                data = response.json()

                for hit in data.get("hits", []):
                    object_id = hit.get("objectID", "")
                    item_id = f"hn_{object_id}"

                    if item_id in seen_ids:
                        continue
                    seen_ids.add(item_id)

                    title = hit.get("title", "")
                    story_text = hit.get("story_text", "")
                    searchable = f"{title} {story_text}"

                    # Apply Databricks filter
                    if filter_databricks and not is_databricks_related(searchable):
                        continue

                    # Parse timestamp
                    published_at = None
                    if hit.get("created_at_i"):
                        published_at = datetime.fromtimestamp(hit["created_at_i"], tz=timezone.utc)

                    url = hit.get("url") or f"https://news.ycombinator.com/item?id={object_id}"

                    yield (
                        item_id,
                        "hacker_news",
                        title,
                        url,
                        story_text,
                        hit.get("author"),
                        published_at,
                        fetched_at,
                        hit.get("points", 0),
                        hit.get("num_comments", 0),
                        extract_keywords(searchable),
                        json.dumps(
                            {
                                "hn_id": object_id,
                                "hn_url": f"https://news.ycombinator.com/item?id={object_id}",
                            }
                        ),
                    )

                    if len(seen_ids) >= limit:
                        return

            except Exception:
                continue


class HackerNewsStreamReader(SimpleDataSourceStreamReader):
    """
    Streaming reader for Hacker News.

    Uses offset-based streaming where offset tracks the last seen timestamp.
    Each microbatch fetches new stories since the last offset.
    """

    def __init__(self, options: Dict[str, str]):
        self.options = options
        self._last_timestamp = None

    def initialOffset(self) -> Dict[str, Any]:
        """Return initial offset (current time minus lookback window)."""
        from datetime import datetime, timezone, timedelta

        days_back = int(self.options.get("days_back", "1"))
        initial_time = datetime.now(timezone.utc) - timedelta(days=days_back)

        return {"timestamp": int(initial_time.timestamp())}

    def read(self, start: Dict[str, Any]) -> Tuple[Iterator[Tuple], Dict[str, Any]]:
        """
        Read new data since start offset.

        Returns:
            Tuple of (data iterator, next offset)
        """
        # Import inside method for pickle serialization
        import requests
        from datetime import datetime, timezone

        from daily_databricks_feed.data_sources.pyspark_sources.base_source import (
            is_databricks_related,
            extract_keywords,
        )

        query = self.options.get("query", "databricks")
        min_points = int(self.options.get("min_points", "5"))
        limit = int(self.options.get("limit", "50"))
        filter_databricks = self.options.get("filter_databricks", "true").lower() == "true"

        start_timestamp = start.get("timestamp", 0)
        fetched_at = datetime.now(timezone.utc)
        max_timestamp = start_timestamp

        results = []

        search_queries = (
            [query] if query != "databricks" else ["databricks", "delta lake", "apache spark"]
        )

        seen_ids = set()

        for search_query in search_queries:
            try:
                params = {
                    "query": search_query,
                    "tags": "story",
                    "numericFilters": f"created_at_i>{start_timestamp},points>{min_points}",
                    "hitsPerPage": min(limit, 50),
                }

                response = requests.get(
                    "https://hn.algolia.com/api/v1/search_by_date",
                    params=params,
                    timeout=30,
                )
                response.raise_for_status()
                data = response.json()

                for hit in data.get("hits", []):
                    object_id = hit.get("objectID", "")
                    item_id = f"hn_{object_id}"

                    if item_id in seen_ids:
                        continue
                    seen_ids.add(item_id)

                    title = hit.get("title", "")
                    story_text = hit.get("story_text", "")
                    searchable = f"{title} {story_text}"

                    if filter_databricks and not is_databricks_related(searchable):
                        continue

                    # Track max timestamp for next offset
                    created_at_i = hit.get("created_at_i", 0)
                    if created_at_i > max_timestamp:
                        max_timestamp = created_at_i

                    published_at = None
                    if created_at_i:
                        published_at = datetime.fromtimestamp(created_at_i, tz=timezone.utc)

                    url = hit.get("url") or f"https://news.ycombinator.com/item?id={object_id}"

                    results.append(
                        (
                            item_id,
                            "hacker_news",
                            title,
                            url,
                            story_text,
                            hit.get("author"),
                            published_at,
                            fetched_at,
                            hit.get("points", 0),
                            hit.get("num_comments", 0),
                            extract_keywords(searchable),
                            json.dumps(
                                {
                                    "hn_id": object_id,
                                    "hn_url": f"https://news.ycombinator.com/item?id={object_id}",
                                }
                            ),
                        )
                    )

            except Exception:
                continue

        # Next offset is the max timestamp seen (or current time if no results)
        if max_timestamp == start_timestamp:
            max_timestamp = int(fetched_at.timestamp())

        next_offset = {"timestamp": max_timestamp}

        return (iter(results), next_offset)

    def readBetweenOffsets(self, start: Dict[str, Any], end: Dict[str, Any]) -> Iterator[Tuple]:
        """
        Deterministically read between offsets (for replay after failure).
        """
        # Import inside method
        import requests
        from datetime import datetime, timezone

        from daily_databricks_feed.data_sources.pyspark_sources.base_source import (
            is_databricks_related,
            extract_keywords,
        )

        query = self.options.get("query", "databricks")
        min_points = int(self.options.get("min_points", "5"))
        filter_databricks = self.options.get("filter_databricks", "true").lower() == "true"

        start_timestamp = start.get("timestamp", 0)
        end_timestamp = end.get("timestamp", int(datetime.now(timezone.utc).timestamp()))
        fetched_at = datetime.now(timezone.utc)

        search_queries = [query] if query != "databricks" else ["databricks", "delta lake"]
        seen_ids = set()

        for search_query in search_queries:
            try:
                params = {
                    "query": search_query,
                    "tags": "story",
                    "numericFilters": f"created_at_i>{start_timestamp},created_at_i<={end_timestamp},points>{min_points}",
                    "hitsPerPage": 100,
                }

                response = requests.get(
                    "https://hn.algolia.com/api/v1/search_by_date",
                    params=params,
                    timeout=30,
                )
                response.raise_for_status()
                data = response.json()

                for hit in data.get("hits", []):
                    object_id = hit.get("objectID", "")
                    item_id = f"hn_{object_id}"

                    if item_id in seen_ids:
                        continue
                    seen_ids.add(item_id)

                    title = hit.get("title", "")
                    story_text = hit.get("story_text", "")
                    searchable = f"{title} {story_text}"

                    if filter_databricks and not is_databricks_related(searchable):
                        continue

                    published_at = None
                    created_at_i = hit.get("created_at_i", 0)
                    if created_at_i:
                        published_at = datetime.fromtimestamp(created_at_i, tz=timezone.utc)

                    url = hit.get("url") or f"https://news.ycombinator.com/item?id={object_id}"

                    yield (
                        item_id,
                        "hacker_news",
                        title,
                        url,
                        story_text,
                        hit.get("author"),
                        published_at,
                        fetched_at,
                        hit.get("points", 0),
                        hit.get("num_comments", 0),
                        extract_keywords(searchable),
                        json.dumps(
                            {
                                "hn_id": object_id,
                                "hn_url": f"https://news.ycombinator.com/item?id={object_id}",
                            }
                        ),
                    )

            except Exception:
                continue

    def commit(self, end: Dict[str, Any]) -> None:
        """Commit offset (no cleanup needed for this source)."""
        pass
