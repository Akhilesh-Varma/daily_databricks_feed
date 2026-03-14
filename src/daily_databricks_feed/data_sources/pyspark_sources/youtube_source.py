"""
PySpark Data Source for YouTube using the Python Data Source API.

Supports both batch and streaming reads from YouTube Data API v3.

Usage:
    # Register the data source
    spark.dataSource.register(YouTubeDataSource)

    # Batch read (requires API key)
    df = spark.read.format("youtube") \
        .option("api_key", "...") \
        .option("query", "databricks tutorial") \
        .load()

    # Streaming read
    df = spark.readStream.format("youtube") \
        .option("api_key", "...") \
        .load()
"""

from pyspark.sql.datasource import (
    DataSource,
    DataSourceReader,
    SimpleDataSourceStreamReader,
    InputPartition,
)
from pyspark.sql.types import StructType
from typing import Iterator, Tuple, Dict, Any
import json


class YouTubeDataSource(DataSource):
    """
    PySpark Data Source for YouTube.

    Options:
        - api_key: YouTube Data API key (required)
        - query: Search query (default: "databricks")
        - days_back: Number of days to look back (default: 7)
        - limit: Maximum items to fetch (default: 50)
        - filter_databricks: Filter for Databricks content (default: true)
    """

    @classmethod
    def name(cls) -> str:
        return "youtube"

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

    def reader(self, schema: StructType) -> "YouTubeReader":
        return YouTubeReader(self.options)

    def simpleStreamReader(self, schema: StructType) -> "YouTubeStreamReader":
        return YouTubeStreamReader(self.options)


class YouTubeReader(DataSourceReader):
    """Batch reader for YouTube."""

    def __init__(self, options: Dict[str, str]):
        self.options = options

    def read(self, partition: InputPartition) -> Iterator[Tuple]:
        import os
        import requests
        from datetime import datetime, timezone, timedelta

        from daily_databricks_feed.data_sources.pyspark_sources.base_source import (
            is_databricks_related,
            extract_keywords,
        )

        api_key = self.options.get("api_key") or os.environ.get("YOUTUBE_API_KEY")

        if not api_key:
            return

        query = self.options.get("query", "databricks")
        days_back = int(self.options.get("days_back", "7"))
        limit = int(self.options.get("limit", "50"))
        filter_databricks = self.options.get("filter_databricks", "true").lower() == "true"

        cutoff_time = datetime.now(timezone.utc) - timedelta(days=days_back)
        fetched_at = datetime.now(timezone.utc)

        search_queries = (
            [query]
            if query != "databricks"
            else [
                "databricks tutorial",
                "databricks",
                "delta lake",
                "apache spark tutorial",
            ]
        )

        seen_ids = set()

        for search_query in search_queries:
            if len(seen_ids) >= limit:
                break

            try:
                # Search for videos
                params = {
                    "key": api_key,
                    "part": "snippet",
                    "q": search_query,
                    "type": "video",
                    "order": "relevance",
                    "publishedAfter": cutoff_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "maxResults": min(limit // len(search_queries), 50),
                    "relevanceLanguage": "en",
                }

                response = requests.get(
                    "https://www.googleapis.com/youtube/v3/search",
                    params=params,
                    timeout=30,
                )
                response.raise_for_status()
                search_data = response.json()

                video_ids = [
                    item["id"]["videoId"]
                    for item in search_data.get("items", [])
                    if item.get("id", {}).get("videoId")
                ]

                if not video_ids:
                    continue

                # Get video statistics
                stats_params = {
                    "key": api_key,
                    "part": "statistics",
                    "id": ",".join(video_ids[:50]),
                }

                stats_response = requests.get(
                    "https://www.googleapis.com/youtube/v3/videos",
                    params=stats_params,
                    timeout=30,
                )
                stats_data = stats_response.json() if stats_response.ok else {"items": []}

                stats_map = {}
                for item in stats_data.get("items", []):
                    video_id = item["id"]
                    statistics = item.get("statistics", {})
                    stats_map[video_id] = {
                        "view_count": int(statistics.get("viewCount", 0)),
                        "like_count": int(statistics.get("likeCount", 0)),
                        "comment_count": int(statistics.get("commentCount", 0)),
                    }

                # Process search results
                for item in search_data.get("items", []):
                    video_id = item.get("id", {}).get("videoId")
                    if not video_id:
                        continue

                    item_id = f"youtube_{video_id}"
                    if item_id in seen_ids:
                        continue
                    seen_ids.add(item_id)

                    snippet = item.get("snippet", {})
                    title = snippet.get("title", "")
                    description = snippet.get("description", "")
                    searchable = f"{title} {description}"

                    if filter_databricks and not is_databricks_related(searchable):
                        continue

                    published_at = None
                    if snippet.get("publishedAt"):
                        published_at = datetime.fromisoformat(
                            snippet["publishedAt"].replace("Z", "+00:00")
                        )

                    video_stats = stats_map.get(video_id, {})

                    yield (
                        item_id,
                        "youtube",
                        title,
                        f"https://www.youtube.com/watch?v={video_id}",
                        description,
                        snippet.get("channelTitle"),
                        published_at,
                        fetched_at,
                        video_stats.get("view_count", 0),
                        video_stats.get("comment_count", 0),
                        extract_keywords(searchable),
                        json.dumps(
                            {
                                "video_id": video_id,
                                "channel_id": snippet.get("channelId"),
                                "channel_title": snippet.get("channelTitle"),
                                "thumbnail_url": snippet.get("thumbnails", {})
                                .get("high", {})
                                .get("url"),
                                "like_count": video_stats.get("like_count", 0),
                            }
                        ),
                    )

            except Exception:
                continue


class YouTubeStreamReader(SimpleDataSourceStreamReader):
    """
    Streaming reader for YouTube.

    Uses offset tracking based on publish timestamps.
    """

    def __init__(self, options: Dict[str, str]):
        self.options = options

    def initialOffset(self) -> Dict[str, Any]:
        """Return initial offset."""
        from datetime import datetime, timezone, timedelta

        days_back = int(self.options.get("days_back", "7"))
        initial_time = datetime.now(timezone.utc) - timedelta(days=days_back)

        return {"published_after": initial_time.isoformat()}

    def read(self, start: Dict[str, Any]) -> Tuple[Iterator[Tuple], Dict[str, Any]]:
        """Read new videos since start offset."""
        import os
        import requests
        from datetime import datetime, timezone

        from daily_databricks_feed.data_sources.pyspark_sources.base_source import (
            is_databricks_related,
            extract_keywords,
        )

        api_key = self.options.get("api_key") or os.environ.get("YOUTUBE_API_KEY")

        if not api_key:
            return (iter([]), start)

        query = self.options.get("query", "databricks")
        limit = int(self.options.get("limit", "25"))
        filter_databricks = self.options.get("filter_databricks", "true").lower() == "true"

        published_after = start.get("published_after", datetime.now(timezone.utc).isoformat())
        fetched_at = datetime.now(timezone.utc)
        latest_published = published_after

        results = []
        seen_ids = set()

        search_queries = [query] if query != "databricks" else ["databricks", "delta lake"]

        for search_query in search_queries:
            try:
                params = {
                    "key": api_key,
                    "part": "snippet",
                    "q": search_query,
                    "type": "video",
                    "order": "date",
                    "publishedAfter": (
                        published_after.replace("+00:00", "Z")
                        if "+00:00" in published_after
                        else published_after
                    ),
                    "maxResults": min(limit, 25),
                    "relevanceLanguage": "en",
                }

                response = requests.get(
                    "https://www.googleapis.com/youtube/v3/search",
                    params=params,
                    timeout=30,
                )

                if not response.ok:
                    continue

                search_data = response.json()

                for item in search_data.get("items", []):
                    video_id = item.get("id", {}).get("videoId")
                    if not video_id:
                        continue

                    item_id = f"youtube_{video_id}"
                    if item_id in seen_ids:
                        continue
                    seen_ids.add(item_id)

                    snippet = item.get("snippet", {})
                    title = snippet.get("title", "")
                    description = snippet.get("description", "")
                    searchable = f"{title} {description}"

                    if filter_databricks and not is_databricks_related(searchable):
                        continue

                    published_at = None
                    published_at_str = snippet.get("publishedAt")
                    if published_at_str:
                        published_at = datetime.fromisoformat(
                            published_at_str.replace("Z", "+00:00")
                        )
                        # Track latest for next offset
                        if published_at_str > latest_published:
                            latest_published = published_at_str

                    results.append(
                        (
                            item_id,
                            "youtube",
                            title,
                            f"https://www.youtube.com/watch?v={video_id}",
                            description,
                            snippet.get("channelTitle"),
                            published_at,
                            fetched_at,
                            0,  # Would need separate API call for stats
                            0,
                            extract_keywords(searchable),
                            json.dumps(
                                {
                                    "video_id": video_id,
                                    "channel_id": snippet.get("channelId"),
                                    "channel_title": snippet.get("channelTitle"),
                                }
                            ),
                        )
                    )

            except Exception:
                continue

        next_offset = {"published_after": latest_published}

        return (iter(results), next_offset)

    def readBetweenOffsets(self, start: Dict[str, Any], end: Dict[str, Any]) -> Iterator[Tuple]:
        """Deterministically read between offsets."""
        # YouTube API doesn't support exact time range queries well
        # Return empty for replay
        return iter([])

    def commit(self, end: Dict[str, Any]) -> None:
        """Commit offset."""
        pass
