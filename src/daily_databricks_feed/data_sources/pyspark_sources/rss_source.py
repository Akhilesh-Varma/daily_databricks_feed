"""
PySpark Data Source for RSS Feeds using the Python Data Source API.

Supports both batch and streaming reads from RSS/Atom feeds.

Usage:
    # Register the data source
    spark.dataSource.register(RSSFeedDataSource)

    # Batch read (default feeds)
    df = spark.read.format("rss").load()

    # Batch read with custom feeds
    df = spark.read.format("rss") \
        .option("feeds", "https://databricks.com/blog/feed,https://delta.io/blog/feed.xml") \
        .load()

    # Streaming read
    df = spark.readStream.format("rss").load()
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
import hashlib


# Default RSS feeds for Databricks content
DEFAULT_FEEDS = [
    "https://www.databricks.com/blog/feed",
    "https://delta.io/blog/feed.xml",
    "https://spark.apache.org/news/feed.xml",
]


class RSSFeedDataSource(DataSource):
    """
    PySpark Data Source for RSS Feeds.

    Options:
        - feeds: Comma-separated feed URLs (default: Databricks-related feeds)
        - days_back: Number of days to look back (default: 7)
        - limit: Maximum items to fetch (default: 100)
        - filter_databricks: Filter for Databricks content (default: true)
    """

    @classmethod
    def name(cls) -> str:
        return "rss"

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

    def reader(self, schema: StructType) -> "RSSFeedReader":
        return RSSFeedReader(self.options)

    def simpleStreamReader(self, schema: StructType) -> "RSSFeedStreamReader":
        return RSSFeedStreamReader(self.options)


class RSSFeedReader(DataSourceReader):
    """Batch reader for RSS feeds."""

    def __init__(self, options: Dict[str, str]):
        self.options = options

    def read(self, partition: InputPartition) -> Iterator[Tuple]:
        import re
        from datetime import datetime, timezone, timedelta
        from time import mktime

        try:
            import feedparser
        except ImportError:
            return

        from daily_databricks_feed.data_sources.pyspark_sources.base_source import (
            is_databricks_related,
            extract_keywords,
        )

        feeds_str = self.options.get("feeds", ",".join(DEFAULT_FEEDS))
        feeds = [f.strip() for f in feeds_str.split(",") if f.strip()]
        days_back = int(self.options.get("days_back", "7"))
        limit = int(self.options.get("limit", "100"))
        filter_databricks = self.options.get("filter_databricks", "true").lower() == "true"

        cutoff_time = datetime.now(timezone.utc) - timedelta(days=days_back)
        fetched_at = datetime.now(timezone.utc)

        seen_urls = set()
        items_yielded = 0

        for feed_url in feeds:
            if items_yielded >= limit:
                break

            try:
                feed = feedparser.parse(feed_url)
                feed_title = feed.feed.get("title", "Unknown Feed")

                # Check if it's an official Databricks feed
                is_official = any(
                    domain in feed_url.lower()
                    for domain in ["databricks.com", "delta.io", "spark.apache.org", "mlflow.org"]
                )

                for entry in feed.entries:
                    if items_yielded >= limit:
                        break

                    # Parse publish time
                    published_at = None
                    time_struct = entry.get("published_parsed") or entry.get("updated_parsed")
                    if time_struct:
                        try:
                            published_at = datetime.fromtimestamp(mktime(time_struct), tz=timezone.utc)
                        except (ValueError, OverflowError):
                            pass

                    # Filter by time
                    if published_at and published_at < cutoff_time:
                        continue

                    # Get URL
                    url = entry.get("link", "")
                    if not url:
                        continue

                    # Deduplicate
                    url_normalized = url.lower().rstrip("/")
                    if url_normalized in seen_urls:
                        continue
                    seen_urls.add(url_normalized)

                    # Generate ID
                    url_hash = hashlib.md5(url.encode()).hexdigest()[:12]
                    item_id = f"rss_{url_hash}"

                    # Get title and content
                    title = entry.get("title", "Untitled")

                    content = None
                    if entry.get("summary"):
                        content = self._clean_html(entry["summary"])
                    elif entry.get("content"):
                        content_list = entry.get("content", [])
                        if content_list and isinstance(content_list, list):
                            content = self._clean_html(content_list[0].get("value", ""))

                    searchable = f"{title} {content or ''}"

                    # Apply filter (official feeds are always relevant)
                    if filter_databricks and not is_official:
                        if not is_databricks_related(searchable):
                            continue

                    # Get author
                    author = entry.get("author")
                    if not author and entry.get("authors"):
                        authors = entry.get("authors", [])
                        if authors and isinstance(authors, list):
                            author = authors[0].get("name")

                    # Get tags from entry
                    entry_tags = []
                    if entry.get("tags"):
                        for tag in entry.get("tags", []):
                            if isinstance(tag, dict) and tag.get("term"):
                                entry_tags.append(tag["term"].lower())

                    all_keywords = list(set(entry_tags + extract_keywords(searchable)))

                    yield (
                        item_id,
                        "rss_feed",
                        title,
                        url,
                        content,
                        author,
                        published_at,
                        fetched_at,
                        0,  # RSS feeds don't have scores
                        0,
                        all_keywords,
                        json.dumps({
                            "feed_name": feed_title,
                            "feed_url": feed_url,
                            "is_official": is_official,
                            "guid": entry.get("id") or entry.get("guid"),
                        }),
                    )
                    items_yielded += 1

            except Exception:
                continue

    def _clean_html(self, html_content: str) -> str:
        """Remove HTML tags from content."""
        import re

        if not html_content:
            return ""

        text = re.sub(r"<[^>]+>", " ", html_content)
        text = re.sub(r"\s+", " ", text)
        text = text.replace("&nbsp;", " ")
        text = text.replace("&amp;", "&")
        text = text.replace("&lt;", "<")
        text = text.replace("&gt;", ">")
        text = text.replace("&quot;", '"')
        text = text.replace("&#39;", "'")

        return text.strip()


class RSSFeedStreamReader(SimpleDataSourceStreamReader):
    """
    Streaming reader for RSS feeds.

    Polls feeds periodically for new content.
    """

    def __init__(self, options: Dict[str, str]):
        self.options = options

    def initialOffset(self) -> Dict[str, Any]:
        """Return initial offset."""
        from datetime import datetime, timezone, timedelta

        days_back = int(self.options.get("days_back", "7"))
        initial_time = datetime.now(timezone.utc) - timedelta(days=days_back)

        return {
            "last_check": initial_time.isoformat(),
            "seen_urls": [],
        }

    def read(self, start: Dict[str, Any]) -> Tuple[Iterator[Tuple], Dict[str, Any]]:
        """Read new entries since start offset."""
        import re
        from datetime import datetime, timezone
        from time import mktime
        import hashlib

        try:
            import feedparser
        except ImportError:
            return (iter([]), start)

        from daily_databricks_feed.data_sources.pyspark_sources.base_source import (
            is_databricks_related,
            extract_keywords,
        )

        feeds_str = self.options.get("feeds", ",".join(DEFAULT_FEEDS))
        feeds = [f.strip() for f in feeds_str.split(",") if f.strip()]
        limit = int(self.options.get("limit", "50"))
        filter_databricks = self.options.get("filter_databricks", "true").lower() == "true"

        last_check_str = start.get("last_check", datetime.now(timezone.utc).isoformat())
        try:
            last_check = datetime.fromisoformat(last_check_str.replace("Z", "+00:00"))
        except:
            last_check = datetime.now(timezone.utc)

        seen_urls = set(start.get("seen_urls", []))
        fetched_at = datetime.now(timezone.utc)

        results = []
        new_seen_urls = list(seen_urls)

        for feed_url in feeds:
            if len(results) >= limit:
                break

            try:
                feed = feedparser.parse(feed_url)
                feed_title = feed.feed.get("title", "Unknown Feed")

                is_official = any(
                    domain in feed_url.lower()
                    for domain in ["databricks.com", "delta.io", "spark.apache.org"]
                )

                for entry in feed.entries:
                    if len(results) >= limit:
                        break

                    url = entry.get("link", "")
                    if not url:
                        continue

                    url_normalized = url.lower().rstrip("/")
                    if url_normalized in seen_urls:
                        continue

                    # Parse publish time
                    published_at = None
                    time_struct = entry.get("published_parsed") or entry.get("updated_parsed")
                    if time_struct:
                        try:
                            published_at = datetime.fromtimestamp(mktime(time_struct), tz=timezone.utc)
                        except:
                            pass

                    # Only get entries newer than last check
                    if published_at and published_at <= last_check:
                        continue

                    url_hash = hashlib.md5(url.encode()).hexdigest()[:12]
                    item_id = f"rss_{url_hash}"

                    title = entry.get("title", "Untitled")

                    content = None
                    if entry.get("summary"):
                        content = self._clean_html(entry["summary"])

                    searchable = f"{title} {content or ''}"

                    if filter_databricks and not is_official:
                        if not is_databricks_related(searchable):
                            continue

                    author = entry.get("author")

                    results.append((
                        item_id,
                        "rss_feed",
                        title,
                        url,
                        content,
                        author,
                        published_at,
                        fetched_at,
                        0,
                        0,
                        extract_keywords(searchable),
                        json.dumps({
                            "feed_name": feed_title,
                            "feed_url": feed_url,
                            "is_official": is_official,
                        }),
                    ))
                    new_seen_urls.append(url_normalized)

            except Exception:
                continue

        # Keep only recent seen URLs
        new_seen_urls = new_seen_urls[-500:]

        next_offset = {
            "last_check": fetched_at.isoformat(),
            "seen_urls": new_seen_urls,
        }

        return (iter(results), next_offset)

    def _clean_html(self, html_content: str) -> str:
        """Remove HTML tags."""
        import re

        if not html_content:
            return ""
        text = re.sub(r"<[^>]+>", " ", html_content)
        text = re.sub(r"\s+", " ", text)
        return text.strip()

    def readBetweenOffsets(
        self, start: Dict[str, Any], end: Dict[str, Any]
    ) -> Iterator[Tuple]:
        """Deterministically read between offsets."""
        # RSS feeds don't support exact time range queries
        return iter([])

    def commit(self, end: Dict[str, Any]) -> None:
        """Commit offset."""
        pass
