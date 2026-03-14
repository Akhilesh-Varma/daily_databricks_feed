"""
PySpark Data Source for Reddit using the Python Data Source API.

Supports both batch and streaming reads from Reddit via PRAW.

Usage:
    # Register the data source
    spark.dataSource.register(RedditDataSource)

    # Batch read (requires credentials)
    df = spark.read.format("reddit") \
        .option("client_id", "...") \
        .option("client_secret", "...") \
        .load()

    # Streaming read
    df = spark.readStream.format("reddit") \
        .option("client_id", "...") \
        .option("client_secret", "...") \
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


class RedditDataSource(DataSource):
    """
    PySpark Data Source for Reddit.

    Options:
        - client_id: Reddit OAuth client ID (required)
        - client_secret: Reddit OAuth client secret (required)
        - subreddits: Comma-separated subreddit names (default: databricks,dataengineering)
        - days_back: Number of days to look back (default: 1)
        - min_score: Minimum upvote score (default: 5)
        - limit: Maximum items to fetch (default: 100)
        - filter_databricks: Filter for Databricks content (default: true)
    """

    @classmethod
    def name(cls) -> str:
        return "reddit"

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

    def reader(self, schema: StructType) -> "RedditReader":
        return RedditReader(self.options)

    def simpleStreamReader(self, schema: StructType) -> "RedditStreamReader":
        return RedditStreamReader(self.options)


class RedditReader(DataSourceReader):
    """Batch reader for Reddit."""

    def __init__(self, options: Dict[str, str]):
        self.options = options

    def read(self, partition: InputPartition) -> Iterator[Tuple]:
        # Import inside method for pickle serialization
        import os
        from datetime import datetime, timezone, timedelta

        from daily_databricks_feed.data_sources.pyspark_sources.base_source import (
            is_databricks_related,
            extract_keywords,
        )

        # Get credentials
        client_id = self.options.get("client_id") or os.environ.get("REDDIT_CLIENT_ID")
        client_secret = self.options.get("client_secret") or os.environ.get("REDDIT_CLIENT_SECRET")

        if not client_id or not client_secret:
            # Return empty if no credentials
            return

        try:
            import praw
        except ImportError:
            # PRAW not installed
            return

        subreddits_str = self.options.get("subreddits", "databricks,dataengineering,apachespark")
        subreddits = [s.strip() for s in subreddits_str.split(",")]
        days_back = int(self.options.get("days_back", "1"))
        min_score = int(self.options.get("min_score", "5"))
        limit = int(self.options.get("limit", "100"))
        filter_databricks = self.options.get("filter_databricks", "true").lower() == "true"

        cutoff_time = datetime.now(timezone.utc) - timedelta(days=days_back)
        fetched_at = datetime.now(timezone.utc)

        # Initialize Reddit client
        reddit = praw.Reddit(
            client_id=client_id,
            client_secret=client_secret,
            user_agent="DailyDatabricksFeed/1.0 PySpark DataSource",
        )

        seen_ids = set()
        items_yielded = 0

        for subreddit_name in subreddits:
            if items_yielded >= limit:
                break

            try:
                subreddit = reddit.subreddit(subreddit_name)

                # Fetch from hot and new
                for submission in list(subreddit.hot(limit=limit // len(subreddits))) + list(
                    subreddit.new(limit=limit // len(subreddits))
                ):

                    if items_yielded >= limit:
                        break

                    item_id = f"reddit_{submission.id}"
                    if item_id in seen_ids:
                        continue
                    seen_ids.add(item_id)

                    # Check score
                    if submission.score < min_score:
                        continue

                    # Check time
                    created_at = datetime.fromtimestamp(submission.created_utc, tz=timezone.utc)
                    if created_at < cutoff_time:
                        continue

                    title = submission.title
                    content = submission.selftext if submission.is_self else None
                    searchable = f"{title} {content or ''}"

                    # Apply filter (r/databricks is always relevant)
                    if filter_databricks and subreddit_name.lower() != "databricks":
                        if not is_databricks_related(searchable):
                            continue

                    url = (
                        f"https://reddit.com{submission.permalink}"
                        if submission.is_self
                        else submission.url
                    )

                    author_name = str(submission.author) if submission.author else None

                    yield (
                        item_id,
                        "reddit",
                        title,
                        url,
                        content,
                        author_name,
                        created_at,
                        fetched_at,
                        submission.score,
                        submission.num_comments,
                        extract_keywords(searchable),
                        json.dumps(
                            {
                                "subreddit": subreddit_name,
                                "reddit_id": submission.id,
                                "permalink": f"https://reddit.com{submission.permalink}",
                                "is_self": submission.is_self,
                                "upvote_ratio": submission.upvote_ratio,
                                "flair": submission.link_flair_text,
                            }
                        ),
                    )
                    items_yielded += 1

            except Exception:
                continue


class RedditStreamReader(SimpleDataSourceStreamReader):
    """
    Streaming reader for Reddit.

    Uses offset-based streaming tracking the latest seen post timestamp.
    """

    def __init__(self, options: Dict[str, str]):
        self.options = options

    def initialOffset(self) -> Dict[str, Any]:
        """Return initial offset."""
        from datetime import datetime, timezone, timedelta

        days_back = int(self.options.get("days_back", "1"))
        initial_time = datetime.now(timezone.utc) - timedelta(days=days_back)

        return {"timestamp": int(initial_time.timestamp()), "seen_ids": []}

    def read(self, start: Dict[str, Any]) -> Tuple[Iterator[Tuple], Dict[str, Any]]:
        """Read new posts since start offset."""
        import os
        from datetime import datetime, timezone

        from daily_databricks_feed.data_sources.pyspark_sources.base_source import (
            is_databricks_related,
            extract_keywords,
        )

        client_id = self.options.get("client_id") or os.environ.get("REDDIT_CLIENT_ID")
        client_secret = self.options.get("client_secret") or os.environ.get("REDDIT_CLIENT_SECRET")

        if not client_id or not client_secret:
            return (iter([]), start)

        try:
            import praw
        except ImportError:
            return (iter([]), start)

        subreddits_str = self.options.get("subreddits", "databricks,dataengineering")
        subreddits = [s.strip() for s in subreddits_str.split(",")]
        min_score = int(self.options.get("min_score", "3"))
        limit = int(self.options.get("limit", "50"))
        filter_databricks = self.options.get("filter_databricks", "true").lower() == "true"

        start_timestamp = start.get("timestamp", 0)
        seen_ids = set(start.get("seen_ids", []))
        fetched_at = datetime.now(timezone.utc)
        max_timestamp = start_timestamp

        results = []
        new_seen_ids = list(seen_ids)

        reddit = praw.Reddit(
            client_id=client_id,
            client_secret=client_secret,
            user_agent="DailyDatabricksFeed/1.0 PySpark DataSource Stream",
        )

        for subreddit_name in subreddits:
            try:
                subreddit = reddit.subreddit(subreddit_name)

                for submission in subreddit.new(limit=limit // len(subreddits)):
                    item_id = f"reddit_{submission.id}"

                    if item_id in seen_ids:
                        continue

                    if submission.score < min_score:
                        continue

                    created_at = datetime.fromtimestamp(submission.created_utc, tz=timezone.utc)

                    # Only get posts after start timestamp
                    if submission.created_utc <= start_timestamp:
                        continue

                    if submission.created_utc > max_timestamp:
                        max_timestamp = int(submission.created_utc)

                    title = submission.title
                    content = submission.selftext if submission.is_self else None
                    searchable = f"{title} {content or ''}"

                    if filter_databricks and subreddit_name.lower() != "databricks":
                        if not is_databricks_related(searchable):
                            continue

                    url = (
                        f"https://reddit.com{submission.permalink}"
                        if submission.is_self
                        else submission.url
                    )

                    author_name = str(submission.author) if submission.author else None

                    results.append(
                        (
                            item_id,
                            "reddit",
                            title,
                            url,
                            content,
                            author_name,
                            created_at,
                            fetched_at,
                            submission.score,
                            submission.num_comments,
                            extract_keywords(searchable),
                            json.dumps(
                                {
                                    "subreddit": subreddit_name,
                                    "reddit_id": submission.id,
                                    "permalink": f"https://reddit.com{submission.permalink}",
                                    "is_self": submission.is_self,
                                }
                            ),
                        )
                    )
                    new_seen_ids.append(item_id)

            except Exception:
                continue

        # Keep only recent seen IDs to prevent unbounded growth
        new_seen_ids = new_seen_ids[-1000:]

        next_offset = {
            "timestamp": (
                max_timestamp if max_timestamp > start_timestamp else int(fetched_at.timestamp())
            ),
            "seen_ids": new_seen_ids,
        }

        return (iter(results), next_offset)

    def readBetweenOffsets(self, start: Dict[str, Any], end: Dict[str, Any]) -> Iterator[Tuple]:
        """Deterministically read between offsets."""
        # For Reddit, we can't perfectly replay due to API limitations
        # Return empty iterator for replay scenarios
        return iter([])

    def commit(self, end: Dict[str, Any]) -> None:
        """Commit offset."""
        pass
