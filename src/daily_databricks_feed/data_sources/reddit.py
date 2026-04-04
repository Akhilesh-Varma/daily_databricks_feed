"""Reddit data source using PRAW (Python Reddit API Wrapper)."""

import os
from datetime import datetime, timezone, timedelta
from typing import List, Optional

from .base import BaseDataSource, NewsItem, rate_limited

# PRAW is optional - gracefully handle if not installed
try:
    import praw
    from praw.models import Submission

    PRAW_AVAILABLE = True
except ImportError:
    PRAW_AVAILABLE = False
    praw = None


class RedditSource(BaseDataSource):
    """
    Fetch news from Reddit using PRAW.

    API Documentation: https://www.reddit.com/dev/api

    Rate Limits:
    - 100 requests per minute for OAuth
    - 10,000 requests per month for free tier
    - We use 1.5 req/sec to be safe

    Requires environment variables:
    - REDDIT_CLIENT_ID
    - REDDIT_CLIENT_SECRET
    """

    SOURCE_NAME = "reddit"
    DEFAULT_RATE_LIMIT = 1.5  # 1.5 requests per second (90/min, under 100 limit)

    # Relevant subreddits for Databricks content
    DEFAULT_SUBREDDITS = [
        "databricks",
        "dataengineering",
        "apachespark",
        "datascience",
        "MachineLearning",
    ]

    def __init__(
        self,
        client_id: Optional[str] = None,
        client_secret: Optional[str] = None,
        user_agent: str = "DailyDatabricksFeed/1.0",
        **kwargs,
    ):
        """
        Initialize Reddit data source.

        Args:
            client_id: Reddit OAuth client ID (or set REDDIT_CLIENT_ID env var)
            client_secret: Reddit OAuth secret (or set REDDIT_CLIENT_SECRET env var)
            user_agent: User agent string for API requests
        """
        super().__init__(**kwargs)

        if not PRAW_AVAILABLE:
            self.logger.warning("PRAW not installed. Reddit source will be disabled.")
            self.reddit = None
            return

        # Get credentials from args or environment
        self.client_id = client_id or os.environ.get("REDDIT_CLIENT_ID")
        self.client_secret = client_secret or os.environ.get("REDDIT_CLIENT_SECRET")

        if not self.client_id or not self.client_secret:
            self.logger.warning(
                "Reddit credentials not provided. Set REDDIT_CLIENT_ID and "
                "REDDIT_CLIENT_SECRET environment variables."
            )
            self.reddit = None
            return

        # Initialize PRAW client (read-only mode)
        try:
            self.reddit = praw.Reddit(
                client_id=self.client_id,
                client_secret=self.client_secret,
                user_agent=user_agent,
            )
            self.logger.info("Reddit client initialized successfully")
        except Exception as e:
            self.logger.error(f"Failed to initialize Reddit client: {e}")
            self.reddit = None

    def is_available(self) -> bool:
        """Check if Reddit API is available and configured."""
        return self.reddit is not None

    def fetch(
        self,
        subreddits: Optional[List[str]] = None,
        days_back: int = 1,
        since_epoch: Optional[int] = None,
        min_score: int = 5,
        limit: int = 100,
        filter_databricks: bool = True,
    ) -> List[NewsItem]:
        """
        Fetch posts from Reddit.

        Args:
            subreddits: List of subreddit names to fetch from
            days_back: Number of days to look back (used only when since_epoch is None)
            since_epoch: Unix epoch of the earliest allowed publish time.
                         When set, takes precedence over days_back so that the
                         PySpark checkpoint boundary is used exactly.
            min_score: Minimum upvote score threshold
            limit: Maximum number of items to return
            filter_databricks: Whether to filter for Databricks-related content

        Returns:
            List of NewsItem objects
        """
        if not self.is_available():
            self.logger.warning("Reddit client not available, returning empty list")
            return []

        subreddits = subreddits or self.DEFAULT_SUBREDDITS
        # since_epoch takes precedence — use checkpoint boundary directly
        if since_epoch is not None:
            cutoff_time = datetime.fromtimestamp(since_epoch, tz=timezone.utc)
        else:
            cutoff_time = datetime.now(timezone.utc) - timedelta(days=days_back)
        all_items = []

        for subreddit_name in subreddits:
            try:
                items = self._fetch_subreddit(
                    subreddit_name=subreddit_name,
                    cutoff_time=cutoff_time,
                    min_score=min_score,
                    limit=limit // len(subreddits),
                )
                all_items.extend(items)
            except Exception as e:
                self.logger.error(f"Error fetching r/{subreddit_name}: {e}")

        # Deduplicate by ID
        seen_ids = set()
        unique_items = []
        for item in all_items:
            if item.id not in seen_ids:
                seen_ids.add(item.id)
                unique_items.append(item)

        # Sort by score descending
        unique_items.sort(key=lambda x: x.score, reverse=True)

        # Apply Databricks filter (except for r/databricks which is always relevant)
        if filter_databricks:
            filtered = []
            for item in unique_items:
                # r/databricks posts are always relevant
                if item.metadata.get("subreddit", "").lower() == "databricks":
                    filtered.append(item)
                elif self.is_databricks_related(f"{item.title} {item.content or ''}"):
                    filtered.append(item)
            unique_items = filtered

        return unique_items[:limit]

    @rate_limited
    def _fetch_subreddit(
        self,
        subreddit_name: str,
        cutoff_time: datetime,
        min_score: int = 0,
        limit: int = 50,
    ) -> List[NewsItem]:
        """
        Fetch posts from a single subreddit.

        Args:
            subreddit_name: Name of the subreddit
            cutoff_time: Oldest allowed post time
            min_score: Minimum score threshold
            limit: Maximum posts to fetch

        Returns:
            List of NewsItem objects
        """
        self.logger.debug(f"Fetching from r/{subreddit_name}")

        subreddit = self.reddit.subreddit(subreddit_name)
        items = []

        # Fetch from hot and new to get recent popular content
        for submission in subreddit.hot(limit=limit):
            item = self._parse_submission(submission, cutoff_time, min_score)
            if item:
                items.append(item)

        for submission in subreddit.new(limit=limit):
            item = self._parse_submission(submission, cutoff_time, min_score)
            if item:
                items.append(item)

        return items

    def _parse_submission(
        self,
        submission,  # praw.models.Submission
        cutoff_time: datetime,
        min_score: int,
    ) -> Optional[NewsItem]:
        """
        Parse a Reddit submission into a NewsItem.

        Args:
            submission: PRAW Submission object
            cutoff_time: Oldest allowed post time
            min_score: Minimum score threshold

        Returns:
            NewsItem or None if filtered out
        """
        try:
            # Check score threshold
            if submission.score < min_score:
                return None

            # Check time threshold
            created_at = datetime.fromtimestamp(submission.created_utc, tz=timezone.utc)
            if created_at < cutoff_time:
                return None

            # Get content (self text or link)
            content = submission.selftext if submission.is_self else None
            url = (
                f"https://reddit.com{submission.permalink}"
                if submission.is_self
                else submission.url
            )

            return NewsItem(
                id=f"reddit_{submission.id}",
                source=self.SOURCE_NAME,
                title=submission.title,
                url=url,
                content=content,
                author=str(submission.author) if submission.author else None,
                published_at=created_at,
                score=submission.score,
                comments_count=submission.num_comments,
                tags=self.extract_keywords(f"{submission.title} {content or ''}"),
                metadata={
                    "subreddit": submission.subreddit.display_name,
                    "reddit_id": submission.id,
                    "permalink": f"https://reddit.com{submission.permalink}",
                    "is_self": submission.is_self,
                    "upvote_ratio": submission.upvote_ratio,
                    "flair": submission.link_flair_text,
                },
            )

        except Exception as e:
            self.logger.warning(f"Error parsing submission: {e}")
            return None
