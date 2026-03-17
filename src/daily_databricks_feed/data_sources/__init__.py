"""Data source implementations for fetching news from various APIs."""

from .base import BaseDataSource
from .hacker_news import HackerNewsSource
from .reddit import RedditSource
from .youtube import YouTubeSource
from .rss_feeds import RSSFeedSource

# PySpark 4.0 Custom Data Source API implementations
try:
    from .pyspark_sources import (
        HackerNewsDataSource,
        RedditDataSource,
        YouTubeDataSource,
        RSSFeedDataSource,
    )
    PYSPARK_SOURCES_AVAILABLE = True
except ImportError:
    PYSPARK_SOURCES_AVAILABLE = False
    HackerNewsDataSource = None
    RedditDataSource = None
    YouTubeDataSource = None
    RSSFeedDataSource = None

__all__ = [
    "BaseDataSource",
    "HackerNewsSource",
    "RedditSource",
    "YouTubeSource",
    "RSSFeedSource",
    "HackerNewsDataSource",
    "RedditDataSource",
    "YouTubeDataSource",
    "RSSFeedDataSource",
    "PYSPARK_SOURCES_AVAILABLE",
]
