"""Daily Databricks Feed - A daily podcast generator for Databricks news."""

__version__ = "0.1.0"
__author__ = "Daily Databricks Feed Team"

from .data_sources import (
    HackerNewsSource,
    RedditSource,
    YouTubeSource,
    RSSFeedSource,
)
from .aggregation.script_generator import ScriptGenerator
from .podcast.tts_generator import TTSGenerator
from .podcast.rss_publisher import RSSPublisher

__all__ = [
    "HackerNewsSource",
    "RedditSource",
    "YouTubeSource",
    "RSSFeedSource",
    "ScriptGenerator",
    "TTSGenerator",
    "RSSPublisher",
]
