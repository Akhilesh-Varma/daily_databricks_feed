"""PySpark 4.0 Custom Data Source API implementations for streaming ingestion.

Each class is a DataSource that exposes a streaming reader backed by the
corresponding REST API.  Register them once per SparkSession before use:

    spark.dataSource.register(HackerNewsDataSource)
    spark.dataSource.register(RedditDataSource)
    spark.dataSource.register(YouTubeDataSource)
    spark.dataSource.register(RSSFeedDataSource)

Then read via:

    spark.readStream.format("hacker_news_news").option(...).load()
    spark.readStream.format("reddit_news").option(...).load()
    spark.readStream.format("youtube_news").option(...).load()
    spark.readStream.format("rss_news").option(...).load()
"""

from .hacker_news_source import HackerNewsDataSource
from .reddit_source import RedditDataSource
from .rss_source import RSSFeedDataSource
from .youtube_source import YouTubeDataSource

__all__ = [
    "HackerNewsDataSource",
    "RedditDataSource",
    "YouTubeDataSource",
    "RSSFeedDataSource",
]
