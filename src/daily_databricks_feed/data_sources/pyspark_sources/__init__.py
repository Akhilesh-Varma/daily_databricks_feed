"""PySpark 4.0 Custom Data Source API implementations for streaming ingestion.

Each class is a DataSource that exposes a streaming reader backed by the
corresponding REST API.  Register them once per SparkSession before use:

    spark.dataSource.register(HackerNewsDataSource)
    spark.dataSource.register(RedditDataSource)
    spark.dataSource.register(YouTubeDataSource)
    spark.dataSource.register(RSSFeedDataSource)
    spark.dataSource.register(GitHubReleasesDataSource)
    spark.dataSource.register(DiscourseDataSource)
    spark.dataSource.register(DevToDataSource)
    spark.dataSource.register(StackOverflowDataSource)
    spark.dataSource.register(PyPIDataSource)

Then read via:

    spark.readStream.format("hacker_news_news").option(...).load()
    spark.readStream.format("reddit_news").option(...).load()
    spark.readStream.format("youtube_news").option(...).load()
    spark.readStream.format("rss_news").option(...).load()
    spark.readStream.format("github_releases_news").option(...).load()
    spark.readStream.format("discourse_news").option(...).load()
    spark.readStream.format("devto_news").option(...).load()
    spark.readStream.format("stackoverflow_news").option(...).load()
    spark.readStream.format("pypi_releases_news").option(...).load()
"""

from .devto_source import DevToDataSource
from .discourse_source import DiscourseDataSource
from .github_source import GitHubReleasesDataSource
from .hacker_news_source import HackerNewsDataSource
from .pypi_source import PyPIDataSource
from .reddit_source import RedditDataSource
from .rss_source import RSSFeedDataSource
from .stackoverflow_source import StackOverflowDataSource
from .youtube_source import YouTubeDataSource

__all__ = [
    "HackerNewsDataSource",
    "RedditDataSource",
    "YouTubeDataSource",
    "RSSFeedDataSource",
    "GitHubReleasesDataSource",
    "DiscourseDataSource",
    "DevToDataSource",
    "StackOverflowDataSource",
    "PyPIDataSource",
]
