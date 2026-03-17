"""PySpark 4.0 Custom Data Source — YouTube (Data API v3).

Options
-------
days_back         int   Days of history on first run (default 7, minimum 7)
limit             int   Max items per invocation (default 30)
filter_databricks bool  Keep only Databricks-related items (default true)

Credentials are read from the YOUTUBE_API_KEY environment variable
(injected by the Databricks job task).
"""

import logging
import os

from pyspark.sql.datasource import DataSource

from .base_source import BRONZE_SCHEMA, BaseNewsStreamReader

logger = logging.getLogger(__name__)


class YouTubeStreamReader(BaseNewsStreamReader):
    _DEFAULT_DAYS_BACK = 7  # YouTube search requires a minimum 7-day window

    def _fetch_items(self, start_epoch: int, end_epoch: int):
        """Fetch YouTube videos published in [start_epoch, end_epoch).

        Passes start_epoch as publishedAfter so only videos newer than the
        last checkpoint are requested from the API.
        """
        from daily_databricks_feed.data_sources.youtube import YouTubeSource

        source = YouTubeSource(
            api_key=os.environ.get("YOUTUBE_API_KEY", self.options.get("api_key", "")),
        )
        if not source.is_available():
            logger.warning("YouTube API not configured — skipping")
            return

        try:
            items = source.fetch_with_retry(
                since_epoch=start_epoch,
                limit=int(self.options.get("limit", "30")),
                filter_databricks=(
                    self.options.get("filter_databricks", "true").lower() == "true"
                ),
            )
        except Exception as exc:
            logger.error("YouTube fetch failed: %s", exc)
            return

        yield from items


class YouTubeDataSource(DataSource):
    """PySpark 4.0 streaming data source for the YouTube Data API v3."""

    @classmethod
    def name(cls) -> str:
        return "youtube_news"

    def schema(self):
        return BRONZE_SCHEMA

    def streamReader(self, schema):
        return YouTubeStreamReader(self.options)
