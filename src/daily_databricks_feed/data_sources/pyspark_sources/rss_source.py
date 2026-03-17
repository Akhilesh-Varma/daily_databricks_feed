"""PySpark 4.0 Custom Data Source — RSS Feeds (feedparser).

Options
-------
days_back         int   Days of history on first run (default 7, minimum 7)
limit             int   Max items per invocation (default 50)
filter_databricks bool  Keep only Databricks-related items (default true)

No credentials required — all configured feeds are public.
"""

import logging

from pyspark.sql.datasource import DataSource

from .base_source import BRONZE_SCHEMA, BaseNewsStreamReader

logger = logging.getLogger(__name__)


class RSSStreamReader(BaseNewsStreamReader):
    _DEFAULT_DAYS_BACK = 7  # RSS feeds benefit from a longer initial lookback window

    def _fetch_items(self, start_epoch: int, end_epoch: int):
        """Fetch RSS articles published in [start_epoch, end_epoch).

        Passes start_epoch as the cutoff so only articles newer than the last
        checkpoint are requested from each feed.
        """
        from daily_databricks_feed.data_sources.rss_feeds import RSSFeedSource

        source = RSSFeedSource()
        if not source.is_available():
            logger.warning("RSS parsing not available — skipping")
            return

        try:
            items = source.fetch_with_retry(
                since_epoch=start_epoch,
                limit=int(self.options.get("limit", "50")),
                filter_databricks=(
                    self.options.get("filter_databricks", "true").lower() == "true"
                ),
            )
        except Exception as exc:
            logger.error("RSS fetch failed: %s", exc)
            return

        yield from items


class RSSFeedDataSource(DataSource):
    """PySpark 4.0 streaming data source for Databricks-related RSS feeds."""

    @classmethod
    def name(cls) -> str:
        return "rss_news"

    def schema(self):
        return BRONZE_SCHEMA

    def streamReader(self, schema):
        return RSSStreamReader(self.options)
