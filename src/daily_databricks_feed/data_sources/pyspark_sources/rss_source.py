"""PySpark 4.0 Custom Data Source — RSS Feeds (feedparser).

Options
-------
days_back         int   Days of history on first run (default 7, minimum 7)
limit             int   Max items per invocation (default 50)
filter_databricks bool  Keep only Databricks-related items (default true)

No credentials required — all configured feeds are public.
"""

import logging
from datetime import datetime, timezone

from pyspark.sql.datasource import DataSource

from .base_source import BRONZE_SCHEMA, BaseNewsStreamReader, item_to_tuple

logger = logging.getLogger(__name__)


class RSSStreamReader(BaseNewsStreamReader):
    _DEFAULT_DAYS_BACK = 7  # RSS feeds benefit from a longer lookback window

    def _fetch_rows(self, start_epoch: int, end_epoch: int, days_back: int):
        from daily_databricks_feed.data_sources.rss_feeds import RSSFeedSource

        source = RSSFeedSource()
        if not source.is_available():
            logger.warning("RSS parsing not available — skipping")
            return

        try:
            items = source.fetch_with_retry(
                days_back=max(days_back, 7),  # enforce minimum lookback
                limit=int(self.options.get("limit", "50")),
                filter_databricks=(
                    self.options.get("filter_databricks", "true").lower() == "true"
                ),
            )
        except Exception as exc:
            logger.error("RSS fetch failed: %s", exc)
            return

        now_str   = datetime.now(timezone.utc).isoformat()
        today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        for item in items:
            yield item_to_tuple(item, now_str, today_str)


class RSSFeedDataSource(DataSource):
    """PySpark 4.0 streaming data source for Databricks-related RSS feeds."""

    @classmethod
    def name(cls) -> str:
        return "rss_news"

    def schema(self):
        return BRONZE_SCHEMA

    def streamReader(self, schema):
        return RSSStreamReader(self.options)
