"""PySpark 4.0 Custom Data Source — Hacker News (Algolia API).

Options
-------
days_back         int   Days of history on first run (default 1)
min_points        int   Minimum story points (default 5)
limit             int   Max items per invocation (default 50)
filter_databricks bool  Keep only Databricks-related items (default true)

No credentials required — Algolia HN API is public.
"""

import logging
from datetime import datetime, timezone

from pyspark.sql.datasource import DataSource

from .base_source import BRONZE_SCHEMA, BaseNewsStreamReader, item_to_tuple

logger = logging.getLogger(__name__)


class HackerNewsStreamReader(BaseNewsStreamReader):
    _DEFAULT_DAYS_BACK = 1

    def _fetch_rows(self, start_epoch: int, end_epoch: int, days_back: int):
        from daily_databricks_feed.data_sources.hacker_news import HackerNewsSource

        source = HackerNewsSource()
        try:
            items = source.fetch_with_retry(
                days_back=days_back,
                min_points=int(self.options.get("min_points", "5")),
                limit=int(self.options.get("limit", "50")),
                filter_databricks=(
                    self.options.get("filter_databricks", "true").lower() == "true"
                ),
            )
        except Exception as exc:
            logger.error("HackerNews fetch failed: %s", exc)
            return

        now_str   = datetime.now(timezone.utc).isoformat()
        today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        for item in items:
            yield item_to_tuple(item, now_str, today_str)


class HackerNewsDataSource(DataSource):
    """PySpark 4.0 streaming data source for the Hacker News Algolia API."""

    @classmethod
    def name(cls) -> str:
        return "hacker_news_news"

    def schema(self):
        return BRONZE_SCHEMA

    def streamReader(self, schema):
        return HackerNewsStreamReader(self.options)
