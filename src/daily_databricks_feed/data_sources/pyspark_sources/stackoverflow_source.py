"""PySpark 4.0 Custom Data Source — Stack Overflow (Stack Exchange API).

Options
-------
days_back         int   Days of history on first run (default 1)
limit             int   Max questions per invocation (default 50)
min_answers       int   Minimum answer count (default 1)
min_score         int   Minimum question score (default 1)
filter_databricks bool  Keep only Databricks-related items (default true)

Optional: STACK_EXCHANGE_API_KEY env var (10,000 req/day vs 300/day without).
"""

import logging

from pyspark.sql.datasource import DataSource

from .base_source import BRONZE_SCHEMA, BaseNewsStreamReader

logger = logging.getLogger(__name__)


class StackOverflowStreamReader(BaseNewsStreamReader):
    _DEFAULT_DAYS_BACK = 1

    def _fetch_items(self, start_epoch: int, end_epoch: int):
        from daily_databricks_feed.data_sources.stackoverflow import StackOverflowSource

        source = StackOverflowSource()
        try:
            items = source.fetch_with_retry(
                since_epoch=start_epoch,
                limit=int(self.options.get("limit", "50")),
                min_answers=int(self.options.get("min_answers", "1")),
                min_score=int(self.options.get("min_score", "1")),
                filter_databricks=(self.options.get("filter_databricks", "true").lower() == "true"),
            )
        except Exception as exc:
            logger.error("Stack Overflow fetch failed: %s", exc)
            return

        yield from items


class StackOverflowDataSource(DataSource):
    """PySpark 4.0 streaming data source for Stack Overflow questions."""

    @classmethod
    def name(cls) -> str:
        return "stackoverflow_news"

    def schema(self):
        return BRONZE_SCHEMA

    def streamReader(self, schema):
        return StackOverflowStreamReader(self.options)
