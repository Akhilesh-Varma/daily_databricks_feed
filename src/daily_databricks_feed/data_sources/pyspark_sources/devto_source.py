"""PySpark 4.0 Custom Data Source — Dev.to articles (Forem API).

Options
-------
days_back         int   Days of history on first run (default 1)
limit             int   Max articles per invocation (default 50)
filter_databricks bool  Keep only Databricks-related items (default true)

No credentials required — Dev.to API is public for reads.
"""

import logging

from pyspark.sql.datasource import DataSource

from .base_source import BRONZE_SCHEMA, BaseNewsStreamReader

logger = logging.getLogger(__name__)


class DevToStreamReader(BaseNewsStreamReader):
    _DEFAULT_DAYS_BACK = 1

    def _fetch_items(self, start_epoch: int, end_epoch: int):
        from daily_databricks_feed.data_sources.devto import DevToSource

        source = DevToSource()
        try:
            items = source.fetch_with_retry(
                since_epoch=start_epoch,
                limit=int(self.options.get("limit", "50")),
                filter_databricks=(
                    self.options.get("filter_databricks", "true").lower() == "true"
                ),
            )
        except Exception as exc:
            logger.error("Dev.to fetch failed: %s", exc)
            return

        yield from items


class DevToDataSource(DataSource):
    """PySpark 4.0 streaming data source for Dev.to developer articles."""

    @classmethod
    def name(cls) -> str:
        return "devto_news"

    def schema(self):
        return BRONZE_SCHEMA

    def streamReader(self, schema):
        return DevToStreamReader(self.options)
