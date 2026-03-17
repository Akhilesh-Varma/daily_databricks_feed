"""PySpark 4.0 Custom Data Source — Databricks Community Forum (Discourse).

Options
-------
days_back  int  Days of history on first run (default 1)
limit      int  Max topics per invocation (default 50)

No credentials required — the Databricks Community forum is public.
"""

import logging

from pyspark.sql.datasource import DataSource

from .base_source import BRONZE_SCHEMA, BaseNewsStreamReader

logger = logging.getLogger(__name__)


class DiscourseStreamReader(BaseNewsStreamReader):
    _DEFAULT_DAYS_BACK = 1

    def _fetch_items(self, start_epoch: int, end_epoch: int):
        from daily_databricks_feed.data_sources.discourse import DatabricksCommunitySource

        source = DatabricksCommunitySource()
        try:
            items = source.fetch_with_retry(
                since_epoch=start_epoch,
                limit=int(self.options.get("limit", "50")),
            )
        except Exception as exc:
            logger.error("Discourse fetch failed: %s", exc)
            return

        yield from items


class DiscourseDataSource(DataSource):
    """PySpark 4.0 streaming data source for the Databricks Community Forum."""

    @classmethod
    def name(cls) -> str:
        return "discourse_news"

    def schema(self):
        return BRONZE_SCHEMA

    def streamReader(self, schema):
        return DiscourseStreamReader(self.options)
