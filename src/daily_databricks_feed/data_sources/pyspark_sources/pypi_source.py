"""PySpark 4.0 Custom Data Source — PyPI package release tracker.

Options
-------
days_back  int  Days of history on first run (default 7)
limit      int  Max releases per invocation (default 50)

No credentials required — PyPI JSON API is public.
"""

import logging

from pyspark.sql.datasource import DataSource

from .base_source import BRONZE_SCHEMA, BaseNewsStreamReader

logger = logging.getLogger(__name__)


class PyPIStreamReader(BaseNewsStreamReader):
    _DEFAULT_DAYS_BACK = 7  # Releases don't happen every day; wider default window

    def _fetch_items(self, start_epoch: int, end_epoch: int):
        from daily_databricks_feed.data_sources.pypi_releases import PyPIReleasesSource

        source = PyPIReleasesSource()
        try:
            items = source.fetch_with_retry(
                since_epoch=start_epoch,
                limit=int(self.options.get("limit", "50")),
            )
        except Exception as exc:
            logger.error("PyPI fetch failed: %s", exc)
            return

        yield from items


class PyPIDataSource(DataSource):
    """PySpark 4.0 streaming data source for PyPI package releases."""

    @classmethod
    def name(cls) -> str:
        return "pypi_releases_news"

    def schema(self):
        return BRONZE_SCHEMA

    def streamReader(self, schema):
        return PyPIStreamReader(self.options)
