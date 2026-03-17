"""PySpark 4.0 Custom Data Source — Reddit (PRAW).

Options
-------
days_back         int   Days of history on first run (default 1)
min_score         int   Minimum post score (default 3)
limit             int   Max items per invocation (default 50)
filter_databricks bool  Keep only Databricks-related items (default true)

Credentials are read from the REDDIT_CLIENT_ID and REDDIT_CLIENT_SECRET
environment variables (injected by the Databricks job task).
"""

import logging
import os

from pyspark.sql.datasource import DataSource

from .base_source import BRONZE_SCHEMA, BaseNewsStreamReader

logger = logging.getLogger(__name__)


class RedditStreamReader(BaseNewsStreamReader):
    _DEFAULT_DAYS_BACK = 1

    def _fetch_items(self, start_epoch: int, end_epoch: int):
        """Fetch Reddit posts published in [start_epoch, end_epoch).

        Passes start_epoch as the cutoff so only posts newer than the last
        checkpoint are requested from the API.
        """
        from daily_databricks_feed.data_sources.reddit import RedditSource

        source = RedditSource(
            client_id=os.environ.get("REDDIT_CLIENT_ID", self.options.get("client_id", "")),
            client_secret=os.environ.get(
                "REDDIT_CLIENT_SECRET", self.options.get("client_secret", "")
            ),
        )
        if not source.is_available():
            logger.warning("Reddit API not configured — skipping")
            return

        try:
            items = source.fetch_with_retry(
                since_epoch=start_epoch,
                min_score=int(self.options.get("min_score", "3")),
                limit=int(self.options.get("limit", "50")),
                filter_databricks=(
                    self.options.get("filter_databricks", "true").lower() == "true"
                ),
            )
        except Exception as exc:
            logger.error("Reddit fetch failed: %s", exc)
            return

        yield from items


class RedditDataSource(DataSource):
    """PySpark 4.0 streaming data source for Reddit via PRAW."""

    @classmethod
    def name(cls) -> str:
        return "reddit_news"

    def schema(self):
        return BRONZE_SCHEMA

    def streamReader(self, schema):
        return RedditStreamReader(self.options)
