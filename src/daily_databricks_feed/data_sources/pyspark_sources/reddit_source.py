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
from datetime import datetime, timezone

from pyspark.sql.datasource import DataSource

from .base_source import BRONZE_SCHEMA, BaseNewsStreamReader, item_to_tuple

logger = logging.getLogger(__name__)


class RedditStreamReader(BaseNewsStreamReader):
    _DEFAULT_DAYS_BACK = 1

    def _fetch_rows(self, start_epoch: int, end_epoch: int, days_back: int):
        from daily_databricks_feed.data_sources.reddit import RedditSource

        source = RedditSource(
            client_id=os.environ.get(
                "REDDIT_CLIENT_ID", self.options.get("client_id", "")
            ),
            client_secret=os.environ.get(
                "REDDIT_CLIENT_SECRET", self.options.get("client_secret", "")
            ),
        )
        if not source.is_available():
            logger.warning("Reddit API not configured — skipping")
            return

        try:
            items = source.fetch_with_retry(
                days_back=days_back,
                min_score=int(self.options.get("min_score", "3")),
                limit=int(self.options.get("limit", "50")),
                filter_databricks=(
                    self.options.get("filter_databricks", "true").lower() == "true"
                ),
            )
        except Exception as exc:
            logger.error("Reddit fetch failed: %s", exc)
            return

        now_str   = datetime.now(timezone.utc).isoformat()
        today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        for item in items:
            yield item_to_tuple(item, now_str, today_str)


class RedditDataSource(DataSource):
    """PySpark 4.0 streaming data source for Reddit via PRAW."""

    @classmethod
    def name(cls) -> str:
        return "reddit_news"

    def schema(self):
        return BRONZE_SCHEMA

    def streamReader(self, schema):
        return RedditStreamReader(self.options)
