"""PySpark 4.0 Custom Data Source — GitHub Releases.

Options
-------
days_back            int   Days of history on first run (default 7)
limit                int   Max releases per invocation (default 50)
include_prereleases  bool  Include pre-release tags (default false)

GITHUB_TOKEN env var is optional but strongly recommended (5000 req/hr vs 60/hr).
"""

import logging

from pyspark.sql.datasource import DataSource

from .base_source import BRONZE_SCHEMA, BaseNewsStreamReader

logger = logging.getLogger(__name__)


class GitHubReleasesStreamReader(BaseNewsStreamReader):
    _DEFAULT_DAYS_BACK = 7  # Releases don't happen every day; wider default window

    def _fetch_items(self, start_epoch: int, end_epoch: int):
        from daily_databricks_feed.data_sources.github_releases import GitHubReleasesSource

        source = GitHubReleasesSource()
        try:
            items = source.fetch_with_retry(
                since_epoch=start_epoch,
                limit=int(self.options.get("limit", "50")),
                include_prereleases=(
                    self.options.get("include_prereleases", "false").lower() == "true"
                ),
            )
        except Exception as exc:
            logger.error("GitHub releases fetch failed: %s", exc)
            return

        yield from items


class GitHubReleasesDataSource(DataSource):
    """PySpark 4.0 streaming data source for GitHub release announcements."""

    @classmethod
    def name(cls) -> str:
        return "github_releases_news"

    def schema(self):
        return BRONZE_SCHEMA

    def streamReader(self, schema):
        return GitHubReleasesStreamReader(self.options)
