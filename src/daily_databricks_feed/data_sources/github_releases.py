"""GitHub Releases data source — tracks OSS repos relevant to Databricks."""

import os
import requests
from datetime import datetime, timezone, timedelta
from typing import List, Optional

from .base import BaseDataSource, NewsItem, rate_limited


class GitHubReleasesSource(BaseDataSource):
    """
    Fetch release announcements from GitHub for Databricks-ecosystem repos.

    API Documentation: https://docs.github.com/en/rest/releases/releases

    Rate Limits:
    - 60 req/hr unauthenticated, 5000 req/hr with GITHUB_TOKEN
    - We track 8 repos, each costs 1 request per poll

    Optional environment variable:
    - GITHUB_TOKEN  (Personal Access Token or Fine-Grained Token, read:public_repo scope)
    """

    SOURCE_NAME = "github_releases"
    DEFAULT_RATE_LIMIT = 2.0  # 2 requests per second

    BASE_URL = "https://api.github.com"

    # Repos tracked in order of relevance to Databricks practitioners
    TRACKED_REPOS = [
        {"owner": "databricks", "repo": "databricks-sdk-py", "label": "Databricks SDK (Python)"},
        {"owner": "databricks", "repo": "cli", "label": "Databricks CLI"},
        {
            "owner": "databricks",
            "repo": "terraform-provider-databricks",
            "label": "Databricks Terraform Provider",
        },
        {"owner": "delta-io", "repo": "delta", "label": "Delta Lake"},
        {"owner": "delta-io", "repo": "delta-rs", "label": "Delta Lake Rust/Python"},
        {"owner": "mlflow", "repo": "mlflow", "label": "MLflow"},
        {"owner": "apache", "repo": "spark", "label": "Apache Spark"},
        {"owner": "databricks", "repo": "databricks-sdk-java", "label": "Databricks SDK (Java)"},
    ]

    def __init__(self, token: Optional[str] = None, **kwargs):
        """
        Args:
            token: GitHub Personal Access Token (or set GITHUB_TOKEN env var).
                   Works without a token at reduced rate (60 req/hr).
        """
        super().__init__(**kwargs)
        self.token = token or os.environ.get("GITHUB_TOKEN")
        if not self.token:
            self.logger.warning(
                "GITHUB_TOKEN not set — GitHub API limited to 60 req/hr. "
                "Set GITHUB_TOKEN for 5,000 req/hr."
            )

    def is_available(self) -> bool:
        return True  # Works unauthenticated; token only affects rate limit

    def _headers(self) -> dict:
        h = {
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        }
        if self.token:
            h["Authorization"] = f"Bearer {self.token}"
        return h

    def fetch(
        self,
        since_epoch: Optional[int] = None,
        days_back: int = 7,
        limit: int = 50,
        include_prereleases: bool = False,
        **kwargs,
    ) -> List[NewsItem]:
        """
        Fetch new releases from all tracked repos.

        Args:
            since_epoch: Unix epoch lower bound (checkpoint boundary).
            days_back: Fallback lookback when since_epoch is None.
            limit: Max total items returned.
            include_prereleases: Whether to include pre-release / alpha tags.
        """
        if since_epoch is not None:
            cutoff = datetime.fromtimestamp(since_epoch, tz=timezone.utc)
        else:
            cutoff = datetime.now(timezone.utc) - timedelta(days=days_back)

        all_items: List[NewsItem] = []

        for repo_cfg in self.TRACKED_REPOS:
            try:
                items = self._fetch_releases(repo_cfg, cutoff, include_prereleases)
                all_items.extend(items)
            except Exception as exc:
                self.logger.error(
                    "Error fetching %s/%s: %s",
                    repo_cfg["owner"],
                    repo_cfg["repo"],
                    exc,
                )

        all_items.sort(
            key=lambda x: x.published_at or datetime.min.replace(tzinfo=timezone.utc),
            reverse=True,
        )
        return all_items[:limit]

    @rate_limited
    def _fetch_releases(
        self,
        repo_cfg: dict,
        cutoff: datetime,
        include_prereleases: bool,
    ) -> List[NewsItem]:
        owner = repo_cfg["owner"]
        repo = repo_cfg["repo"]
        label = repo_cfg["label"]

        url = f"{self.BASE_URL}/repos/{owner}/{repo}/releases"
        resp = requests.get(url, headers=self._headers(), params={"per_page": 10}, timeout=30)
        resp.raise_for_status()

        items: List[NewsItem] = []
        for release in resp.json():
            if release.get("draft"):
                continue
            if not include_prereleases and release.get("prerelease"):
                continue

            published_at: Optional[datetime] = None
            if release.get("published_at"):
                published_at = datetime.fromisoformat(
                    release["published_at"].replace("Z", "+00:00")
                )

            if published_at and published_at < cutoff:
                continue

            tag = release.get("tag_name", "")
            title = f"{label} {tag} released"
            body = (release.get("body") or "")[:2000]  # Release notes (Markdown)

            item = NewsItem(
                id=f"github_release_{owner}_{repo}_{release['id']}",
                source=self.SOURCE_NAME,
                title=title,
                url=release.get("html_url", ""),
                content=body,
                author=release.get("author", {}).get("login"),
                published_at=published_at,
                score=0,
                comments_count=0,
                tags=self.extract_keywords(f"{title} {body}"),
                metadata={
                    "repo": f"{owner}/{repo}",
                    "label": label,
                    "tag": tag,
                    "prerelease": release.get("prerelease", False),
                    "release_id": release["id"],
                    "category": "release",
                },
            )
            items.append(item)

        self.logger.info("Fetched %d releases from %s/%s", len(items), owner, repo)
        return items
