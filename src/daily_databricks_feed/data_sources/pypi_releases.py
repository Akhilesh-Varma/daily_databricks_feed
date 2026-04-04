"""PyPI release tracker — detects new versions of Databricks-ecosystem packages."""

import requests
from datetime import datetime, timezone, timedelta
from typing import List, Optional

from .base import BaseDataSource, NewsItem, rate_limited


class PyPIReleasesSource(BaseDataSource):
    """
    Poll the PyPI JSON API for new releases of Databricks-related Python packages.

    API Documentation: https://warehouse.pypa.io/api-reference/json.html
    Rate Limits: No documented limit; we use 2 req/sec.
    Authentication: None required.
    """

    SOURCE_NAME = "pypi_releases"
    DEFAULT_RATE_LIMIT = 2.0

    BASE_URL = "https://pypi.org/pypi"

    # Packages to track — ordered by user impact
    TRACKED_PACKAGES = [
        {"name": "databricks-sdk", "label": "Databricks SDK"},
        {"name": "mlflow", "label": "MLflow"},
        {"name": "databricks-connect", "label": "Databricks Connect"},
        {"name": "pyspark", "label": "Apache Spark (PySpark)"},
        {"name": "delta-spark", "label": "Delta Lake (PySpark)"},
        {"name": "databricks-feature-engineering", "label": "Databricks Feature Engineering"},
        {"name": "databricks-vectorsearch", "label": "Databricks Vector Search"},
        {"name": "databricks-langchain", "label": "Databricks LangChain Integration"},
    ]

    def is_available(self) -> bool:
        return True  # Public, no credentials needed

    def fetch(
        self,
        since_epoch: Optional[int] = None,
        days_back: int = 7,
        limit: int = 50,
        **kwargs,
    ) -> List[NewsItem]:
        """
        Fetch packages that published a new release within the time window.

        Args:
            since_epoch: Unix epoch lower bound (checkpoint boundary).
            days_back: Fallback lookback when since_epoch is None.
            limit: Max items to return.
        """
        if since_epoch is not None:
            cutoff = datetime.fromtimestamp(since_epoch, tz=timezone.utc)
        else:
            cutoff = datetime.now(timezone.utc) - timedelta(days=days_back)

        all_items: List[NewsItem] = []

        for pkg_cfg in self.TRACKED_PACKAGES:
            try:
                items = self._fetch_package(pkg_cfg, cutoff)
                all_items.extend(items)
            except Exception as exc:
                self.logger.error("Error fetching PyPI package '%s': %s", pkg_cfg["name"], exc)

        all_items.sort(
            key=lambda x: x.published_at or datetime.min.replace(tzinfo=timezone.utc),
            reverse=True,
        )
        return all_items[:limit]

    @rate_limited
    def _fetch_package(self, pkg_cfg: dict, cutoff: datetime) -> List[NewsItem]:
        pkg_name = pkg_cfg["name"]
        label = pkg_cfg["label"]

        resp = requests.get(f"{self.BASE_URL}/{pkg_name}/json", timeout=30)
        if resp.status_code == 404:
            self.logger.debug("Package '%s' not found on PyPI", pkg_name)
            return []
        resp.raise_for_status()

        data = resp.json()
        releases: dict = data.get("releases", {})
        info: dict = data.get("info", {})

        items: List[NewsItem] = []

        for version, files in releases.items():
            if not files:
                continue

            # Use the earliest upload time for this version (first file uploaded)
            upload_times = []
            for f in files:
                upload_str = f.get("upload_time_iso_8601") or f.get("upload_time")
                if upload_str:
                    try:
                        dt = datetime.fromisoformat(upload_str.replace("Z", "+00:00"))
                        if dt.tzinfo is None:
                            dt = dt.replace(tzinfo=timezone.utc)
                        upload_times.append(dt)
                    except ValueError:
                        pass

            if not upload_times:
                continue

            published_at = min(upload_times)  # earliest file = release moment

            if published_at < cutoff:
                continue

            # Build release notes URL (PyPI changelog page)
            url = f"https://pypi.org/project/{pkg_name}/{version}/"

            # Use project description as content summary (first 500 chars)
            summary = info.get("summary") or ""
            project_url = info.get("project_url") or info.get("project_urls", {}).get(
                "Changelog", ""
            )

            content = (f"{label} version {version} was released on PyPI. " f"{summary}").strip()

            title = f"{label} {version} released on PyPI"

            item = NewsItem(
                id=f"pypi_{pkg_name}_{version}",
                source=self.SOURCE_NAME,
                title=title,
                url=url,
                content=content,
                author=info.get("author") or info.get("maintainer"),
                published_at=published_at,
                score=0,
                comments_count=0,
                tags=self.extract_keywords(f"{label} {summary}"),
                metadata={
                    "package": pkg_name,
                    "label": label,
                    "version": version,
                    "home_page": info.get("home_page") or info.get("project_url"),
                    "requires_python": info.get("requires_python"),
                    "category": "package_release",
                },
            )
            items.append(item)

        self.logger.info("Found %d new release(s) for '%s' since cutoff", len(items), pkg_name)
        return items
