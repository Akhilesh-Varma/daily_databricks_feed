"""Dev.to article data source using the Forem REST API."""

import requests
from datetime import datetime, timezone, timedelta
from typing import List, Optional

from .base import BaseDataSource, NewsItem, rate_limited


class DevToSource(BaseDataSource):
    """
    Fetch developer articles from dev.to (Forem) related to Databricks/data engineering.

    API Documentation: https://developers.forem.com/api
    Rate Limits: No documented hard limit; we use 2 req/sec.
    Authentication: None required for public reads.
    """

    SOURCE_NAME = "devto"
    DEFAULT_RATE_LIMIT = 2.0

    BASE_URL = "https://dev.to/api"

    # Tags to query — ordered by specificity
    TAGS = [
        "databricks",
        "apachespark",
        "dataengineering",
        "deltalake",
        "mlflow",
        "pyspark",
    ]

    def is_available(self) -> bool:
        return True  # No credentials needed

    def fetch(
        self,
        since_epoch: Optional[int] = None,
        days_back: int = 1,
        limit: int = 50,
        filter_databricks: bool = True,
        **kwargs,
    ) -> List[NewsItem]:
        """
        Fetch articles from dev.to for each tracked tag.

        Args:
            since_epoch: Unix epoch lower bound (checkpoint boundary).
            days_back: Fallback lookback when since_epoch is None.
            limit: Max items to return.
            filter_databricks: Drop articles without Databricks keywords.
        """
        if since_epoch is not None:
            cutoff = datetime.fromtimestamp(since_epoch, tz=timezone.utc)
        else:
            cutoff = datetime.now(timezone.utc) - timedelta(days=days_back)

        all_items: List[NewsItem] = []
        seen_ids: set = set()

        per_tag = max(1, limit // len(self.TAGS))

        for tag in self.TAGS:
            try:
                items = self._fetch_tag(tag, cutoff, per_page=min(per_tag, 30))
                for item in items:
                    if item.id not in seen_ids:
                        seen_ids.add(item.id)
                        all_items.append(item)
            except Exception as exc:
                self.logger.error("Error fetching dev.to tag '%s': %s", tag, exc)

        if filter_databricks:
            all_items = self.filter_databricks_content(all_items)

        all_items.sort(key=lambda x: x.score, reverse=True)
        return all_items[:limit]

    @rate_limited
    def _fetch_tag(self, tag: str, cutoff: datetime, per_page: int = 30) -> List[NewsItem]:
        resp = requests.get(
            f"{self.BASE_URL}/articles",
            params={"tag": tag, "per_page": per_page, "state": "fresh"},
            timeout=30,
        )
        resp.raise_for_status()

        items: List[NewsItem] = []
        for article in resp.json():
            try:
                item = self._parse_article(article, cutoff)
                if item:
                    items.append(item)
            except Exception as exc:
                self.logger.warning("Error parsing dev.to article: %s", exc)

        self.logger.info("Fetched %d articles from dev.to tag '%s'", len(items), tag)
        return items

    def _parse_article(self, article: dict, cutoff: datetime) -> Optional[NewsItem]:
        published_str = article.get("published_at")
        published_at: Optional[datetime] = None
        if published_str:
            published_at = datetime.fromisoformat(published_str.replace("Z", "+00:00"))

        if published_at and published_at < cutoff:
            return None

        article_id = article.get("id", "")
        title = article.get("title", "")
        if not title:
            return None

        url = article.get("url", "")
        description = article.get("description") or ""
        content = f"{description}"

        # Dev.to provides reading_time_minutes and reactions_count
        reactions = article.get("public_reactions_count", 0) or 0
        comments = article.get("comments_count", 0) or 0

        tag_list = article.get("tag_list", [])  # list of strings

        return NewsItem(
            id=f"devto_{article_id}",
            source=self.SOURCE_NAME,
            title=title,
            url=url,
            content=content,
            author=article.get("user", {}).get("name"),
            published_at=published_at,
            score=reactions,
            comments_count=comments,
            tags=list(set(
                [t.lower() for t in tag_list]
                + self.extract_keywords(f"{title} {description}")
            )),
            metadata={
                "article_id": article_id,
                "reading_time_minutes": article.get("reading_time_minutes", 0),
                "cover_image": article.get("cover_image"),
                "category": "tutorial_article",
            },
        )
