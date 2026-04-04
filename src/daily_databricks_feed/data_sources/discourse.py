"""Databricks Community Forum data source (Discourse REST API)."""

import hashlib
import requests
from datetime import datetime, timezone, timedelta
from typing import List, Optional

from .base import BaseDataSource, NewsItem, rate_limited


class DatabricksCommunitySource(BaseDataSource):
    """
    Fetch trending topics from community.databricks.com (Discourse forum).

    API Documentation: https://docs.discourse.org/
    Rate Limits: No documented public limit; we use 1 req/sec to be polite.
    Authentication: None required for public read-only access.
    """

    SOURCE_NAME = "databricks_community"
    DEFAULT_RATE_LIMIT = 1.0

    BASE_URL = "https://community.databricks.com"

    # Endpoints to poll; each returns a page of topics
    _ENDPOINTS = [
        "/latest.json",  # Most recently active topics
        "/top/daily.json",  # Highest-engagement topics today
        "/top/weekly.json",  # Weekly trending (good for slower periods)
    ]

    def is_available(self) -> bool:
        return True  # Public, no credentials needed

    def fetch(
        self,
        since_epoch: Optional[int] = None,
        days_back: int = 1,
        limit: int = 50,
        **kwargs,
    ) -> List[NewsItem]:
        """
        Fetch trending Databricks community topics.

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
        seen_ids: set = set()

        for endpoint in self._ENDPOINTS:
            try:
                items = self._fetch_endpoint(endpoint, cutoff)
                for item in items:
                    if item.id not in seen_ids:
                        seen_ids.add(item.id)
                        all_items.append(item)
            except Exception as exc:
                self.logger.error("Error fetching Discourse endpoint %s: %s", endpoint, exc)

        all_items.sort(
            key=lambda x: x.score,  # sort by view+like composite score
            reverse=True,
        )
        return all_items[:limit]

    @rate_limited
    def _fetch_endpoint(self, endpoint: str, cutoff: datetime) -> List[NewsItem]:
        url = f"{self.BASE_URL}{endpoint}"
        resp = requests.get(url, timeout=30, headers={"User-Agent": "DailyDatabricksFeed/1.0"})
        resp.raise_for_status()

        data = resp.json()
        topics = data.get("topic_list", {}).get("topics", [])

        items: List[NewsItem] = []
        for topic in topics:
            try:
                item = self._parse_topic(topic, cutoff)
                if item:
                    items.append(item)
            except Exception as exc:
                self.logger.warning("Error parsing Discourse topic: %s", exc)

        return items

    def _parse_topic(self, topic: dict, cutoff: datetime) -> Optional[NewsItem]:
        created_str = topic.get("created_at")
        published_at: Optional[datetime] = None
        if created_str:
            published_at = datetime.fromisoformat(created_str.replace("Z", "+00:00"))

        # Allow topics with recent *activity* even if created before cutoff
        bumped_str = topic.get("bumped_at")
        bumped_at: Optional[datetime] = None
        if bumped_str:
            bumped_at = datetime.fromisoformat(bumped_str.replace("Z", "+00:00"))

        effective_time = bumped_at or published_at
        if effective_time and effective_time < cutoff:
            return None

        title = topic.get("title", "")
        if not title:
            return None

        topic_id = topic.get("id", "")
        url = f"{self.BASE_URL}/t/{topic.get('slug', str(topic_id))}/{topic_id}"

        # Composite engagement score: views + 10× likes
        views = topic.get("views", 0) or 0
        likes = topic.get("like_count", 0) or 0
        replies = topic.get("posts_count", 0) or 0
        score = views + likes * 10

        content = topic.get("excerpt") or title

        return NewsItem(
            id=f"discourse_{topic_id}",
            source=self.SOURCE_NAME,
            title=title,
            url=url,
            content=content,
            author=None,  # Author not in topic list response
            published_at=published_at,
            score=score,
            comments_count=replies,
            tags=self.extract_keywords(f"{title} {content}"),
            metadata={
                "topic_id": topic_id,
                "views": views,
                "likes": likes,
                "category_id": topic.get("category_id"),
                "category": "community_discussion",
            },
        )
