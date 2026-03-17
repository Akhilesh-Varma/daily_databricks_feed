"""Stack Overflow data source via the Stack Exchange REST API."""

import os
import requests
from datetime import datetime, timezone, timedelta
from typing import List, Optional

from .base import BaseDataSource, NewsItem, rate_limited


class StackOverflowSource(BaseDataSource):
    """
    Fetch highly-voted, recently-answered Stack Overflow questions about
    Databricks, PySpark, and the data lakehouse ecosystem.

    API Documentation: https://api.stackexchange.com/docs
    Rate Limits:
    - 300 req/day without key
    - 10,000 req/day with STACK_EXCHANGE_API_KEY (free, register at stackapps.com)

    Optional environment variable:
    - STACK_EXCHANGE_API_KEY
    """

    SOURCE_NAME = "stackoverflow"
    DEFAULT_RATE_LIMIT = 0.5  # conservative; 300 req/day without key

    BASE_URL = "https://api.stackexchange.com/2.3"

    # Tag groups to query — use semicolon to AND tags within a group
    TAG_QUERIES = [
        "databricks",
        "apache-spark;databricks",
        "pyspark;databricks",
        "delta-lake",
        "mlflow",
        "apache-spark;delta-lake",
    ]

    def __init__(self, api_key: Optional[str] = None, **kwargs):
        """
        Args:
            api_key: Stack Exchange API key (or set STACK_EXCHANGE_API_KEY).
        """
        super().__init__(**kwargs)
        self.api_key = api_key or os.environ.get("STACK_EXCHANGE_API_KEY")
        if not self.api_key:
            self.logger.warning(
                "STACK_EXCHANGE_API_KEY not set — limited to 300 req/day. "
                "Register at https://stackapps.com for a free key."
            )

    def is_available(self) -> bool:
        return True  # Works without a key at reduced quota

    def fetch(
        self,
        since_epoch: Optional[int] = None,
        days_back: int = 1,
        limit: int = 50,
        min_answers: int = 1,
        min_score: int = 1,
        filter_databricks: bool = True,
        **kwargs,
    ) -> List[NewsItem]:
        """
        Fetch recently-created questions with at least one answer.

        Args:
            since_epoch: Unix epoch lower bound (checkpoint boundary).
            days_back: Fallback lookback when since_epoch is None.
            limit: Max items to return.
            min_answers: Minimum number of answers (filters unanswered noise).
            min_score: Minimum question score (upvotes - downvotes).
            filter_databricks: Drop items without Databricks keywords.
        """
        if since_epoch is not None:
            from_date = since_epoch
        else:
            from_date = int(
                (datetime.now(timezone.utc) - timedelta(days=days_back)).timestamp()
            )

        all_items: List[NewsItem] = []
        seen_ids: set = set()

        per_query = max(1, limit // len(self.TAG_QUERIES))

        for tags in self.TAG_QUERIES:
            try:
                items = self._fetch_questions(tags, from_date, per_query, min_score)
                for item in items:
                    if item.id not in seen_ids:
                        seen_ids.add(item.id)
                        all_items.append(item)
            except Exception as exc:
                self.logger.error("Error fetching SO tag '%s': %s", tags, exc)

        # Apply answer count filter after dedup
        all_items = [i for i in all_items if i.comments_count >= min_answers]

        if filter_databricks:
            all_items = self.filter_databricks_content(all_items)

        all_items.sort(key=lambda x: x.score, reverse=True)
        return all_items[:limit]

    @rate_limited
    def _fetch_questions(
        self, tags: str, from_date: int, page_size: int, min_score: int
    ) -> List[NewsItem]:
        params = {
            "tagged": tags,
            "fromdate": from_date,
            "order": "desc",
            "sort": "votes",
            "site": "stackoverflow",
            "filter": "withbody",  # includes body text
            "pagesize": min(page_size, 30),
        }
        if self.api_key:
            params["key"] = self.api_key

        resp = requests.get(
            f"{self.BASE_URL}/questions", params=params, timeout=30
        )
        resp.raise_for_status()

        data = resp.json()
        if data.get("quota_remaining") is not None:
            self.logger.debug("Stack Exchange quota remaining: %d", data["quota_remaining"])

        items: List[NewsItem] = []
        for q in data.get("items", []):
            try:
                item = self._parse_question(q, min_score)
                if item:
                    items.append(item)
            except Exception as exc:
                self.logger.warning("Error parsing SO question: %s", exc)

        return items

    def _parse_question(self, q: dict, min_score: int) -> Optional[NewsItem]:
        score = q.get("score", 0) or 0
        if score < min_score:
            return None

        question_id = q.get("question_id", "")
        title = q.get("title", "")
        if not title:
            return None

        # creation_date is a Unix timestamp integer
        created_ts = q.get("creation_date")
        published_at: Optional[datetime] = None
        if created_ts:
            published_at = datetime.fromtimestamp(created_ts, tz=timezone.utc)

        body = q.get("body") or ""
        # Strip HTML tags from body
        import re
        content = re.sub(r"<[^>]+>", " ", body)
        content = re.sub(r"\s+", " ", content).strip()[:1000]

        q_tags = [t.lower() for t in q.get("tags", [])]

        return NewsItem(
            id=f"so_{question_id}",
            source=self.SOURCE_NAME,
            title=title,
            url=q.get("link", f"https://stackoverflow.com/q/{question_id}"),
            content=content,
            author=q.get("owner", {}).get("display_name"),
            published_at=published_at,
            score=score,
            comments_count=q.get("answer_count", 0) or 0,
            tags=list(set(q_tags + self.extract_keywords(f"{title} {content}"))),
            metadata={
                "question_id": question_id,
                "view_count": q.get("view_count", 0),
                "answer_count": q.get("answer_count", 0),
                "is_answered": q.get("is_answered", False),
                "accepted_answer_id": q.get("accepted_answer_id"),
                "category": "developer_qa",
            },
        )
