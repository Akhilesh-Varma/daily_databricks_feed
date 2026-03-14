"""Hacker News data source using Algolia API."""

import requests
from datetime import datetime, timezone, timedelta
from typing import List, Optional
from urllib.parse import urlencode

from .base import BaseDataSource, NewsItem, rate_limited


class HackerNewsSource(BaseDataSource):
    """
    Fetch news from Hacker News using the Algolia Search API.

    API Documentation: https://hn.algolia.com/api

    Rate Limits:
    - 10,000 requests per hour
    - We use 2 req/sec to be safe
    """

    SOURCE_NAME = "hacker_news"
    DEFAULT_RATE_LIMIT = 2.0  # 2 requests per second

    BASE_URL = "https://hn.algolia.com/api/v1"

    def __init__(self, **kwargs):
        """Initialize Hacker News data source."""
        super().__init__(**kwargs)

    @rate_limited
    def _make_request(self, endpoint: str, params: dict) -> dict:
        """
        Make a rate-limited request to the Algolia API.

        Args:
            endpoint: API endpoint (search, search_by_date, items/{id})
            params: Query parameters

        Returns:
            JSON response as dictionary
        """
        url = f"{self.BASE_URL}/{endpoint}"
        self.logger.debug(f"Requesting: {url}?{urlencode(params)}")

        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        return response.json()

    def fetch(
        self,
        query: Optional[str] = None,
        days_back: int = 1,
        min_points: int = 5,
        limit: int = 100,
        filter_databricks: bool = True,
    ) -> List[NewsItem]:
        """
        Fetch news from Hacker News.

        Args:
            query: Search query (if None, searches for Databricks keywords)
            days_back: Number of days to look back
            min_points: Minimum points/score threshold
            limit: Maximum number of items to return
            filter_databricks: Whether to filter for Databricks-related content

        Returns:
            List of NewsItem objects
        """
        all_items = []

        # If no query specified, search for each Databricks keyword
        search_queries = (
            [query]
            if query
            else [
                "databricks",
                "delta lake",
                "apache spark",
                "lakehouse",
                "mlflow",
            ]
        )

        # Calculate timestamp for date filter
        cutoff_time = datetime.now(timezone.utc) - timedelta(days=days_back)
        cutoff_timestamp = int(cutoff_time.timestamp())

        for search_query in search_queries:
            try:
                items = self._search(
                    query=search_query,
                    min_timestamp=cutoff_timestamp,
                    min_points=min_points,
                    limit=limit // len(search_queries),
                )
                all_items.extend(items)
            except Exception as e:
                self.logger.error(f"Error searching for '{search_query}': {e}")

        # Deduplicate by ID
        seen_ids = set()
        unique_items = []
        for item in all_items:
            if item.id not in seen_ids:
                seen_ids.add(item.id)
                unique_items.append(item)

        # Sort by score descending
        unique_items.sort(key=lambda x: x.score, reverse=True)

        # Apply Databricks filter if requested
        if filter_databricks:
            unique_items = self.filter_databricks_content(unique_items)

        return unique_items[:limit]

    def _search(
        self,
        query: str,
        min_timestamp: int,
        min_points: int = 0,
        limit: int = 50,
    ) -> List[NewsItem]:
        """
        Search Hacker News for stories.

        Args:
            query: Search query
            min_timestamp: Unix timestamp for oldest allowed post
            min_points: Minimum points threshold
            limit: Maximum results

        Returns:
            List of NewsItem objects
        """
        params = {
            "query": query,
            "tags": "story",
            "numericFilters": f"created_at_i>{min_timestamp},points>{min_points}",
            "hitsPerPage": min(limit, 100),
        }

        data = self._make_request("search", params)
        return self._parse_results(data.get("hits", []))

    def _parse_results(self, hits: List[dict]) -> List[NewsItem]:
        """
        Parse Algolia search results into NewsItem objects.

        Args:
            hits: List of hit objects from Algolia

        Returns:
            List of NewsItem objects
        """
        items = []

        for hit in hits:
            try:
                # Parse timestamp
                created_at = None
                if hit.get("created_at_i"):
                    created_at = datetime.fromtimestamp(hit["created_at_i"], tz=timezone.utc)

                # Build HN URL
                object_id = hit.get("objectID", "")
                url = hit.get("url") or f"https://news.ycombinator.com/item?id={object_id}"

                item = NewsItem(
                    id=f"hn_{object_id}",
                    source=self.SOURCE_NAME,
                    title=hit.get("title", ""),
                    url=url,
                    content=hit.get("story_text"),
                    author=hit.get("author"),
                    published_at=created_at,
                    score=hit.get("points", 0),
                    comments_count=hit.get("num_comments", 0),
                    tags=self.extract_keywords(
                        f"{hit.get('title', '')} {hit.get('story_text', '')}"
                    ),
                    metadata={
                        "hn_id": object_id,
                        "hn_url": f"https://news.ycombinator.com/item?id={object_id}",
                    },
                )
                items.append(item)

            except Exception as e:
                self.logger.warning(f"Error parsing hit: {e}")

        return items

    def get_top_stories(self, limit: int = 30) -> List[NewsItem]:
        """
        Get current top stories from HN front page.

        Args:
            limit: Maximum number of stories

        Returns:
            List of NewsItem objects
        """
        # Get top story IDs from official API
        response = requests.get("https://hacker-news.firebaseio.com/v0/topstories.json", timeout=30)
        response.raise_for_status()
        story_ids = response.json()[:limit]

        items = []
        for story_id in story_ids:
            try:
                item = self._get_item(story_id)
                if item:
                    items.append(item)
            except Exception as e:
                self.logger.warning(f"Error fetching story {story_id}: {e}")

        return self.filter_databricks_content(items)

    @rate_limited
    def _get_item(self, item_id: int) -> Optional[NewsItem]:
        """
        Get a single item by ID from the official HN API.

        Args:
            item_id: Hacker News item ID

        Returns:
            NewsItem or None if not found/invalid
        """
        url = f"https://hacker-news.firebaseio.com/v0/item/{item_id}.json"
        response = requests.get(url, timeout=30)
        response.raise_for_status()

        data = response.json()
        if not data or data.get("type") != "story":
            return None

        created_at = None
        if data.get("time"):
            created_at = datetime.fromtimestamp(data["time"], tz=timezone.utc)

        return NewsItem(
            id=f"hn_{item_id}",
            source=self.SOURCE_NAME,
            title=data.get("title", ""),
            url=data.get("url", f"https://news.ycombinator.com/item?id={item_id}"),
            content=data.get("text"),
            author=data.get("by"),
            published_at=created_at,
            score=data.get("score", 0),
            comments_count=data.get("descendants", 0),
            tags=self.extract_keywords(f"{data.get('title', '')} {data.get('text', '')}"),
            metadata={
                "hn_id": str(item_id),
                "hn_url": f"https://news.ycombinator.com/item?id={item_id}",
            },
        )
