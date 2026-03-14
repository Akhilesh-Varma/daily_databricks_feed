"""YouTube data source using YouTube Data API v3."""

import os
from datetime import datetime, timezone, timedelta
from typing import List, Optional
import requests

from .base import BaseDataSource, NewsItem, rate_limited


class YouTubeSource(BaseDataSource):
    """
    Fetch videos from YouTube using the Data API v3.

    API Documentation: https://developers.google.com/youtube/v3

    Rate Limits:
    - 10,000 units per day (free tier)
    - Search costs 100 units, list costs 1 unit
    - We use 0.5 req/sec to preserve quota

    Requires environment variables:
    - YOUTUBE_API_KEY
    """

    SOURCE_NAME = "youtube"
    DEFAULT_RATE_LIMIT = 0.5  # 0.5 requests per second to preserve quota

    BASE_URL = "https://www.googleapis.com/youtube/v3"

    # Relevant YouTube channels for Databricks content
    DEFAULT_CHANNELS = [
        "UCTPjwxo4K0r7-qYdPV2l-4A",  # Databricks official
        "UC3q8O3Bh2Le8Rj1-Q-_UUbA",  # Data Engineering
    ]

    def __init__(
        self,
        api_key: Optional[str] = None,
        **kwargs,
    ):
        """
        Initialize YouTube data source.

        Args:
            api_key: YouTube Data API key (or set YOUTUBE_API_KEY env var)
        """
        super().__init__(**kwargs)

        self.api_key = api_key or os.environ.get("YOUTUBE_API_KEY")

        if not self.api_key:
            self.logger.warning(
                "YouTube API key not provided. Set YOUTUBE_API_KEY environment variable."
            )

    def is_available(self) -> bool:
        """Check if YouTube API is available and configured."""
        return self.api_key is not None

    @rate_limited
    def _make_request(self, endpoint: str, params: dict) -> dict:
        """
        Make a rate-limited request to the YouTube API.

        Args:
            endpoint: API endpoint (search, videos, channels)
            params: Query parameters

        Returns:
            JSON response as dictionary
        """
        params["key"] = self.api_key
        url = f"{self.BASE_URL}/{endpoint}"

        self.logger.debug(f"Requesting: {url}")

        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        return response.json()

    def fetch(
        self,
        query: Optional[str] = None,
        days_back: int = 7,
        limit: int = 50,
        filter_databricks: bool = True,
    ) -> List[NewsItem]:
        """
        Fetch videos from YouTube.

        Args:
            query: Search query (if None, searches for Databricks keywords)
            days_back: Number of days to look back
            limit: Maximum number of items to return
            filter_databricks: Whether to filter for Databricks-related content

        Returns:
            List of NewsItem objects
        """
        if not self.is_available():
            self.logger.warning("YouTube API key not available, returning empty list")
            return []

        all_items = []

        # If no query specified, search for Databricks-related terms
        search_queries = (
            [query]
            if query
            else [
                "databricks tutorial",
                "databricks",
                "delta lake",
                "apache spark tutorial",
                "lakehouse",
            ]
        )

        # Calculate timestamp for date filter
        cutoff_time = datetime.now(timezone.utc) - timedelta(days=days_back)

        for search_query in search_queries:
            try:
                items = self._search(
                    query=search_query,
                    published_after=cutoff_time,
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

        # Sort by score (view count) descending
        unique_items.sort(key=lambda x: x.score, reverse=True)

        # Apply Databricks filter if requested
        if filter_databricks:
            unique_items = self.filter_databricks_content(unique_items)

        return unique_items[:limit]

    def _search(
        self,
        query: str,
        published_after: datetime,
        limit: int = 25,
    ) -> List[NewsItem]:
        """
        Search YouTube for videos.

        Args:
            query: Search query
            published_after: Oldest allowed publish date
            limit: Maximum results

        Returns:
            List of NewsItem objects
        """
        params = {
            "part": "snippet",
            "q": query,
            "type": "video",
            "order": "relevance",
            "publishedAfter": published_after.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "maxResults": min(limit, 50),
            "relevanceLanguage": "en",
        }

        data = self._make_request("search", params)
        items = data.get("items", [])

        # Get video IDs for statistics
        video_ids = [item["id"]["videoId"] for item in items if item.get("id", {}).get("videoId")]

        # Fetch video statistics (view counts, etc.)
        stats = self._get_video_statistics(video_ids) if video_ids else {}

        return self._parse_search_results(items, stats)

    def _get_video_statistics(self, video_ids: List[str]) -> dict:
        """
        Get statistics for multiple videos.

        Args:
            video_ids: List of YouTube video IDs

        Returns:
            Dict mapping video ID to statistics
        """
        if not video_ids:
            return {}

        params = {
            "part": "statistics",
            "id": ",".join(video_ids[:50]),  # API limit is 50
        }

        try:
            data = self._make_request("videos", params)
            stats = {}
            for item in data.get("items", []):
                video_id = item["id"]
                statistics = item.get("statistics", {})
                stats[video_id] = {
                    "view_count": int(statistics.get("viewCount", 0)),
                    "like_count": int(statistics.get("likeCount", 0)),
                    "comment_count": int(statistics.get("commentCount", 0)),
                }
            return stats
        except Exception as e:
            self.logger.warning(f"Error fetching video statistics: {e}")
            return {}

    def _parse_search_results(self, items: List[dict], stats: dict) -> List[NewsItem]:
        """
        Parse YouTube search results into NewsItem objects.

        Args:
            items: List of search result items
            stats: Dict of video statistics

        Returns:
            List of NewsItem objects
        """
        news_items = []

        for item in items:
            try:
                video_id = item.get("id", {}).get("videoId")
                if not video_id:
                    continue

                snippet = item.get("snippet", {})

                # Parse publish time
                published_at = None
                if snippet.get("publishedAt"):
                    published_at = datetime.fromisoformat(
                        snippet["publishedAt"].replace("Z", "+00:00")
                    )

                # Get statistics for this video
                video_stats = stats.get(video_id, {})

                # Build searchable text for keyword extraction
                title = snippet.get("title", "")
                description = snippet.get("description", "")
                searchable_text = f"{title} {description}"

                news_item = NewsItem(
                    id=f"youtube_{video_id}",
                    source=self.SOURCE_NAME,
                    title=title,
                    url=f"https://www.youtube.com/watch?v={video_id}",
                    content=description,
                    author=snippet.get("channelTitle"),
                    published_at=published_at,
                    score=video_stats.get("view_count", 0),
                    comments_count=video_stats.get("comment_count", 0),
                    tags=self.extract_keywords(searchable_text),
                    metadata={
                        "video_id": video_id,
                        "channel_id": snippet.get("channelId"),
                        "channel_title": snippet.get("channelTitle"),
                        "thumbnail_url": snippet.get("thumbnails", {}).get("high", {}).get("url"),
                        "like_count": video_stats.get("like_count", 0),
                    },
                )
                news_items.append(news_item)

            except Exception as e:
                self.logger.warning(f"Error parsing search result: {e}")

        return news_items

    def get_channel_videos(
        self,
        channel_id: str,
        days_back: int = 7,
        limit: int = 10,
    ) -> List[NewsItem]:
        """
        Get recent videos from a specific channel.

        Args:
            channel_id: YouTube channel ID
            days_back: Number of days to look back
            limit: Maximum results

        Returns:
            List of NewsItem objects
        """
        if not self.is_available():
            return []

        cutoff_time = datetime.now(timezone.utc) - timedelta(days=days_back)

        params = {
            "part": "snippet",
            "channelId": channel_id,
            "type": "video",
            "order": "date",
            "publishedAfter": cutoff_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "maxResults": min(limit, 50),
        }

        try:
            data = self._make_request("search", params)
            items = data.get("items", [])
            video_ids = [
                item["id"]["videoId"] for item in items if item.get("id", {}).get("videoId")
            ]
            stats = self._get_video_statistics(video_ids) if video_ids else {}
            return self._parse_search_results(items, stats)
        except Exception as e:
            self.logger.error(f"Error fetching channel videos: {e}")
            return []

    def get_databricks_channel_videos(self, days_back: int = 7) -> List[NewsItem]:
        """
        Get recent videos from the official Databricks YouTube channel.

        Args:
            days_back: Number of days to look back

        Returns:
            List of NewsItem objects
        """
        # Databricks official channel ID
        databricks_channel_id = "UCTPjwxo4K0r7-qYdPV2l-4A"
        return self.get_channel_videos(databricks_channel_id, days_back=days_back)
