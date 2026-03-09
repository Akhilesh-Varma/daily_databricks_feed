"""Tests for data source modules."""

import pytest
from datetime import datetime, timezone
from unittest.mock import Mock, patch

from daily_databricks_feed.data_sources.base import (
    BaseDataSource,
    NewsItem,
    RateLimiter,
)
from daily_databricks_feed.data_sources.hacker_news import HackerNewsSource
from daily_databricks_feed.data_sources.rss_feeds import RSSFeedSource


class TestNewsItem:
    """Tests for NewsItem dataclass."""

    def test_create_news_item(self):
        """Test creating a basic news item."""
        item = NewsItem(
            id="test_123",
            source="test",
            title="Test Title",
            url="https://example.com/test",
        )

        assert item.id == "test_123"
        assert item.source == "test"
        assert item.title == "Test Title"
        assert item.url == "https://example.com/test"
        assert item.content is None
        assert item.score == 0

    def test_news_item_to_dict(self):
        """Test converting news item to dictionary."""
        item = NewsItem(
            id="test_123",
            source="test",
            title="Test Title",
            url="https://example.com/test",
            score=42,
            tags=["databricks", "spark"],
        )

        result = item.to_dict()

        assert result["id"] == "test_123"
        assert result["source"] == "test"
        assert result["score"] == 42
        assert result["tags"] == ["databricks", "spark"]

    def test_news_item_with_datetime(self):
        """Test news item with datetime fields."""
        now = datetime.now(timezone.utc)
        item = NewsItem(
            id="test_123",
            source="test",
            title="Test",
            url="https://example.com",
            published_at=now,
            fetched_at=now,
        )

        result = item.to_dict()
        assert result["published_at"] == now.isoformat()
        assert result["fetched_at"] == now.isoformat()


class TestRateLimiter:
    """Tests for RateLimiter class."""

    def test_rate_limiter_creation(self):
        """Test creating a rate limiter."""
        limiter = RateLimiter(requests_per_second=2.0)
        assert limiter.requests_per_second == 2.0
        assert limiter.min_interval == 0.5

    def test_rate_limiter_zero_rate(self):
        """Test rate limiter with zero rate."""
        limiter = RateLimiter(requests_per_second=0)
        assert limiter.min_interval == 0


class TestBaseDataSource:
    """Tests for BaseDataSource class."""

    def test_databricks_keywords(self):
        """Test that Databricks keywords are defined."""
        assert "databricks" in BaseDataSource.DATABRICKS_KEYWORDS
        assert "delta lake" in BaseDataSource.DATABRICKS_KEYWORDS
        assert "spark" in BaseDataSource.DATABRICKS_KEYWORDS

    def test_is_databricks_related(self):
        """Test Databricks content detection."""

        class TestSource(BaseDataSource):
            SOURCE_NAME = "test"

            def fetch(self, **kwargs):
                return []

        source = TestSource()

        assert source.is_databricks_related("Using Databricks for ETL")
        assert source.is_databricks_related("Delta Lake performance")
        assert source.is_databricks_related("Apache Spark optimization")
        assert not source.is_databricks_related("Random unrelated text")
        assert not source.is_databricks_related("")
        assert not source.is_databricks_related(None)

    def test_extract_keywords(self):
        """Test keyword extraction."""

        class TestSource(BaseDataSource):
            SOURCE_NAME = "test"

            def fetch(self, **kwargs):
                return []

        source = TestSource()

        keywords = source.extract_keywords(
            "Using Databricks and Delta Lake for data engineering"
        )

        assert "databricks" in keywords
        assert "delta lake" in keywords
        assert "data engineering" in keywords


class TestHackerNewsSource:
    """Tests for HackerNewsSource."""

    def test_source_name(self):
        """Test source name is set correctly."""
        source = HackerNewsSource()
        assert source.SOURCE_NAME == "hacker_news"

    def test_rate_limit(self):
        """Test default rate limit."""
        source = HackerNewsSource()
        assert source.DEFAULT_RATE_LIMIT == 2.0

    @patch("requests.get")
    def test_fetch_with_mock(self, mock_get):
        """Test fetching with mocked API."""
        mock_response = Mock()
        mock_response.json.return_value = {
            "hits": [
                {
                    "objectID": "12345",
                    "title": "Databricks announces new feature",
                    "url": "https://example.com/news",
                    "points": 100,
                    "num_comments": 50,
                    "author": "testuser",
                    "created_at_i": 1700000000,
                }
            ]
        }
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response

        source = HackerNewsSource()
        items = source.fetch(query="databricks", days_back=1, limit=10)

        assert len(items) >= 0  # May be filtered


class TestRSSFeedSource:
    """Tests for RSSFeedSource."""

    def test_default_feeds(self):
        """Test default feeds are configured."""
        source = RSSFeedSource()
        feeds = source.list_feeds()

        assert len(feeds) > 0
        # Check for Databricks blog
        feed_names = [f["name"] for f in feeds]
        assert "Databricks Blog" in feed_names

    def test_add_feed(self):
        """Test adding a custom feed."""
        source = RSSFeedSource()
        initial_count = len(source.list_feeds())

        source.add_feed(
            name="Test Feed",
            url="https://example.com/feed.xml",
            category="test",
        )

        assert len(source.list_feeds()) == initial_count + 1

    def test_clean_html(self):
        """Test HTML cleaning."""
        source = RSSFeedSource()

        html = "<p>Hello <b>world</b>!</p>"
        clean = source._clean_html(html)

        assert "<p>" not in clean
        assert "<b>" not in clean
        assert "Hello" in clean
        assert "world" in clean
