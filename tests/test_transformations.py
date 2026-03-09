"""Tests for transformation modules."""

import pytest
from datetime import datetime, timezone

from daily_databricks_feed.data_sources.base import NewsItem
from daily_databricks_feed.transformations.bronze_to_silver import (
    BronzeToSilverTransformer,
    SilverNewsItem,
)


class TestBronzeToSilverTransformer:
    """Tests for BronzeToSilverTransformer."""

    @pytest.fixture
    def transformer(self):
        """Create a transformer instance."""
        return BronzeToSilverTransformer()

    @pytest.fixture
    def sample_news_items(self):
        """Create sample news items for testing."""
        return [
            NewsItem(
                id="item_1",
                source="hacker_news",
                title="Databricks Announces New Feature",
                url="https://example.com/article1",
                content="This is about Databricks and Delta Lake.",
                score=100,
                comments_count=50,
            ),
            NewsItem(
                id="item_2",
                source="reddit",
                title="Apache Spark Performance Tips",
                url="https://example.com/article2",
                content="Tips for optimizing Spark jobs.",
                score=50,
                comments_count=20,
            ),
            NewsItem(
                id="item_3",
                source="rss_feed",
                title="Duplicate of Article 1",
                url="https://example.com/article1",  # Same URL
                content="This is about Databricks and Delta Lake.",
                score=10,
            ),
        ]

    def test_transform_single_item(self, transformer):
        """Test transforming a single news item."""
        item = NewsItem(
            id="test_1",
            source="test",
            title="Test Article About Databricks",
            url="https://example.com/test",
            content="Using Databricks for data engineering.",
            score=42,
        )

        result = transformer.transform([item])

        assert len(result) == 1
        silver_item = result[0]
        assert silver_item.id == "test_1"
        assert silver_item.source == "test"
        assert "databricks" in silver_item.keywords
        assert silver_item.quality_score > 0

    def test_transform_extracts_keywords(self, transformer):
        """Test that keywords are extracted correctly."""
        item = NewsItem(
            id="test_1",
            source="test",
            title="Delta Lake and MLflow Tutorial",
            url="https://example.com/test",
            content="Learn about Delta Lake, MLflow, and Apache Spark.",
        )

        result = transformer.transform([item])

        keywords = result[0].keywords
        assert "delta lake" in keywords
        assert "mlflow" in keywords
        assert "spark" in keywords or "apache spark" in keywords

    def test_transform_detects_duplicates(self, transformer, sample_news_items):
        """Test that duplicates are detected."""
        result = transformer.transform(sample_news_items)

        # Should detect duplicate based on URL
        duplicates = [item for item in result if item.is_duplicate]
        non_duplicates = [item for item in result if not item.is_duplicate]

        assert len(duplicates) >= 1
        assert len(non_duplicates) >= 2

    def test_deduplicate(self, transformer, sample_news_items):
        """Test deduplication method."""
        silver_items = transformer.transform(sample_news_items)
        deduplicated = transformer.deduplicate(silver_items)

        assert len(deduplicated) < len(silver_items)
        assert all(not item.is_duplicate for item in deduplicated)

    def test_filter_by_quality(self, transformer, sample_news_items):
        """Test quality filtering."""
        silver_items = transformer.transform(sample_news_items)
        filtered = transformer.filter_by_quality(silver_items, min_quality=0.3)

        assert all(item.quality_score >= 0.3 for item in filtered)

    def test_rank_items(self, transformer, sample_news_items):
        """Test ranking by quality score."""
        silver_items = transformer.transform(sample_news_items)
        ranked = transformer.rank_items(silver_items)

        # Check items are sorted by quality score descending
        scores = [item.quality_score for item in ranked]
        assert scores == sorted(scores, reverse=True)

    def test_clean_text(self, transformer):
        """Test text cleaning."""
        html_text = "<p>Hello <b>world</b>! Check http://example.com</p>"
        cleaned = transformer._clean_text(html_text)

        assert "<p>" not in cleaned
        assert "<b>" not in cleaned
        assert "http://" not in cleaned
        assert "Hello" in cleaned
        assert "world" in cleaned

    def test_normalize_url(self, transformer):
        """Test URL normalization."""
        url1 = "https://EXAMPLE.com/article/?utm_source=test"
        url2 = "https://example.com/article"

        norm1 = transformer._normalize_url(url1)
        norm2 = transformer._normalize_url(url2)

        assert norm1 == norm2

    def test_calculate_sentiment(self, transformer):
        """Test sentiment calculation."""
        positive_text = "This is great, excellent, and amazing!"
        negative_text = "This is terrible, awful, and broken."
        neutral_text = "This is a news article about technology."

        positive_score = transformer._calculate_sentiment(positive_text)
        negative_score = transformer._calculate_sentiment(negative_text)
        neutral_score = transformer._calculate_sentiment(neutral_text)

        assert positive_score > 0
        assert negative_score < 0
        assert neutral_score == 0

    def test_quality_score_components(self, transformer):
        """Test that quality score considers various factors."""
        # High quality item
        high_quality = NewsItem(
            id="high",
            source="rss_feed",  # Official source
            title="Comprehensive Guide to Databricks Delta Lake Best Practices",
            url="https://example.com/high",
            content="Long detailed content " * 100,  # Long content
            score=500,  # High social score
            comments_count=100,
        )

        # Low quality item
        low_quality = NewsItem(
            id="low",
            source="reddit",
            title="Short",
            url="https://example.com/low",
            content="",  # No content
            score=1,
            comments_count=0,
        )

        high_result = transformer.transform([high_quality])[0]
        low_result = transformer.transform([low_quality])[0]

        assert high_result.quality_score > low_result.quality_score


class TestSilverNewsItem:
    """Tests for SilverNewsItem dataclass."""

    def test_to_dict(self):
        """Test converting silver item to dictionary."""
        now = datetime.now(timezone.utc)
        item = SilverNewsItem(
            id="test_1",
            source="test",
            title="Original Title",
            title_cleaned="original title",
            url="https://example.com",
            url_normalized="https://example.com",
            content="Content",
            content_cleaned="content",
            author="Author",
            published_at=now,
            fetched_at=now,
            score=42,
            comments_count=10,
            keywords=["databricks"],
            entities=["databricks", "spark"],
            sentiment_score=0.5,
            content_hash="abc123",
            is_duplicate=False,
            duplicate_of=None,
            quality_score=0.7,
            metadata={"key": "value"},
        )

        result = item.to_dict()

        assert result["id"] == "test_1"
        assert result["quality_score"] == 0.7
        assert result["keywords"] == ["databricks"]
        assert result["is_duplicate"] is False
