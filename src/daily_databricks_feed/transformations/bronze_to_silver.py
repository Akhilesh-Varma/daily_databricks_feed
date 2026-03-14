"""Bronze to Silver transformation - clean, deduplicate, and extract keywords."""

import re
import hashlib
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional, Set
from dataclasses import dataclass, field

from ..data_sources.base import NewsItem


@dataclass
class SilverNewsItem:
    """Cleaned and enriched news item for silver layer."""

    id: str
    source: str
    title: str
    title_cleaned: str
    url: str
    url_normalized: str
    content: Optional[str]
    content_cleaned: Optional[str]
    author: Optional[str]
    published_at: Optional[datetime]
    fetched_at: datetime
    score: int
    comments_count: int
    keywords: List[str]
    entities: List[str]
    sentiment_score: float
    content_hash: str
    is_duplicate: bool
    duplicate_of: Optional[str]
    quality_score: float
    metadata: Dict[str, Any]

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for storage."""
        return {
            "id": self.id,
            "source": self.source,
            "title": self.title,
            "title_cleaned": self.title_cleaned,
            "url": self.url,
            "url_normalized": self.url_normalized,
            "content": self.content,
            "content_cleaned": self.content_cleaned,
            "author": self.author,
            "published_at": (self.published_at.isoformat() if self.published_at else None),
            "fetched_at": self.fetched_at.isoformat(),
            "score": self.score,
            "comments_count": self.comments_count,
            "keywords": self.keywords,
            "entities": self.entities,
            "sentiment_score": self.sentiment_score,
            "content_hash": self.content_hash,
            "is_duplicate": self.is_duplicate,
            "duplicate_of": self.duplicate_of,
            "quality_score": self.quality_score,
            "metadata": self.metadata,
        }


class BronzeToSilverTransformer:
    """Transform bronze (raw) news items to silver (cleaned) items."""

    # Common tech entities to extract
    TECH_ENTITIES = [
        "databricks",
        "delta lake",
        "apache spark",
        "mlflow",
        "unity catalog",
        "aws",
        "azure",
        "gcp",
        "google cloud",
        "snowflake",
        "dbt",
        "airflow",
        "kubernetes",
        "docker",
        "python",
        "scala",
        "sql",
        "kafka",
        "flink",
        "presto",
        "trino",
        "iceberg",
        "hudi",
        "parquet",
        "avro",
        "json",
        "tensorflow",
        "pytorch",
        "openai",
        "llm",
        "machine learning",
        "deep learning",
        "ai",
        "artificial intelligence",
        "data warehouse",
        "data lake",
        "etl",
        "elt",
        "streaming",
        "batch processing",
    ]

    # Positive sentiment words for simple scoring
    POSITIVE_WORDS = [
        "great",
        "excellent",
        "amazing",
        "awesome",
        "fantastic",
        "love",
        "best",
        "good",
        "better",
        "improve",
        "fast",
        "efficient",
        "powerful",
        "innovative",
        "breakthrough",
        "success",
        "easy",
        "simple",
        "useful",
        "helpful",
    ]

    # Negative sentiment words
    NEGATIVE_WORDS = [
        "bad",
        "worst",
        "terrible",
        "awful",
        "hate",
        "slow",
        "bug",
        "broken",
        "fail",
        "failure",
        "problem",
        "issue",
        "difficult",
        "complex",
        "confusing",
        "expensive",
        "error",
        "crash",
        "disappointing",
    ]

    def __init__(self):
        """Initialize the transformer."""
        self._seen_content_hashes: Set[str] = set()
        self._hash_to_id: Dict[str, str] = {}

    def transform(self, items: List[NewsItem]) -> List[SilverNewsItem]:
        """
        Transform bronze items to silver items.

        Args:
            items: List of raw NewsItem objects

        Returns:
            List of cleaned SilverNewsItem objects
        """
        silver_items = []

        # Reset duplicate tracking for this batch
        self._seen_content_hashes = set()
        self._hash_to_id = {}

        for item in items:
            silver_item = self._transform_item(item)
            silver_items.append(silver_item)

        return silver_items

    def _transform_item(self, item: NewsItem) -> SilverNewsItem:
        """
        Transform a single bronze item to silver.

        Args:
            item: Raw NewsItem

        Returns:
            Cleaned SilverNewsItem
        """
        # Clean title
        title_cleaned = self._clean_text(item.title)

        # Normalize URL for deduplication
        url_normalized = self._normalize_url(item.url)

        # Clean content
        content_cleaned = None
        if item.content:
            content_cleaned = self._clean_text(item.content)

        # Generate content hash for deduplication
        hash_input = f"{title_cleaned}|{url_normalized}"
        content_hash = hashlib.md5(hash_input.encode()).hexdigest()

        # Check for duplicates
        is_duplicate = content_hash in self._seen_content_hashes
        duplicate_of = self._hash_to_id.get(content_hash) if is_duplicate else None

        if not is_duplicate:
            self._seen_content_hashes.add(content_hash)
            self._hash_to_id[content_hash] = item.id

        # Extract keywords
        searchable_text = f"{title_cleaned} {content_cleaned or ''}"
        keywords = self._extract_keywords(searchable_text)

        # Extract entities
        entities = self._extract_entities(searchable_text)

        # Calculate simple sentiment score
        sentiment_score = self._calculate_sentiment(searchable_text)

        # Calculate quality score
        quality_score = self._calculate_quality_score(
            item=item,
            title_cleaned=title_cleaned,
            content_cleaned=content_cleaned,
            keywords=keywords,
            is_duplicate=is_duplicate,
        )

        return SilverNewsItem(
            id=item.id,
            source=item.source,
            title=item.title,
            title_cleaned=title_cleaned,
            url=item.url,
            url_normalized=url_normalized,
            content=item.content,
            content_cleaned=content_cleaned,
            author=item.author,
            published_at=item.published_at,
            fetched_at=item.fetched_at,
            score=item.score,
            comments_count=item.comments_count,
            keywords=keywords,
            entities=entities,
            sentiment_score=sentiment_score,
            content_hash=content_hash,
            is_duplicate=is_duplicate,
            duplicate_of=duplicate_of,
            quality_score=quality_score,
            metadata=item.metadata,
        )

    def _clean_text(self, text: str) -> str:
        """
        Clean and normalize text.

        Args:
            text: Raw text

        Returns:
            Cleaned text
        """
        if not text:
            return ""

        # Remove HTML tags
        text = re.sub(r"<[^>]+>", " ", text)

        # Remove URLs
        text = re.sub(r"http[s]?://\S+", "", text)

        # Remove special characters but keep sentence structure
        text = re.sub(r"[^\w\s.,!?'-]", " ", text)

        # Normalize whitespace
        text = re.sub(r"\s+", " ", text)

        # Trim
        text = text.strip()

        return text

    def _normalize_url(self, url: str) -> str:
        """
        Normalize URL for deduplication.

        Args:
            url: Raw URL

        Returns:
            Normalized URL
        """
        if not url:
            return ""

        # Lowercase
        url = url.lower()

        # Remove trailing slash
        url = url.rstrip("/")

        # Remove common tracking parameters
        url = re.sub(r"[?&](utm_\w+|ref|source|campaign)=[^&]*", "", url)

        # Remove fragment
        url = url.split("#")[0]

        return url

    def _extract_keywords(self, text: str) -> List[str]:
        """
        Extract Databricks-related keywords from text.

        Args:
            text: Text to extract keywords from

        Returns:
            List of found keywords
        """
        if not text:
            return []

        text_lower = text.lower()

        # Keywords related to Databricks ecosystem
        keyword_list = [
            "databricks",
            "delta lake",
            "delta-lake",
            "apache spark",
            "spark",
            "mlflow",
            "unity catalog",
            "photon",
            "delta sharing",
            "delta live tables",
            "dlt",
            "lakehouse",
            "medallion",
            "data engineering",
            "data science",
            "machine learning",
            "ml",
            "ai",
            "etl",
            "elt",
            "streaming",
            "batch",
            "sql",
            "python",
            "scala",
            "notebook",
            "cluster",
            "job",
            "workflow",
            "pipeline",
        ]

        found = []
        for keyword in keyword_list:
            if keyword in text_lower:
                # Normalize similar keywords
                normalized = keyword.replace("-", " ").replace("_", " ")
                if normalized not in found:
                    found.append(normalized)

        return found

    def _extract_entities(self, text: str) -> List[str]:
        """
        Extract technology entities from text.

        Args:
            text: Text to extract entities from

        Returns:
            List of found entities
        """
        if not text:
            return []

        text_lower = text.lower()
        found = []

        for entity in self.TECH_ENTITIES:
            if entity in text_lower and entity not in found:
                found.append(entity)

        return found

    def _calculate_sentiment(self, text: str) -> float:
        """
        Calculate simple sentiment score.

        Args:
            text: Text to analyze

        Returns:
            Sentiment score from -1.0 (negative) to 1.0 (positive)
        """
        if not text:
            return 0.0

        text_lower = text.lower()
        words = text_lower.split()

        positive_count = sum(1 for word in words if word in self.POSITIVE_WORDS)
        negative_count = sum(1 for word in words if word in self.NEGATIVE_WORDS)

        total = positive_count + negative_count
        if total == 0:
            return 0.0

        return (positive_count - negative_count) / total

    def _calculate_quality_score(
        self,
        item: NewsItem,
        title_cleaned: str,
        content_cleaned: Optional[str],
        keywords: List[str],
        is_duplicate: bool,
    ) -> float:
        """
        Calculate quality score for ranking.

        Args:
            item: Original news item
            title_cleaned: Cleaned title
            content_cleaned: Cleaned content
            keywords: Extracted keywords
            is_duplicate: Whether item is a duplicate

        Returns:
            Quality score from 0.0 to 1.0
        """
        score = 0.0

        # Penalize duplicates
        if is_duplicate:
            return 0.0

        # Title quality (0.2 max)
        if len(title_cleaned) > 20:
            score += 0.1
        if len(title_cleaned) > 50:
            score += 0.1

        # Has content (0.2 max)
        if content_cleaned and len(content_cleaned) > 100:
            score += 0.1
        if content_cleaned and len(content_cleaned) > 500:
            score += 0.1

        # Has keywords (0.2 max)
        keyword_score = min(len(keywords) / 5.0, 1.0) * 0.2
        score += keyword_score

        # Social proof (0.2 max)
        if item.score > 0:
            # Log scale for score
            import math

            social_score = min(math.log10(item.score + 1) / 4.0, 1.0) * 0.1
            score += social_score
        if item.comments_count > 0:
            import math

            comments_score = min(math.log10(item.comments_count + 1) / 3.0, 1.0) * 0.1
            score += comments_score

        # Source credibility (0.2 max)
        source_scores = {
            "rss_feed": 0.2,  # Official blogs
            "hacker_news": 0.15,
            "youtube": 0.15,
            "reddit": 0.1,
        }
        score += source_scores.get(item.source, 0.1)

        return min(score, 1.0)

    def deduplicate(self, items: List[SilverNewsItem]) -> List[SilverNewsItem]:
        """
        Remove duplicate items from the list.

        Args:
            items: List of silver items

        Returns:
            Deduplicated list
        """
        return [item for item in items if not item.is_duplicate]

    def filter_by_quality(
        self, items: List[SilverNewsItem], min_quality: float = 0.3
    ) -> List[SilverNewsItem]:
        """
        Filter items by minimum quality score.

        Args:
            items: List of silver items
            min_quality: Minimum quality score threshold

        Returns:
            Filtered list
        """
        return [item for item in items if item.quality_score >= min_quality]

    def rank_items(self, items: List[SilverNewsItem]) -> List[SilverNewsItem]:
        """
        Rank items by quality score.

        Args:
            items: List of silver items

        Returns:
            Sorted list (highest quality first)
        """
        return sorted(items, key=lambda x: x.quality_score, reverse=True)
