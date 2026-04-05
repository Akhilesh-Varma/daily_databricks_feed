"""Base class for all data sources with built-in rate limiting."""

import time
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import List, Optional, Dict, Any
from functools import wraps

logger = logging.getLogger(__name__)


@dataclass
class NewsItem:
    """Represents a single news item from any source."""

    id: str
    source: str
    title: str
    url: str
    content: Optional[str] = None
    author: Optional[str] = None
    published_at: Optional[datetime] = None
    fetched_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    score: int = 0
    comments_count: int = 0
    tags: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for storage."""
        return {
            "id": self.id,
            "source": self.source,
            "title": self.title,
            "url": self.url,
            "content": self.content,
            "author": self.author,
            "published_at": (self.published_at.isoformat() if self.published_at else None),
            "fetched_at": self.fetched_at.isoformat(),
            "score": self.score,
            "comments_count": self.comments_count,
            "tags": self.tags,
            "metadata": self.metadata,
        }


class RateLimiter:
    """Token bucket rate limiter for API calls."""

    def __init__(self, requests_per_second: float):
        """
        Initialize rate limiter.

        Args:
            requests_per_second: Maximum requests allowed per second
        """
        self.requests_per_second = requests_per_second
        self.min_interval = 1.0 / requests_per_second if requests_per_second > 0 else 0
        self.last_request_time: Optional[float] = None

    def wait(self) -> None:
        """Wait if necessary to respect rate limit."""
        if self.last_request_time is not None:
            elapsed = time.time() - self.last_request_time
            if elapsed < self.min_interval:
                sleep_time = self.min_interval - elapsed
                logger.debug(f"Rate limiting: sleeping for {sleep_time:.3f}s")
                time.sleep(sleep_time)
        self.last_request_time = time.time()


def rate_limited(func):
    """Decorator to apply rate limiting to a method."""

    @wraps(func)
    def wrapper(self, *args, **kwargs):
        if hasattr(self, "_rate_limiter"):
            self._rate_limiter.wait()
        return func(self, *args, **kwargs)

    return wrapper


class BaseDataSource(ABC):
    """
    Abstract base class for all data sources.

    Provides:
    - Rate limiting
    - Retry logic with exponential backoff
    - Logging
    - Common filtering for Databricks-related content
    """

    # Override in subclasses
    SOURCE_NAME = "base"
    DEFAULT_RATE_LIMIT = 1.0  # requests per second

    # Keywords to filter for relevant content
    DATABRICKS_KEYWORDS = [
        # Databricks platform — core
        "databricks",
        "delta lake",
        "delta-lake",
        "deltalake",
        "apache spark",
        "lakehouse",
        "mlflow",
        "unity catalog",
        "databricks sql",
        "delta sharing",
        "delta live tables",
        "lakeflow",
        "photon",
        "medallion architecture",
        "dbx",
        # Databricks latest releases
        "lakebase",
        "lakebridge",
        "databricks apps",
        "databricks connect",
        "databricks serverless",
        "databricks workflows",
        "databricks marketplace",
        "databricks partner connect",
        # Pipelines — old and new nomenclature
        "delta live tables",
        "dlt",
        "lakeflow pipelines",
        "spark declarative pipelines",
        "sdp",
        "declarative pipelines",
        "serverless dlt",
        # Bundles — old and new nomenclature
        "databricks asset bundle",
        "dab",
        "databricks automation bundle",
        "declarative automation bundle",
        "declarative automation",
        "bundle deploy",
        # Vector search & AI infrastructure
        "databricks vector search",
        "mosaic vector search",
        "ai functions",
        "databricks model serving",
        "databricks playground",
        "dbrx",
        # Migration & databases
        "lakebridge",
        "databricks migration",
        "warehouse migration",
        "sql migration",
        "postgres",
        "postgresql",
        "managed postgres",
        "oltp",
        # Open table formats
        "apache iceberg",
        "iceberg",
        "apache hudi",
        "hudi",
        "apache paimon",
        "open table format",
        # AI / LLM
        "large language model",
        "llm",
        "generative ai",
        "gen ai",
        "claude",
        "anthropic",
        "gpt",
        "openai",
        "llama",
        "mistral",
        "hugging face",
        "rag",
        "retrieval augmented",
        "vector search",
        "embedding",
        "fine-tuning",
        "mosaic ai",
        "ai gateway",
        # Cloud data services
        "azure synapse",
        "azure data factory",
        "microsoft fabric",
        "fabric lakehouse",
        "synapse analytics",
        "aws glue",
        "amazon redshift",
        "bigquery",
        "snowflake",
        # Data engineering ecosystem
        "dbt",
        "apache kafka",
        "apache flink",
        "data engineering",
        "data lakehouse",
        "data mesh",
        "data catalog",
        "data governance",
    ]

    def __init__(
        self,
        rate_limit: Optional[float] = None,
        max_retries: int = 3,
        retry_delay: float = 1.0,
    ):
        """
        Initialize the data source.

        Args:
            rate_limit: Requests per second (uses DEFAULT_RATE_LIMIT if None)
            max_retries: Maximum retry attempts for failed requests
            retry_delay: Base delay between retries (exponential backoff applied)
        """
        self._rate_limiter = RateLimiter(rate_limit or self.DEFAULT_RATE_LIMIT)
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.logger = logging.getLogger(f"{__name__}.{self.SOURCE_NAME}")

    @abstractmethod
    def fetch(self, **kwargs) -> List[NewsItem]:
        """
        Fetch news items from the source.

        Returns:
            List of NewsItem objects
        """
        pass

    def fetch_with_retry(self, **kwargs) -> List[NewsItem]:
        """
        Fetch with automatic retry on failure.

        Returns:
            List of NewsItem objects
        """
        last_exception = None

        for attempt in range(self.max_retries):
            try:
                return self.fetch(**kwargs)
            except Exception as e:
                last_exception = e
                delay = self.retry_delay * (2**attempt)
                self.logger.warning(
                    f"Attempt {attempt + 1}/{self.max_retries} failed: {e}. "
                    f"Retrying in {delay:.1f}s..."
                )
                time.sleep(delay)

        self.logger.error(f"All {self.max_retries} attempts failed")
        raise last_exception

    def is_databricks_related(self, text: str) -> bool:
        """
        Check if text contains Databricks-related keywords.

        Args:
            text: Text to check (title, content, etc.)

        Returns:
            True if text contains relevant keywords
        """
        if not text:
            return False

        text_lower = text.lower()
        return any(keyword in text_lower for keyword in self.DATABRICKS_KEYWORDS)

    def filter_databricks_content(self, items: List[NewsItem]) -> List[NewsItem]:
        """
        Filter items to only include Databricks-related content.

        Args:
            items: List of NewsItem objects

        Returns:
            Filtered list containing only relevant items
        """
        filtered = []
        for item in items:
            searchable_text = f"{item.title} {item.content or ''}"
            if self.is_databricks_related(searchable_text):
                filtered.append(item)

        self.logger.info(f"Filtered {len(items)} items to {len(filtered)} Databricks-related items")
        return filtered

    def extract_keywords(self, text: str) -> List[str]:
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
        return [kw for kw in self.DATABRICKS_KEYWORDS if kw in text_lower]
