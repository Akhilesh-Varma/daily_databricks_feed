"""RSS Feed data source using feedparser."""

from datetime import datetime, timezone, timedelta
from typing import List, Optional
from time import mktime
import hashlib

from .base import BaseDataSource, NewsItem, rate_limited

# feedparser is optional - gracefully handle if not installed
try:
    import feedparser

    FEEDPARSER_AVAILABLE = True
except ImportError:
    FEEDPARSER_AVAILABLE = False
    feedparser = None


class RSSFeedSource(BaseDataSource):
    """
    Fetch news from RSS/Atom feeds using feedparser.

    No authentication required - unlimited requests.
    Rate limit is set conservatively to be a good citizen.
    """

    SOURCE_NAME = "rss_feed"
    DEFAULT_RATE_LIMIT = 1.0  # 1 request per second

    # Default RSS feeds for Databricks content
    DEFAULT_FEEDS = [
        # ── Official Databricks & ecosystem ──────────────────────────────────
        {
            "name": "Databricks Blog",
            "url": "https://www.databricks.com/blog/feed",
            "category": "official",
        },
        {
            "name": "Delta Lake Blog",
            "url": "https://delta.io/blog/feed.xml",
            "category": "official",
        },
        {
            "name": "MLflow Blog",
            "url": "https://mlflow.org/blog/feed.xml",
            "category": "official",
        },
        {
            "name": "Apache Spark News",
            "url": "https://spark.apache.org/news/feed.xml",
            "category": "official",
        },
        # ── Medium — Databricks & data engineering tags ───────────────────────
        {
            "name": "Medium — Databricks",
            "url": "https://medium.com/feed/tag/databricks",
            "category": "community",
        },
        {
            "name": "Medium — Apache Spark",
            "url": "https://medium.com/feed/tag/apache-spark",
            "category": "community",
        },
        {
            "name": "Medium — Delta Lake",
            "url": "https://medium.com/feed/tag/delta-lake",
            "category": "community",
        },
        {
            "name": "Medium — Data Engineering",
            "url": "https://medium.com/feed/tag/data-engineering",
            "category": "community",
        },
        {
            "name": "Towards Data Science",
            "url": "https://towardsdatascience.com/feed",
            "category": "community",
        },
        # ── Research — arXiv (Atom feeds parsed by feedparser) ───────────────
        {
            "name": "arXiv — Databases & Data Engineering (cs.DB)",
            "url": "https://export.arxiv.org/rss/cs.DB",
            "category": "research",
        },
        {
            "name": "arXiv — Distributed & Parallel Computing (cs.DC)",
            "url": "https://export.arxiv.org/rss/cs.DC",
            "category": "research",
        },
        # ── Newsletters ───────────────────────────────────────────────────────
        {
            "name": "Data Engineering Weekly",
            "url": "https://www.dataengineeringweekly.com/feed",
            "category": "newsletter",
        },
        {
            "name": "The Sequence (AI/ML newsletter)",
            "url": "https://thesequence.substack.com/feed",
            "category": "newsletter",
        },
    ]

    def __init__(self, feeds: Optional[List[dict]] = None, **kwargs):
        """
        Initialize RSS Feed data source.

        Args:
            feeds: List of feed configs with 'name', 'url', and optional 'category'
        """
        super().__init__(**kwargs)

        if not FEEDPARSER_AVAILABLE:
            self.logger.warning("feedparser not installed. RSS source will be disabled.")
            self.feeds = []
            return

        self.feeds = feeds or self.DEFAULT_FEEDS

    def is_available(self) -> bool:
        """Check if RSS parsing is available."""
        return FEEDPARSER_AVAILABLE and len(self.feeds) > 0

    def fetch(
        self,
        feeds: Optional[List[dict]] = None,
        days_back: int = 7,
        since_epoch: Optional[int] = None,
        limit: int = 100,
        filter_databricks: bool = True,
    ) -> List[NewsItem]:
        """
        Fetch articles from RSS feeds.

        Args:
            feeds: Optional list of feed configs to override defaults
            days_back: Number of days to look back (used only when since_epoch is None)
            since_epoch: Unix epoch of the earliest allowed publish time.
                         When set, takes precedence over days_back so that the
                         PySpark checkpoint boundary is used exactly.
            limit: Maximum number of items to return
            filter_databricks: Whether to filter for Databricks-related content

        Returns:
            List of NewsItem objects
        """
        if not self.is_available():
            self.logger.warning("RSS parsing not available, returning empty list")
            return []

        feeds_to_fetch = feeds or self.feeds
        # since_epoch takes precedence — use checkpoint boundary directly
        if since_epoch is not None:
            cutoff_time = datetime.fromtimestamp(since_epoch, tz=timezone.utc)
        else:
            cutoff_time = datetime.now(timezone.utc) - timedelta(days=days_back)
        all_items = []

        for feed_config in feeds_to_fetch:
            try:
                items = self._fetch_feed(
                    url=feed_config["url"],
                    name=feed_config.get("name", "Unknown"),
                    category=feed_config.get("category", "general"),
                    cutoff_time=cutoff_time,
                )
                all_items.extend(items)
            except Exception as e:
                self.logger.error(f"Error fetching feed {feed_config.get('name')}: {e}")

        # Deduplicate by URL (RSS feeds often have duplicate content)
        seen_urls = set()
        unique_items = []
        for item in all_items:
            url_normalized = item.url.lower().rstrip("/")
            if url_normalized not in seen_urls:
                seen_urls.add(url_normalized)
                unique_items.append(item)

        # Sort by publish date descending
        unique_items.sort(
            key=lambda x: x.published_at or datetime.min.replace(tzinfo=timezone.utc),
            reverse=True,
        )

        # Apply Databricks filter if requested.
        # Official and newsletter feeds are trusted curators — always include.
        # Research papers are arXiv-selected — include if any keyword matches.
        # Community / Medium feeds require a keyword match.
        if filter_databricks:
            trusted_categories = {"official", "newsletter"}
            filtered = []
            for item in unique_items:
                cat = item.metadata.get("category", "")
                if cat in trusted_categories:
                    filtered.append(item)
                elif self.is_databricks_related(f"{item.title} {item.content or ''}"):
                    filtered.append(item)
            unique_items = filtered

        return unique_items[:limit]

    @rate_limited
    def _fetch_feed(
        self,
        url: str,
        name: str,
        category: str,
        cutoff_time: datetime,
    ) -> List[NewsItem]:
        """
        Fetch and parse a single RSS feed.

        Args:
            url: Feed URL
            name: Feed name for logging
            category: Feed category
            cutoff_time: Oldest allowed article time

        Returns:
            List of NewsItem objects
        """
        self.logger.debug(f"Fetching feed: {name} ({url})")

        feed = feedparser.parse(url)

        if feed.bozo and feed.bozo_exception:
            self.logger.warning(f"Feed parsing warning for {name}: {feed.bozo_exception}")

        items = []
        for entry in feed.entries:
            try:
                item = self._parse_entry(entry, name, category, cutoff_time)
                if item:
                    items.append(item)
            except Exception as e:
                self.logger.warning(f"Error parsing entry from {name}: {e}")

        self.logger.info(f"Fetched {len(items)} items from {name}")
        return items

    def _parse_entry(
        self,
        entry: dict,
        feed_name: str,
        category: str,
        cutoff_time: datetime,
    ) -> Optional[NewsItem]:
        """
        Parse a single RSS entry into a NewsItem.

        Args:
            entry: feedparser entry dict
            feed_name: Name of the source feed
            category: Feed category
            cutoff_time: Oldest allowed time

        Returns:
            NewsItem or None if filtered out
        """
        # Parse publish time
        published_at = None
        time_struct = entry.get("published_parsed") or entry.get("updated_parsed")
        if time_struct:
            try:
                published_at = datetime.fromtimestamp(mktime(time_struct), tz=timezone.utc)
            except (ValueError, OverflowError):
                pass

        # Filter by time
        if published_at and published_at < cutoff_time:
            return None

        # Get URL
        url = entry.get("link", "")
        if not url:
            return None

        # Generate unique ID from URL
        url_hash = hashlib.md5(url.encode()).hexdigest()[:12]
        item_id = f"rss_{url_hash}"

        # Get content (prefer summary, fall back to content)
        content = None
        if entry.get("summary"):
            content = self._clean_html(entry["summary"])
        elif entry.get("content"):
            # content is a list of dicts with 'value' key
            content_list = entry.get("content", [])
            if content_list and isinstance(content_list, list):
                content = self._clean_html(content_list[0].get("value", ""))

        # Get author
        author = entry.get("author")
        if not author and entry.get("authors"):
            authors = entry.get("authors", [])
            if authors and isinstance(authors, list):
                author = authors[0].get("name")

        # Get title
        title = entry.get("title", "Untitled")

        # Get tags from entry
        entry_tags = []
        if entry.get("tags"):
            for tag in entry.get("tags", []):
                if isinstance(tag, dict) and tag.get("term"):
                    entry_tags.append(tag["term"].lower())

        # Extract Databricks keywords
        searchable_text = f"{title} {content or ''}"
        databricks_tags = self.extract_keywords(searchable_text)

        return NewsItem(
            id=item_id,
            source=self.SOURCE_NAME,
            title=title,
            url=url,
            content=content,
            author=author,
            published_at=published_at,
            score=0,  # RSS feeds don't have scores
            comments_count=0,
            tags=list(set(entry_tags + databricks_tags)),
            metadata={
                "feed_name": feed_name,
                "category": category,
                "guid": entry.get("id") or entry.get("guid"),
            },
        )

    def _clean_html(self, html_content: str) -> str:
        """
        Remove HTML tags from content.

        Args:
            html_content: HTML string

        Returns:
            Plain text content
        """
        import re

        if not html_content:
            return ""

        # Remove HTML tags
        text = re.sub(r"<[^>]+>", " ", html_content)
        # Remove extra whitespace
        text = re.sub(r"\s+", " ", text)
        # Decode HTML entities
        text = text.replace("&nbsp;", " ")
        text = text.replace("&amp;", "&")
        text = text.replace("&lt;", "<")
        text = text.replace("&gt;", ">")
        text = text.replace("&quot;", '"')
        text = text.replace("&#39;", "'")

        return text.strip()

    def add_feed(self, name: str, url: str, category: str = "custom") -> None:
        """
        Add a new feed to the list.

        Args:
            name: Feed name
            url: Feed URL
            category: Feed category
        """
        self.feeds.append({"name": name, "url": url, "category": category})

    def list_feeds(self) -> List[dict]:
        """
        Get list of configured feeds.

        Returns:
            List of feed configurations
        """
        return self.feeds.copy()
