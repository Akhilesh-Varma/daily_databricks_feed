"""RSS feed publisher for podcast distribution."""

import os
import json
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional
from dataclasses import dataclass, field
import logging
import hashlib

logger = logging.getLogger(__name__)

# Google Cloud Storage is optional
try:
    from google.cloud import storage

    GCS_AVAILABLE = True
except ImportError:
    GCS_AVAILABLE = False


@dataclass
class PodcastEpisode:
    """Metadata for a single podcast episode."""

    guid: str
    title: str
    description: str
    audio_url: str
    audio_size_bytes: int
    duration_seconds: int
    published_at: datetime
    episode_number: int = 0
    season_number: int = 1
    explicit: bool = False
    keywords: List[str] = field(default_factory=list)
    image_url: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "guid": self.guid,
            "title": self.title,
            "description": self.description,
            "audio_url": self.audio_url,
            "audio_size_bytes": self.audio_size_bytes,
            "duration_seconds": self.duration_seconds,
            "published_at": self.published_at.isoformat(),
            "episode_number": self.episode_number,
            "season_number": self.season_number,
            "explicit": self.explicit,
            "keywords": self.keywords,
            "image_url": self.image_url,
        }


@dataclass
class PodcastFeed:
    """Podcast feed metadata."""

    title: str
    description: str
    author: str
    email: str
    website_url: str
    feed_url: str
    image_url: str
    language: str = "en-us"
    category: str = "Technology"
    subcategory: str = "Tech News"
    explicit: bool = False
    copyright: str = ""
    episodes: List[PodcastEpisode] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "title": self.title,
            "description": self.description,
            "author": self.author,
            "email": self.email,
            "website_url": self.website_url,
            "feed_url": self.feed_url,
            "image_url": self.image_url,
            "language": self.language,
            "category": self.category,
            "subcategory": self.subcategory,
            "explicit": self.explicit,
            "copyright": self.copyright,
            "episode_count": len(self.episodes),
        }


class RSSPublisher:
    """
    Generate and publish RSS 2.0 feeds with iTunes podcast extensions.

    Supports:
    - RSS 2.0 with iTunes/Apple Podcasts extensions
    - Spotify podcast requirements
    - Google Podcasts compatibility
    - Upload to Google Cloud Storage
    """

    # RSS namespaces
    ITUNES_NS = "http://www.itunes.com/dtds/podcast-1.0.dtd"
    CONTENT_NS = "http://purl.org/rss/1.0/modules/content/"
    ATOM_NS = "http://www.w3.org/2005/Atom"

    def __init__(
        self,
        bucket_name: Optional[str] = None,
        credentials_json: Optional[str] = None,
        base_url: Optional[str] = None,
    ):
        """
        Initialize RSS publisher.

        Args:
            bucket_name: GCS bucket name for audio files
            credentials_json: GCP service account JSON
            base_url: Base URL for audio files (e.g., https://storage.googleapis.com/bucket)
        """
        self.bucket_name = bucket_name or os.environ.get("GCS_BUCKET_NAME")
        self.credentials_json = credentials_json or os.environ.get("GCP_SERVICE_ACCOUNT_JSON")
        self.base_url = base_url or os.environ.get("AUDIO_BASE_URL", "")

        self._gcs_client = None

    def _get_gcs_client(self):
        """Get or create GCS client."""
        if not GCS_AVAILABLE:
            raise ImportError(
                "google-cloud-storage not installed. " "Run: pip install google-cloud-storage"
            )

        if self._gcs_client is None:
            if self.credentials_json:
                from google.oauth2 import service_account

                creds_dict = json.loads(self.credentials_json)
                credentials = service_account.Credentials.from_service_account_info(creds_dict)
                self._gcs_client = storage.Client(credentials=credentials)
            else:
                self._gcs_client = storage.Client()

        return self._gcs_client

    def generate_rss(self, feed: PodcastFeed) -> str:
        """
        Generate RSS 2.0 XML with iTunes extensions.

        Args:
            feed: PodcastFeed with episodes

        Returns:
            RSS XML string
        """
        # Register namespaces
        ET.register_namespace("itunes", self.ITUNES_NS)
        ET.register_namespace("content", self.CONTENT_NS)
        ET.register_namespace("atom", self.ATOM_NS)

        # Create root element
        rss = ET.Element("rss")
        rss.set("version", "2.0")
        rss.set(f"xmlns:itunes", self.ITUNES_NS)
        rss.set(f"xmlns:content", self.CONTENT_NS)
        rss.set(f"xmlns:atom", self.ATOM_NS)

        channel = ET.SubElement(rss, "channel")

        # Required channel elements
        ET.SubElement(channel, "title").text = feed.title
        ET.SubElement(channel, "description").text = feed.description
        ET.SubElement(channel, "link").text = feed.website_url
        ET.SubElement(channel, "language").text = feed.language
        ET.SubElement(channel, "copyright").text = (
            feed.copyright or f"Copyright {datetime.now().year}"
        )
        ET.SubElement(channel, "lastBuildDate").text = self._format_rfc822(
            datetime.now(timezone.utc)
        )

        # Atom self link (required by some validators)
        atom_link = ET.SubElement(channel, f"{{{self.ATOM_NS}}}link")
        atom_link.set("href", feed.feed_url)
        atom_link.set("rel", "self")
        atom_link.set("type", "application/rss+xml")

        # iTunes-specific channel elements
        ET.SubElement(channel, f"{{{self.ITUNES_NS}}}author").text = feed.author
        ET.SubElement(channel, f"{{{self.ITUNES_NS}}}summary").text = feed.description
        ET.SubElement(channel, f"{{{self.ITUNES_NS}}}explicit").text = (
            "yes" if feed.explicit else "no"
        )
        ET.SubElement(channel, f"{{{self.ITUNES_NS}}}type").text = "episodic"

        # Owner info
        owner = ET.SubElement(channel, f"{{{self.ITUNES_NS}}}owner")
        ET.SubElement(owner, f"{{{self.ITUNES_NS}}}name").text = feed.author
        ET.SubElement(owner, f"{{{self.ITUNES_NS}}}email").text = feed.email

        # Image
        image = ET.SubElement(channel, "image")
        ET.SubElement(image, "url").text = feed.image_url
        ET.SubElement(image, "title").text = feed.title
        ET.SubElement(image, "link").text = feed.website_url

        itunes_image = ET.SubElement(channel, f"{{{self.ITUNES_NS}}}image")
        itunes_image.set("href", feed.image_url)

        # Category
        category = ET.SubElement(channel, f"{{{self.ITUNES_NS}}}category")
        category.set("text", feed.category)
        if feed.subcategory:
            subcategory = ET.SubElement(category, f"{{{self.ITUNES_NS}}}category")
            subcategory.set("text", feed.subcategory)

        # Episodes
        for episode in feed.episodes:
            self._add_episode(channel, episode)

        # Generate XML string with declaration
        xml_str = ET.tostring(rss, encoding="unicode", method="xml")
        return f'<?xml version="1.0" encoding="UTF-8"?>\n{xml_str}'

    def _add_episode(self, channel: ET.Element, episode: PodcastEpisode) -> None:
        """Add an episode item to the channel."""
        item = ET.SubElement(channel, "item")

        # Required elements
        ET.SubElement(item, "title").text = episode.title
        ET.SubElement(item, "description").text = episode.description
        ET.SubElement(item, f"{{{self.CONTENT_NS}}}encoded").text = (
            f"<![CDATA[{episode.description}]]>"
        )
        ET.SubElement(item, "pubDate").text = self._format_rfc822(episode.published_at)
        ET.SubElement(item, "guid").text = episode.guid

        # Enclosure (the audio file)
        enclosure = ET.SubElement(item, "enclosure")
        enclosure.set("url", episode.audio_url)
        enclosure.set("length", str(episode.audio_size_bytes))
        enclosure.set("type", "audio/mpeg")

        # iTunes elements
        ET.SubElement(item, f"{{{self.ITUNES_NS}}}title").text = episode.title
        ET.SubElement(item, f"{{{self.ITUNES_NS}}}summary").text = episode.description
        ET.SubElement(item, f"{{{self.ITUNES_NS}}}duration").text = self._format_duration(
            episode.duration_seconds
        )
        ET.SubElement(item, f"{{{self.ITUNES_NS}}}explicit").text = (
            "yes" if episode.explicit else "no"
        )
        ET.SubElement(item, f"{{{self.ITUNES_NS}}}episodeType").text = "full"

        if episode.episode_number > 0:
            ET.SubElement(item, f"{{{self.ITUNES_NS}}}episode").text = str(episode.episode_number)
        if episode.season_number > 0:
            ET.SubElement(item, f"{{{self.ITUNES_NS}}}season").text = str(episode.season_number)

        if episode.image_url:
            itunes_image = ET.SubElement(item, f"{{{self.ITUNES_NS}}}image")
            itunes_image.set("href", episode.image_url)

        if episode.keywords:
            ET.SubElement(item, f"{{{self.ITUNES_NS}}}keywords").text = ",".join(episode.keywords)

    def _format_rfc822(self, dt: datetime) -> str:
        """Format datetime as RFC 822 string."""
        return dt.strftime("%a, %d %b %Y %H:%M:%S %z")

    def _format_duration(self, seconds: int) -> str:
        """Format duration as HH:MM:SS."""
        hours = seconds // 3600
        minutes = (seconds % 3600) // 60
        secs = seconds % 60

        if hours > 0:
            return f"{hours:02d}:{minutes:02d}:{secs:02d}"
        else:
            return f"{minutes:02d}:{secs:02d}"

    def upload_audio(
        self,
        audio_data: bytes,
        filename: str,
        content_type: str = "audio/mpeg",
    ) -> str:
        """
        Upload audio file to GCS.

        Args:
            audio_data: Audio file bytes
            filename: Destination filename
            content_type: MIME type

        Returns:
            Public URL of uploaded file
        """
        if not self.bucket_name:
            raise ValueError("GCS bucket name not configured")

        client = self._get_gcs_client()
        bucket = client.bucket(self.bucket_name)
        blob = bucket.blob(f"episodes/{filename}")

        blob.upload_from_string(audio_data, content_type=content_type)

        # Make publicly accessible
        blob.make_public()

        return blob.public_url

    def upload_feed(self, feed_xml: str, filename: str = "feed.xml") -> str:
        """
        Upload RSS feed to GCS.

        Args:
            feed_xml: RSS XML string
            filename: Destination filename

        Returns:
            Public URL of uploaded feed
        """
        if not self.bucket_name:
            raise ValueError("GCS bucket name not configured")

        client = self._get_gcs_client()
        bucket = client.bucket(self.bucket_name)
        blob = bucket.blob(filename)

        blob.upload_from_string(
            feed_xml.encode("utf-8"),
            content_type="application/rss+xml",
        )

        # Make publicly accessible
        blob.make_public()

        return blob.public_url

    def publish_episode(
        self,
        audio_data: bytes,
        title: str,
        description: str,
        duration_seconds: int,
        episode_date: Optional[datetime] = None,
        episode_number: int = 0,
        keywords: Optional[List[str]] = None,
    ) -> PodcastEpisode:
        """
        Publish a new episode: upload audio and create episode metadata.

        Args:
            audio_data: MP3 audio bytes
            title: Episode title
            description: Episode description
            duration_seconds: Duration in seconds
            episode_date: Publication date (defaults to now)
            episode_number: Episode number
            keywords: Episode keywords/tags

        Returns:
            PodcastEpisode with audio URL
        """
        episode_date = episode_date or datetime.now(timezone.utc)

        # Generate filename and GUID
        date_str = episode_date.strftime("%Y-%m-%d")
        filename = f"daily-databricks-{date_str}.mp3"
        guid = hashlib.md5(f"{title}-{date_str}".encode()).hexdigest()

        # Upload audio
        audio_url = self.upload_audio(audio_data, filename)

        return PodcastEpisode(
            guid=guid,
            title=title,
            description=description,
            audio_url=audio_url,
            audio_size_bytes=len(audio_data),
            duration_seconds=duration_seconds,
            published_at=episode_date,
            episode_number=episode_number,
            keywords=keywords or [],
        )

    def load_feed(self, feed_url_or_path: str) -> Optional[PodcastFeed]:
        """
        Load existing feed from URL or local path.

        Args:
            feed_url_or_path: URL or file path to RSS feed

        Returns:
            PodcastFeed or None if not found
        """
        # This is a simplified loader - in production, parse the XML fully
        logger.info(f"Loading feed from {feed_url_or_path}")
        return None  # Implement full XML parsing if needed

    def save_feed_locally(self, feed_xml: str, filepath: str) -> str:
        """
        Save RSS feed to local file.

        Args:
            feed_xml: RSS XML string
            filepath: Local file path

        Returns:
            File path
        """
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(feed_xml)
        return filepath


def create_default_feed(
    base_url: str,
    author: str = "Daily Databricks Team",
    email: str = "podcast@example.com",
) -> PodcastFeed:
    """
    Create a default podcast feed configuration.

    Args:
        base_url: Base URL for the podcast (e.g., https://storage.googleapis.com/bucket)
        author: Podcast author name
        email: Contact email

    Returns:
        Configured PodcastFeed
    """
    return PodcastFeed(
        title="Daily Databricks Digest",
        description=(
            "Your daily 5-minute update on Databricks, data engineering, "
            "and lakehouse news. Stay informed about the latest in "
            "Apache Spark, Delta Lake, MLflow, and the data ecosystem."
        ),
        author=author,
        email=email,
        website_url=base_url,
        feed_url=f"{base_url}/feed.xml",
        image_url=f"{base_url}/cover.jpg",
        language="en-us",
        category="Technology",
        subcategory="Tech News",
        explicit=False,
        copyright=f"Copyright {datetime.now().year} {author}",
        episodes=[],
    )
