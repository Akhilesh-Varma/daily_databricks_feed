"""Tests for podcast generation and publishing modules."""

import pytest
from datetime import datetime, timezone
from unittest.mock import Mock, patch

from daily_databricks_feed.podcast.tts_generator import (
    TTSGenerator,
    MockTTSGenerator,
    VoiceConfig,
    VOICES,
)
from daily_databricks_feed.podcast.rss_publisher import (
    RSSPublisher,
    PodcastFeed,
    PodcastEpisode,
    create_default_feed,
)


class TestVoiceConfig:
    """Tests for VoiceConfig."""

    def test_to_dict(self):
        """Test voice config to dict conversion."""
        voice = VoiceConfig(
            name="en-US-Neural2-F",
            language_code="en-US",
            ssml_gender="FEMALE",
        )

        result = voice.to_dict()

        assert result["name"] == "en-US-Neural2-F"
        assert result["languageCode"] == "en-US"
        assert result["ssmlGender"] == "FEMALE"

    def test_predefined_voices(self):
        """Test predefined voice configurations."""
        assert "female_host" in VOICES
        assert "male_host" in VOICES
        assert VOICES["female_host"].ssml_gender == "FEMALE"
        assert VOICES["male_host"].ssml_gender == "MALE"


class TestMockTTSGenerator:
    """Tests for MockTTSGenerator."""

    @pytest.fixture
    def mock_tts(self):
        """Create a mock TTS generator."""
        return MockTTSGenerator()

    @pytest.fixture
    def sample_script(self):
        """Create a sample script for testing."""
        return {
            "episode_date": "2024-01-15",
            "title": "Test Episode",
            "intro": "Welcome to the show! Today we have great content.",
            "stories": [
                {"title": "Story 1", "content": "First story about Databricks.", "source": "test"},
                {"title": "Story 2", "content": "Second story about Delta Lake.", "source": "test"},
            ],
            "outro": "Thanks for listening! See you next time.",
        }

    def test_is_available(self, mock_tts):
        """Test mock TTS is always available."""
        assert mock_tts.is_available()

    def test_generate_podcast(self, mock_tts, sample_script):
        """Test generating podcast with mock TTS."""
        result = mock_tts.generate_podcast(sample_script)

        assert result.episode_date == "2024-01-15"
        assert result.title == "Test Episode"
        assert result.duration_seconds > 0
        assert result.file_size_bytes > 0
        assert len(result.segments) == 4  # intro + 2 stories + outro

    def test_alternating_voices(self, mock_tts, sample_script):
        """Test that voices alternate between stories."""
        # Add more stories
        sample_script["stories"] = [
            {"title": f"Story {i}", "content": f"Content {i}", "source": "test"}
            for i in range(4)
        ]

        result = mock_tts.generate_podcast(sample_script)

        # Get story segments (exclude intro and outro)
        story_segments = [s for s in result.segments if s["type"] == "story"]

        # Check voice alternation
        voices = [s["voice"] for s in story_segments]
        assert len(set(voices)) == 2  # Should have 2 different voices

    def test_wrap_ssml(self, mock_tts):
        """Test SSML wrapping."""
        text = "Hello world"
        ssml = mock_tts._wrap_ssml(text, add_pause_after=True)

        assert "<speak>" in ssml
        assert "</speak>" in ssml
        assert "Hello world" in ssml
        assert "<break" in ssml

    def test_estimate_duration(self, mock_tts):
        """Test duration estimation."""
        short_text = "Hello"
        long_text = "Hello " * 100

        short_duration = mock_tts._estimate_duration(short_text)
        long_duration = mock_tts._estimate_duration(long_text)

        assert long_duration > short_duration
        assert short_duration > 0


class TestPodcastEpisode:
    """Tests for PodcastEpisode."""

    def test_to_dict(self):
        """Test episode to dict conversion."""
        episode = PodcastEpisode(
            guid="test-123",
            title="Test Episode",
            description="Test description",
            audio_url="https://example.com/audio.mp3",
            audio_size_bytes=1000000,
            duration_seconds=300,
            published_at=datetime(2024, 1, 15, tzinfo=timezone.utc),
            episode_number=1,
            keywords=["databricks", "spark"],
        )

        result = episode.to_dict()

        assert result["guid"] == "test-123"
        assert result["title"] == "Test Episode"
        assert result["duration_seconds"] == 300
        assert result["episode_number"] == 1


class TestPodcastFeed:
    """Tests for PodcastFeed."""

    def test_to_dict(self):
        """Test feed to dict conversion."""
        feed = PodcastFeed(
            title="Test Podcast",
            description="A test podcast",
            author="Test Author",
            email="test@example.com",
            website_url="https://example.com",
            feed_url="https://example.com/feed.xml",
            image_url="https://example.com/image.jpg",
        )

        result = feed.to_dict()

        assert result["title"] == "Test Podcast"
        assert result["author"] == "Test Author"
        assert result["episode_count"] == 0


class TestRSSPublisher:
    """Tests for RSSPublisher."""

    @pytest.fixture
    def publisher(self):
        """Create a publisher without GCS."""
        return RSSPublisher()

    @pytest.fixture
    def sample_feed(self):
        """Create a sample feed for testing."""
        return create_default_feed(
            base_url="https://example.com/podcast",
            author="Test Author",
            email="test@example.com",
        )

    @pytest.fixture
    def sample_episode(self):
        """Create a sample episode."""
        return PodcastEpisode(
            guid="test-123",
            title="Test Episode",
            description="Test description with details.",
            audio_url="https://example.com/audio.mp3",
            audio_size_bytes=5000000,
            duration_seconds=300,
            published_at=datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc),
            episode_number=1,
            keywords=["databricks", "podcast"],
        )

    def test_generate_rss(self, publisher, sample_feed, sample_episode):
        """Test RSS generation."""
        sample_feed.episodes.append(sample_episode)

        rss_xml = publisher.generate_rss(sample_feed)

        # Check XML structure
        assert '<?xml version="1.0"' in rss_xml
        assert "<rss" in rss_xml
        assert "<channel>" in rss_xml
        assert "<item>" in rss_xml
        assert "Test Podcast" in rss_xml
        assert "Test Episode" in rss_xml

    def test_rss_has_itunes_tags(self, publisher, sample_feed, sample_episode):
        """Test that RSS has iTunes tags."""
        sample_feed.episodes.append(sample_episode)

        rss_xml = publisher.generate_rss(sample_feed)

        assert "itunes" in rss_xml.lower()
        assert "<itunes:author>" in rss_xml or "itunes:author" in rss_xml

    def test_format_duration(self, publisher):
        """Test duration formatting."""
        assert publisher._format_duration(65) == "01:05"
        assert publisher._format_duration(3665) == "01:01:05"
        assert publisher._format_duration(300) == "05:00"

    def test_format_rfc822(self, publisher):
        """Test RFC 822 date formatting."""
        dt = datetime(2024, 1, 15, 12, 30, 0, tzinfo=timezone.utc)
        formatted = publisher._format_rfc822(dt)

        assert "Mon" in formatted
        assert "15" in formatted
        assert "Jan" in formatted
        assert "2024" in formatted

    def test_create_default_feed(self):
        """Test default feed creation."""
        feed = create_default_feed(
            base_url="https://storage.example.com/podcast",
            author="Podcast Team",
            email="podcast@example.com",
        )

        assert feed.title == "Daily Databricks Digest"
        assert feed.author == "Podcast Team"
        assert "databricks" in feed.description.lower()
        assert feed.feed_url == "https://storage.example.com/podcast/feed.xml"


class TestRSSPublisherLocalSave:
    """Tests for local file operations."""

    def test_save_feed_locally(self, tmp_path):
        """Test saving feed to local file."""
        publisher = RSSPublisher()
        feed = create_default_feed(base_url="https://example.com")

        rss_xml = publisher.generate_rss(feed)
        filepath = tmp_path / "feed.xml"

        result = publisher.save_feed_locally(rss_xml, str(filepath))

        assert filepath.exists()
        content = filepath.read_text()
        assert "Daily Databricks Digest" in content
