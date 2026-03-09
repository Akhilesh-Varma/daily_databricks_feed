"""Tests for script generation module."""

import pytest
from datetime import datetime, timezone
from unittest.mock import Mock, patch

from daily_databricks_feed.aggregation.script_generator import (
    ScriptGenerator,
    PodcastScript,
    select_top_stories,
    FallbackProvider,
)


class TestScriptGenerator:
    """Tests for ScriptGenerator class."""

    @pytest.fixture
    def generator(self):
        """Create a script generator with no API keys (uses fallback)."""
        return ScriptGenerator()

    @pytest.fixture
    def sample_stories(self):
        """Create sample stories for testing."""
        return [
            {
                "title": "Databricks Launches New Feature",
                "content": "Databricks announced a new feature for data processing.",
                "source": "hacker_news",
                "url": "https://example.com/1",
                "quality_score": 0.8,
            },
            {
                "title": "Delta Lake 3.0 Released",
                "content": "The new version of Delta Lake brings performance improvements.",
                "source": "rss_feed",
                "url": "https://example.com/2",
                "quality_score": 0.7,
            },
            {
                "title": "Apache Spark Tips and Tricks",
                "content": "Learn how to optimize your Spark jobs for better performance.",
                "source": "youtube",
                "url": "https://example.com/3",
                "quality_score": 0.6,
            },
        ]

    def test_get_available_provider(self, generator):
        """Test that fallback provider is returned when no API keys."""
        provider = generator.get_available_provider()
        assert isinstance(provider, FallbackProvider)

    def test_generate_script_with_fallback(self, generator, sample_stories):
        """Test script generation with fallback provider."""
        script = generator.generate_script(
            stories=sample_stories,
            date=datetime(2024, 1, 15, tzinfo=timezone.utc),
            podcast_name="Test Podcast",
        )

        assert isinstance(script, PodcastScript)
        assert script.episode_date == "2024-01-15"
        assert script.word_count > 0
        assert script.estimated_duration_seconds > 0
        assert "Fallback" in script.metadata.get("provider", "")

    def test_script_has_required_sections(self, generator, sample_stories):
        """Test that generated script has all sections."""
        script = generator.generate_script(
            stories=sample_stories,
            podcast_name="Test Podcast",
        )

        # Script should have intro, stories, outro, and full script
        assert script.full_script is not None
        assert len(script.full_script) > 0

    def test_generate_ssml(self, generator, sample_stories):
        """Test SSML generation."""
        script = generator.generate_script(
            stories=sample_stories,
            podcast_name="Test Podcast",
        )

        ssml = generator.generate_ssml(script)

        assert "<speak>" in ssml
        assert "</speak>" in ssml
        assert "<break" in ssml or "<p>" in ssml

    def test_build_prompt(self, generator, sample_stories):
        """Test prompt building."""
        prompt = generator._build_prompt(
            stories=sample_stories,
            date_str="January 15, 2024",
            podcast_name="Daily Databricks Digest",
        )

        assert "Daily Databricks Digest" in prompt
        assert "January 15, 2024" in prompt
        assert "Databricks Launches New Feature" in prompt
        assert "INTRO" in prompt
        assert "OUTRO" in prompt


class TestPodcastScript:
    """Tests for PodcastScript dataclass."""

    def test_to_dict(self):
        """Test converting script to dictionary."""
        script = PodcastScript(
            episode_date="2024-01-15",
            title="Test Episode",
            intro="Welcome to the show!",
            stories=[{"title": "Story 1", "content": "Content 1", "source": "test"}],
            outro="Thanks for listening!",
            full_script="Full script content...",
            word_count=100,
            estimated_duration_seconds=60,
            metadata={"provider": "test"},
        )

        result = script.to_dict()

        assert result["episode_date"] == "2024-01-15"
        assert result["title"] == "Test Episode"
        assert result["word_count"] == 100
        assert len(result["stories"]) == 1


class TestSelectTopStories:
    """Tests for story selection function."""

    @pytest.fixture
    def sample_items(self):
        """Create sample items for selection."""
        return [
            {"title": "Story 1", "source": "source_a", "quality_score": 0.9},
            {"title": "Story 2", "source": "source_a", "quality_score": 0.8},
            {"title": "Story 3", "source": "source_b", "quality_score": 0.7},
            {"title": "Story 4", "source": "source_a", "quality_score": 0.6},
            {"title": "Story 5", "source": "source_c", "quality_score": 0.5},
            {"title": "Story 6", "source": "source_b", "quality_score": 0.4},
        ]

    def test_select_top_stories_limit(self, sample_items):
        """Test that selection respects limit."""
        selected = select_top_stories(sample_items, max_stories=3)
        assert len(selected) == 3

    def test_select_top_stories_quality_order(self, sample_items):
        """Test that highest quality stories are selected."""
        selected = select_top_stories(sample_items, max_stories=3, diversity_weight=0)

        # Without diversity weight, should be top 3 by quality
        scores = [s["quality_score"] for s in selected]
        assert scores == sorted(scores, reverse=True)

    def test_select_top_stories_diversity(self, sample_items):
        """Test that diversity weight affects selection."""
        # With high diversity weight, should favor different sources
        selected = select_top_stories(sample_items, max_stories=4, diversity_weight=0.5)

        sources = [s["source"] for s in selected]
        # Should have multiple different sources
        assert len(set(sources)) >= 2

    def test_select_top_stories_empty_input(self):
        """Test selection with empty input."""
        selected = select_top_stories([], max_stories=5)
        assert selected == []

    def test_select_top_stories_fewer_than_limit(self, sample_items):
        """Test selection when fewer items than limit."""
        selected = select_top_stories(sample_items[:2], max_stories=10)
        assert len(selected) == 2


class TestFallbackProvider:
    """Tests for FallbackProvider."""

    def test_generate(self):
        """Test fallback generation."""
        provider = FallbackProvider()
        result = provider.generate("Test prompt")

        assert isinstance(result, str)
        assert len(result) > 0
        assert "Daily Databricks Digest" in result or "podcast" in result.lower()
