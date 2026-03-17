"""Script generator for podcast using free LLM APIs."""

import os
import json
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
import logging
import requests

logger = logging.getLogger(__name__)


@dataclass
class PodcastScript:
    """Generated podcast script with segments."""

    episode_date: str
    title: str
    intro: str
    stories: List[Dict[str, str]]  # Each story has 'title', 'content', 'source'
    outro: str
    full_script: str
    word_count: int
    estimated_duration_seconds: int
    metadata: Dict[str, Any]

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for storage."""
        return {
            "episode_date": self.episode_date,
            "title": self.title,
            "intro": self.intro,
            "stories": self.stories,
            "outro": self.outro,
            "full_script": self.full_script,
            "word_count": self.word_count,
            "estimated_duration_seconds": self.estimated_duration_seconds,
            "metadata": self.metadata,
        }


class LLMProvider:
    """Base class for LLM providers."""

    def generate(self, prompt: str, max_tokens: int = 2000) -> str:
        raise NotImplementedError


class GroqProvider(LLMProvider):
    """Groq API provider - fastest free option."""

    BASE_URL = "https://api.groq.com/openai/v1/chat/completions"

    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or os.environ.get("GROQ_API_KEY")

    def is_available(self) -> bool:
        return self.api_key is not None

    def generate(self, prompt: str, max_tokens: int = 2000) -> str:
        if not self.is_available():
            raise ValueError("Groq API key not configured")

        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }

        data = {
            "model": "llama-3.3-70b-versatile",
            "messages": [
                {
                    "role": "system",
                    "content": "You are a professional podcast script writer. Write engaging, conversational scripts for a daily tech news podcast about Databricks and data engineering.",
                },
                {"role": "user", "content": prompt},
            ],
            "max_tokens": max_tokens,
            "temperature": 0.7,
        }

        response = requests.post(self.BASE_URL, headers=headers, json=data, timeout=60)
        response.raise_for_status()

        result = response.json()
        return result["choices"][0]["message"]["content"]


class AnthropicProvider(LLMProvider):
    """Anthropic Claude API provider."""

    BASE_URL = "https://api.anthropic.com/v1/messages"

    def __init__(self, api_key: Optional[str] = None):
        self.api_key = (
            api_key or os.environ.get("CLAUDE_API_KEY") or os.environ.get("ANTHROPIC_API_KEY")
        )

    def is_available(self) -> bool:
        return self.api_key is not None

    def generate(self, prompt: str, max_tokens: int = 2000) -> str:
        if not self.is_available():
            raise ValueError("Anthropic API key not configured")

        headers = {
            "x-api-key": self.api_key,
            "Content-Type": "application/json",
            "anthropic-version": "2023-06-01",
        }

        data = {
            "model": "claude-sonnet-4-6",
            "max_tokens": max_tokens,
            "system": "You are a professional podcast script writer. Write engaging, conversational scripts for a daily tech news podcast about Databricks and data engineering.",
            "messages": [{"role": "user", "content": prompt}],
        }

        response = requests.post(self.BASE_URL, headers=headers, json=data, timeout=60)
        response.raise_for_status()

        result = response.json()
        return result["content"][0]["text"]


class GeminiProvider(LLMProvider):
    """Google Gemini API provider."""

    BASE_URL = (
        "https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent"
    )

    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or os.environ.get("GOOGLE_API_KEY")

    def is_available(self) -> bool:
        return self.api_key is not None

    def generate(self, prompt: str, max_tokens: int = 2000) -> str:
        if not self.is_available():
            raise ValueError("Google API key not configured")

        url = f"{self.BASE_URL}?key={self.api_key}"

        data = {
            "contents": [{"parts": [{"text": prompt}]}],
            "generationConfig": {
                "maxOutputTokens": max_tokens,
                "temperature": 0.7,
            },
            "systemInstruction": {
                "parts": [
                    {
                        "text": "You are a professional podcast script writer. Write engaging, conversational scripts for a daily tech news podcast about Databricks and data engineering."
                    }
                ]
            },
        }

        response = requests.post(url, json=data, timeout=60)
        response.raise_for_status()

        result = response.json()
        return result["candidates"][0]["content"]["parts"][0]["text"]


class FallbackProvider(LLMProvider):
    """Fallback template-based generation when no LLM is available."""

    def generate(self, prompt: str, max_tokens: int = 2000) -> str:
        # This is a simple template-based fallback
        # It won't produce as good results but ensures the pipeline doesn't fail
        logger.warning("Using fallback template-based generation")
        return self._generate_template_script(prompt)

    def _generate_template_script(self, prompt: str) -> str:
        """Generate a basic script from template when no LLM available."""
        # Extract story information from the prompt
        # This is a very basic implementation
        return """
Welcome to the Daily Databricks Digest! I'm your host, bringing you the latest news
from the world of Databricks and data engineering.

Today we have several interesting stories to cover. Let's dive in.

[Story summaries would go here based on the input data]

That's all for today's episode. Thanks for listening to the Daily Databricks Digest.
Don't forget to subscribe and we'll see you tomorrow with more data engineering news!
"""


class ScriptGenerator:
    """
    Generate podcast scripts from news summaries using LLM APIs.

    Supports multiple providers with automatic fallback:
    1. Groq (fastest, free tier)
    2. Anthropic Claude
    3. Google Gemini
    4. Template-based fallback
    """

    # Target script specifications
    TARGET_WORD_COUNT = 850  # ~5-6 min podcast
    WORDS_PER_MINUTE = 150  # Average speaking rate

    def __init__(
        self,
        groq_api_key: Optional[str] = None,
        anthropic_api_key: Optional[str] = None,
        google_api_key: Optional[str] = None,
    ):
        """
        Initialize script generator with available LLM providers.

        Args:
            groq_api_key: Groq API key
            anthropic_api_key: Anthropic API key
            google_api_key: Google Gemini API key
        """
        self.providers = [
            AnthropicProvider(anthropic_api_key),
            GroqProvider(groq_api_key),
            GeminiProvider(google_api_key),
            FallbackProvider(),
        ]

    def get_available_provider(self) -> LLMProvider:
        """Get first available LLM provider."""
        for provider in self.providers:
            if isinstance(provider, FallbackProvider) or provider.is_available():
                return provider
        return FallbackProvider()

    def generate_script(
        self,
        stories: List[Dict[str, Any]],
        date: Optional[datetime] = None,
        podcast_name: str = "Daily Databricks Digest",
    ) -> PodcastScript:
        """
        Generate a podcast script from news stories.

        Args:
            stories: List of story dicts with 'title', 'content', 'source', 'url'
            date: Episode date (defaults to today)
            podcast_name: Name of the podcast

        Returns:
            PodcastScript object
        """
        date = date or datetime.now(timezone.utc)
        date_str = date.strftime("%B %d, %Y")

        # Build the prompt
        prompt = self._build_prompt(stories, date_str, podcast_name)

        # Generate script using available provider
        provider = self.get_available_provider()
        logger.info(f"Using LLM provider: {type(provider).__name__}")

        try:
            raw_script = provider.generate(prompt, max_tokens=2000)
        except Exception as e:
            logger.error(f"Error generating script: {e}")
            raw_script = FallbackProvider().generate(prompt)

        # Parse the generated script
        parsed = self._parse_script(raw_script, stories)

        # Calculate metrics
        word_count = len(parsed["full_script"].split())
        duration_seconds = int((word_count / self.WORDS_PER_MINUTE) * 60)

        return PodcastScript(
            episode_date=date.strftime("%Y-%m-%d"),
            title=f"{podcast_name} - {date_str}",
            intro=parsed["intro"],
            stories=parsed["stories"],
            outro=parsed["outro"],
            full_script=parsed["full_script"],
            word_count=word_count,
            estimated_duration_seconds=duration_seconds,
            metadata={
                "provider": type(provider).__name__,
                "story_count": len(stories),
                "generated_at": datetime.now(timezone.utc).isoformat(),
            },
        )

    # Maps internal source names to listener-friendly labels and podcast treatment
    SOURCE_LABELS = {
        "github_releases":     ("Official Release",       "🚀"),
        "pypi_releases":       ("Package Release",        "📦"),
        "rss_feed":            ("Official Blog / Article","📰"),
        "hacker_news":         ("Tech Community Buzz",    "💬"),
        "reddit":              ("Reddit Community",       "👥"),
        "stackoverflow":       ("Developer Q&A",          "🔧"),
        "devto":               ("Dev Tutorial / Article", "✍️"),
        "databricks_community":("Databricks Community",   "🏢"),
        "youtube":             ("Video Content",          "🎥"),
    }

    # Podcast treatment instructions per source type
    SOURCE_TREATMENT = {
        "github_releases":      "Read this as a formal release announcement. Mention the version number and 1-2 key highlights from the release notes.",
        "pypi_releases":        "Frame this as a quick package update note. Mention the version and why practitioners should upgrade.",
        "rss_feed":             "Summarize the blog post's key insight. Add context on why this matters for data engineers.",
        "hacker_news":          "Mention that this is trending in the tech community. Add your own take on why it caught people's attention.",
        "reddit":               "Frame this as what the community is actively discussing. Capture the spirit of the thread.",
        "stackoverflow":        "Present this as a common pain point developers are hitting. If answered, briefly mention the solution direction.",
        "devto":                "Introduce this as a practical tutorial or guide. Highlight what skill or pattern listeners will learn.",
        "databricks_community": "Frame this as a real-world question or discussion from Databricks practitioners in the field.",
        "youtube":              "Mention this as a video resource worth watching for a deeper dive.",
    }

    def _build_prompt(self, stories: List[Dict[str, Any]], date_str: str, podcast_name: str) -> str:
        """Build a source-aware prompt for richer podcast script generation."""
        stories_text = ""
        for i, story in enumerate(stories[:10], 1):
            source = story.get("source", "unknown")
            label, emoji = self.SOURCE_LABELS.get(source, ("News", "📌"))
            treatment = self.SOURCE_TREATMENT.get(source, "Cover this as a general news item.")

            stories_text += f"""
Story {i} [{emoji} {label}]:
Title: {story.get('title', 'Untitled')}
Source: {source}
Summary: {story.get('content', story.get('title', ''))[:500]}
Podcast treatment hint: {treatment}
---
"""

        # Identify if there are any official releases today for special framing
        release_sources = {"github_releases", "pypi_releases"}
        has_releases = any(s.get("source") in release_sources for s in stories[:10])
        release_tease = " We also have some fresh software releases to cover." if has_releases else ""

        prompt = f"""Write a podcast script for "{podcast_name}" dated {date_str}.

The podcast should be approximately {self.TARGET_WORD_COUNT} words (about 5-6 minutes when read aloud).

Today's content comes from 9 different sources: official Databricks & OSS project blogs,
GitHub release announcements, PyPI package updates, Hacker News, Reddit, Stack Overflow,
Dev.to tutorials, the Databricks Community Forum, and YouTube.{release_tease}

Here are today's top stories:

{stories_text}

Please write a complete, engaging podcast script with these sections:

1. **[INTRO]** (3-4 sentences):
   - Warm welcome and date
   - Set context: "Today we're covering everything from official announcements to what
     the community is building and asking about"
   - Tease 1-2 of the most exciting stories

2. **[STORY N: Title]** for each story (one per story, covering 5-7 stories):
   - Use the "Podcast treatment hint" to frame each story appropriately
   - For releases: lead with "Big news — version X of Y just dropped..."
   - For community content: lead with "The community has been buzzing about..."
   - For tutorials: lead with "If you want to learn..."
   - For Q&A/Stack Overflow: lead with "A common question practitioners are hitting is..."
   - Keep each story to 3-5 sentences — enough to be informative, not exhausting
   - End each story with a brief "why it matters" or "what to do next" sentence

3. **[OUTRO]** (3-4 sentences):
   - Thank listeners
   - Remind them of the breadth of sources ("from official release notes to community discussions")
   - Call to action: subscribe, follow along, check the show notes for links
   - Warm sign-off

Important guidelines:
- Conversational, audio-first language — no markdown, no bullet points in the script itself
- Natural transitions between stories ("Speaking of releases...", "On a more community note...",
  "Switching gears to the developer side...")
- Group related stories together where natural (e.g. two releases back-to-back)
- Add personality: a touch of excitement for big releases, empathy for tricky SO questions
- Vary sentence length for rhythm — mix short punchy sentences with longer explanations
- Avoid reading raw URLs; say "check the show notes" instead
"""
        return prompt

    def _parse_script(
        self, raw_script: str, original_stories: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Parse the generated script into sections."""
        import re

        result = {
            "intro": "",
            "stories": [],
            "outro": "",
            "full_script": raw_script,
        }

        # Try to extract intro
        intro_match = re.search(
            r"\[INTRO\](.*?)(?=\[STORY|\[OUTRO|$)", raw_script, re.DOTALL | re.IGNORECASE
        )
        if intro_match:
            result["intro"] = intro_match.group(1).strip()

        # Try to extract stories
        story_pattern = r"\[STORY\s*\d*:?\s*(.*?)\](.*?)(?=\[STORY|\[OUTRO|$)"
        story_matches = re.findall(story_pattern, raw_script, re.DOTALL | re.IGNORECASE)

        for i, (title, content) in enumerate(story_matches):
            # Try to match with original story
            source = "Unknown"
            if i < len(original_stories):
                source = original_stories[i].get("source", "Unknown")

            result["stories"].append(
                {
                    "title": title.strip() or f"Story {i + 1}",
                    "content": content.strip(),
                    "source": source,
                }
            )

        # Try to extract outro
        outro_match = re.search(r"\[OUTRO\](.*?)$", raw_script, re.DOTALL | re.IGNORECASE)
        if outro_match:
            result["outro"] = outro_match.group(1).strip()

        # If parsing failed, use the whole script
        if not result["intro"] and not result["stories"]:
            result["intro"] = raw_script[:500]
            result["outro"] = raw_script[-300:]

        return result

    def generate_ssml(self, script: PodcastScript) -> str:
        """
        Convert script to SSML for text-to-speech.

        Args:
            script: PodcastScript object

        Returns:
            SSML-formatted string
        """
        ssml_parts = ["<speak>"]

        # Intro with medium pause after
        ssml_parts.append(f"<p>{script.intro}</p>")
        ssml_parts.append('<break time="1s"/>')

        # Stories with pauses between
        for story in script.stories:
            ssml_parts.append(f'<p>{story["content"]}</p>')
            ssml_parts.append('<break time="800ms"/>')

        # Outro
        ssml_parts.append('<break time="1s"/>')
        ssml_parts.append(f"<p>{script.outro}</p>")

        ssml_parts.append("</speak>")

        return "\n".join(ssml_parts)


def select_top_stories(
    items: List[Dict[str, Any]],
    max_stories: int = 10,
    diversity_weight: float = 0.3,
) -> List[Dict[str, Any]]:
    """
    Select top stories for the podcast with source diversity and category balance.

    Ensures the final episode has a mix of:
    - Official announcements / releases
    - Community discussions
    - Educational / tutorial content

    Args:
        items: List of news items (as dicts)
        max_stories: Maximum number of stories to select
        diversity_weight: How much to prioritize source diversity (0-1)

    Returns:
        Selected stories ordered for good podcast flow
    """
    if not items:
        return []

    # Category groupings for diversity balancing
    RELEASE_SOURCES = {"github_releases", "pypi_releases"}
    COMMUNITY_SOURCES = {"hacker_news", "reddit", "databricks_community", "stackoverflow"}
    CONTENT_SOURCES = {"rss_feed", "devto", "youtube"}

    # First pass: sort by quality score
    sorted_items = sorted(items, key=lambda x: x.get("quality_score", 0), reverse=True)

    selected = []
    source_counts: Dict[str, int] = {}

    for item in sorted_items:
        if len(selected) >= max_stories:
            break

        source = item.get("source", "unknown")
        source_count = source_counts.get(source, 0)
        diversity_penalty = source_count * diversity_weight
        effective_score = item.get("quality_score", 0) - diversity_penalty

        if effective_score > 0.2 or len(selected) < max_stories // 2:
            selected.append(item)
            source_counts[source] = source_count + 1

    # Second pass: reorder for good podcast flow
    # Order: releases → official content → community → tutorials/Q&A
    def _flow_key(item: Dict[str, Any]) -> int:
        src = item.get("source", "")
        if src in RELEASE_SOURCES:
            return 0
        if src == "rss_feed":
            return 1
        if src in COMMUNITY_SOURCES:
            return 2
        if src in CONTENT_SOURCES:
            return 3
        return 4

    selected.sort(key=_flow_key)
    return selected
