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

    def _build_prompt(self, stories: List[Dict[str, Any]], date_str: str, podcast_name: str) -> str:
        """Build the prompt for script generation."""
        stories_text = ""
        for i, story in enumerate(stories[:10], 1):  # Limit to 10 stories
            stories_text += f"""
Story {i}:
Title: {story.get('title', 'Untitled')}
Source: {story.get('source', 'Unknown')}
Summary: {story.get('content', story.get('title', ''))[:500]}
---
"""

        prompt = f"""Write a podcast script for "{podcast_name}" dated {date_str}.

The podcast should be approximately {self.TARGET_WORD_COUNT} words (about 5-6 minutes when read aloud).

Here are today's top stories about Databricks and data engineering:

{stories_text}

Please write a complete podcast script with:

1. **INTRO** (2-3 sentences): A warm welcome, mention the date, and tease what's coming up.

2. **STORIES** (main content): Cover the top 5-7 most interesting stories. For each story:
   - Introduce it naturally with a transition
   - Explain why it matters to data engineers and Databricks users
   - Keep each story to 2-3 sentences
   - Make it conversational and engaging

3. **OUTRO** (2-3 sentences): Thank listeners, remind them to subscribe, and sign off.

Important guidelines:
- Use conversational language suitable for audio
- Include natural transitions between stories
- Avoid jargon without brief explanations
- Make it sound like two hosts talking (use "we" and vary between "I")
- Add personality and enthusiasm
- Don't just read headlines - provide insight and context

Format the script with clear section markers:
[INTRO]
...
[STORY 1: Title]
...
[STORY 2: Title]
...
[OUTRO]
...
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
    Select top stories for the podcast with source diversity.

    Args:
        items: List of news items (as dicts)
        max_stories: Maximum number of stories to select
        diversity_weight: How much to prioritize source diversity (0-1)

    Returns:
        Selected stories
    """
    if not items:
        return []

    # Sort by quality score
    sorted_items = sorted(items, key=lambda x: x.get("quality_score", 0), reverse=True)

    selected = []
    source_counts: Dict[str, int] = {}

    for item in sorted_items:
        if len(selected) >= max_stories:
            break

        source = item.get("source", "unknown")

        # Calculate diversity penalty
        source_count = source_counts.get(source, 0)
        diversity_penalty = source_count * diversity_weight

        # Adjust effective score
        effective_score = item.get("quality_score", 0) - diversity_penalty

        # Select if effective score is still good or we don't have enough stories
        if effective_score > 0.2 or len(selected) < max_stories // 2:
            selected.append(item)
            source_counts[source] = source_count + 1

    return selected
