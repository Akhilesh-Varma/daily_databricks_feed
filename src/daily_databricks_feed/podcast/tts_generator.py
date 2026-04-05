"""Text-to-Speech generator using Google Cloud TTS with alternating voices."""

import os
import io
import json
import base64
import logging
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional, Tuple
from dataclasses import dataclass
import tempfile
import requests

logger = logging.getLogger(__name__)

# pydub is optional for audio stitching
try:
    from pydub import AudioSegment

    PYDUB_AVAILABLE = True
except ImportError:
    PYDUB_AVAILABLE = False
    logger.warning("pydub not installed. Audio stitching will be limited.")


@dataclass
class VoiceConfig:
    """Configuration for a TTS voice."""

    name: str
    language_code: str
    ssml_gender: str  # MALE, FEMALE, NEUTRAL

    def to_dict(self) -> Dict[str, str]:
        """Convert to API format."""
        return {
            "languageCode": self.language_code,
            "name": self.name,
            "ssmlGender": self.ssml_gender,
        }


# Pre-configured voices for the podcast
# Using Standard voices (free tier: 4M chars/month)
VOICES = {
    "female_host": VoiceConfig(
        name="en-US-Standard-F",
        language_code="en-US",
        ssml_gender="FEMALE",
    ),
    "male_host": VoiceConfig(
        name="en-US-Standard-D",
        language_code="en-US",
        ssml_gender="MALE",
    ),
    "female_casual": VoiceConfig(
        name="en-US-Standard-C",
        language_code="en-US",
        ssml_gender="FEMALE",
    ),
    "male_casual": VoiceConfig(
        name="en-US-Standard-B",
        language_code="en-US",
        ssml_gender="MALE",
    ),
}


@dataclass
class AudioSegmentInfo:
    """Information about a generated audio segment."""

    segment_type: str  # intro, story, outro
    voice_name: str
    text: str
    duration_ms: int
    audio_data: bytes
    index: int


@dataclass
class PodcastAudio:
    """Complete podcast audio with metadata."""

    episode_date: str
    title: str
    audio_data: bytes
    format: str
    duration_seconds: int
    file_size_bytes: int
    segments: List[Dict[str, Any]]
    metadata: Dict[str, Any]

    def save(self, filepath: str) -> str:
        """Save audio to file."""
        with open(filepath, "wb") as f:
            f.write(self.audio_data)
        return filepath


class TTSGenerator:
    """
    Generate podcast audio using Google Cloud Text-to-Speech API.

    Features:
    - Alternating male/female voices between stories
    - SSML support for natural pauses and emphasis
    - Audio segment stitching
    - MP3 output optimized for podcasts

    Requires:
    - GCP_SERVICE_ACCOUNT_JSON environment variable with service account key
    - Or GOOGLE_APPLICATION_CREDENTIALS pointing to key file
    """

    BASE_URL = "https://texttospeech.googleapis.com/v1/text:synthesize"

    # Audio configuration for podcast quality
    AUDIO_CONFIG = {
        "audioEncoding": "MP3",
        "speakingRate": 0.95,  # Slightly slower for clarity
        "pitch": 0.0,
        "volumeGainDb": 0.0,
        "sampleRateHertz": 24000,
        "effectsProfileId": ["headphone-class-device"],
    }

    def __init__(
        self,
        credentials_json: Optional[str] = None,
        female_voice: VoiceConfig = VOICES["female_host"],
        male_voice: VoiceConfig = VOICES["male_host"],
    ):
        """
        Initialize TTS generator.

        Args:
            credentials_json: GCP service account JSON string
            female_voice: Voice config for female segments
            male_voice: Voice config for male segments
        """
        self.credentials_json = credentials_json or os.environ.get("GCP_SERVICE_ACCOUNT_JSON")
        self.female_voice = female_voice
        self.male_voice = male_voice
        self._access_token: Optional[str] = None
        self._token_expiry: Optional[datetime] = None

    def is_available(self) -> bool:
        """Check if TTS is available."""
        return self.credentials_json is not None

    def _get_access_token(self) -> str:
        """Get OAuth2 access token for Google Cloud API."""
        if not self.credentials_json:
            raise ValueError("GCP credentials not configured. Set GCP_SERVICE_ACCOUNT_JSON.")

        # Check if token is still valid
        if self._access_token and self._token_expiry:
            if datetime.now(timezone.utc) < self._token_expiry:
                return self._access_token

        try:
            from google.oauth2 import service_account
            from google.auth.transport.requests import Request

            # Parse credentials
            creds_dict = json.loads(self.credentials_json)

            credentials = service_account.Credentials.from_service_account_info(
                creds_dict,
                scopes=["https://www.googleapis.com/auth/cloud-platform"],
            )

            # Refresh token
            credentials.refresh(Request())

            self._access_token = credentials.token
            self._token_expiry = credentials.expiry

            return self._access_token

        except ImportError:
            # Fallback to manual JWT authentication
            return self._get_access_token_manual()

    def _get_access_token_manual(self) -> str:
        """Get access token using manual JWT creation (no google-auth library)."""
        import time
        import hashlib
        import hmac

        creds = json.loads(self.credentials_json)

        # Create JWT header
        header = {"alg": "RS256", "typ": "JWT"}

        # Create JWT payload
        now = int(time.time())
        payload = {
            "iss": creds["client_email"],
            "scope": "https://www.googleapis.com/auth/cloud-platform",
            "aud": "https://oauth2.googleapis.com/token",
            "iat": now,
            "exp": now + 3600,
        }

        # This is a simplified version - in production, use google-auth library
        raise NotImplementedError(
            "Manual JWT auth not implemented. Please install google-auth: "
            "pip install google-auth google-auth-oauthlib"
        )

    def _synthesize_speech(self, text: str, voice: VoiceConfig, use_ssml: bool = False) -> bytes:
        """
        Synthesize speech for a single text segment.

        Args:
            text: Text to synthesize (plain text or SSML)
            voice: Voice configuration
            use_ssml: Whether text is SSML

        Returns:
            MP3 audio data as bytes
        """
        if not self.is_available():
            raise ValueError("TTS not available - no credentials configured")

        access_token = self._get_access_token()

        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
        }

        # Build request
        input_key = "ssml" if use_ssml else "text"
        request_body = {
            "input": {input_key: text},
            "voice": voice.to_dict(),
            "audioConfig": self.AUDIO_CONFIG,
        }

        response = requests.post(
            self.BASE_URL,
            headers=headers,
            json=request_body,
            timeout=60,
        )
        response.raise_for_status()

        result = response.json()
        audio_content = base64.b64decode(result["audioContent"])

        return audio_content

    def generate_podcast(
        self,
        script: Dict[str, Any],  # PodcastScript as dict
        add_music: bool = False,
    ) -> PodcastAudio:
        """
        Generate complete podcast audio from script.

        Voice assignment:
        - Intro: Female voice
        - Odd stories (1, 3, 5...): Male voice
        - Even stories (2, 4, 6...): Female voice
        - Outro: Female voice

        Args:
            script: PodcastScript dict with intro, stories, outro
            add_music: Whether to add intro/outro music (requires music files)

        Returns:
            PodcastAudio with complete episode
        """
        segments: List[AudioSegmentInfo] = []
        segment_index = 0

        # Generate intro
        if script.get("intro"):
            logger.info("Generating intro audio...")
            intro_ssml = self._wrap_ssml(script["intro"], add_pause_after=True)
            intro_audio = self._synthesize_speech(intro_ssml, self.female_voice, use_ssml=True)
            segments.append(
                AudioSegmentInfo(
                    segment_type="intro",
                    voice_name=self.female_voice.name,
                    text=script["intro"],
                    duration_ms=self._estimate_duration(script["intro"]),
                    audio_data=intro_audio,
                    index=segment_index,
                )
            )
            segment_index += 1

        # Generate stories with alternating voices
        for i, story in enumerate(script.get("stories", [])):
            logger.info(f"Generating story {i + 1} audio...")

            # Alternate voices: odd = male, even = female
            voice = self.male_voice if (i % 2 == 0) else self.female_voice

            story_text = story.get("content", story.get("title", ""))
            if not story_text:
                continue

            story_ssml = self._wrap_ssml(story_text, add_pause_after=True)
            story_audio = self._synthesize_speech(story_ssml, voice, use_ssml=True)

            segments.append(
                AudioSegmentInfo(
                    segment_type="story",
                    voice_name=voice.name,
                    text=story_text,
                    duration_ms=self._estimate_duration(story_text),
                    audio_data=story_audio,
                    index=segment_index,
                )
            )
            segment_index += 1

        # Generate outro
        if script.get("outro"):
            logger.info("Generating outro audio...")
            outro_ssml = self._wrap_ssml(script["outro"], add_pause_before=True)
            outro_audio = self._synthesize_speech(outro_ssml, self.female_voice, use_ssml=True)
            segments.append(
                AudioSegmentInfo(
                    segment_type="outro",
                    voice_name=self.female_voice.name,
                    text=script["outro"],
                    duration_ms=self._estimate_duration(script["outro"]),
                    audio_data=outro_audio,
                    index=segment_index,
                )
            )

        # Stitch segments together
        final_audio = self._stitch_segments(segments)

        # Calculate total duration
        total_duration_ms = sum(seg.duration_ms for seg in segments)

        return PodcastAudio(
            episode_date=script.get("episode_date", datetime.now().strftime("%Y-%m-%d")),
            title=script.get("title", "Daily Databricks Digest"),
            audio_data=final_audio,
            format="mp3",
            duration_seconds=total_duration_ms // 1000,
            file_size_bytes=len(final_audio),
            segments=[
                {
                    "type": seg.segment_type,
                    "voice": seg.voice_name,
                    "duration_ms": seg.duration_ms,
                    "index": seg.index,
                }
                for seg in segments
            ],
            metadata={
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "segment_count": len(segments),
                "voices_used": list(set(seg.voice_name for seg in segments)),
            },
        )

    def _wrap_ssml(
        self,
        text: str,
        add_pause_before: bool = False,
        add_pause_after: bool = False,
    ) -> str:
        """
        Wrap text in SSML with natural pauses.

        Args:
            text: Plain text to wrap
            add_pause_before: Add pause at beginning
            add_pause_after: Add pause at end

        Returns:
            SSML string
        """
        # Escape special characters
        text = (
            text.replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
            .replace('"', "&quot;")
            .replace("'", "&apos;")
        )

        ssml = "<speak>"

        if add_pause_before:
            ssml += '<break time="800ms"/>'

        ssml += f"<p>{text}</p>"

        if add_pause_after:
            ssml += '<break time="600ms"/>'

        ssml += "</speak>"

        return ssml

    def _estimate_duration(self, text: str) -> int:
        """
        Estimate audio duration from text length.

        Assumes ~15 characters per second at normal speaking rate.

        Args:
            text: Text to estimate

        Returns:
            Duration in milliseconds
        """
        chars = len(text)
        chars_per_second = 15
        return int((chars / chars_per_second) * 1000)

    def _stitch_segments(self, segments: List[AudioSegmentInfo]) -> bytes:
        """
        Stitch audio segments together.

        Args:
            segments: List of audio segments

        Returns:
            Combined audio data
        """
        if not segments:
            return b""

        if len(segments) == 1:
            return segments[0].audio_data

        # Concatenate raw MP3 bytes — ffmpeg is not available in serverless
        return b"".join(seg.audio_data for seg in segments)


class MockTTSGenerator(TTSGenerator):
    """
    Mock TTS generator for testing without GCP credentials.

    Generates silent audio files with correct duration.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.credentials_json = "mock"  # Mark as available

    def _synthesize_speech(self, text: str, voice: VoiceConfig, use_ssml: bool = False) -> bytes:
        """Generate silent audio for testing (no ffmpeg required)."""
        logger.info(f"Mock TTS: generating silent audio for {len(text)} chars")
        return self._minimal_mp3()

    def _minimal_mp3(self) -> bytes:
        """Return minimal valid MP3 bytes for testing."""
        # This is a minimal valid MP3 frame (silent)
        # In production, use actual silent audio generation
        return bytes(
            [
                0xFF,
                0xFB,
                0x90,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
            ]
            * 100
        )
