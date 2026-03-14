"""Secrets management for GitHub Actions and local development."""

import os
import json
import logging
from typing import Optional, Dict, Any
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class SecretConfig:
    """Configuration for a secret."""

    name: str
    env_var: str
    required: bool = True
    description: str = ""


# All secrets used by the application
SECRETS = {
    "reddit_client_id": SecretConfig(
        name="reddit_client_id",
        env_var="REDDIT_CLIENT_ID",
        required=False,
        description="Reddit OAuth app client ID",
    ),
    "reddit_client_secret": SecretConfig(
        name="reddit_client_secret",
        env_var="REDDIT_CLIENT_SECRET",
        required=False,
        description="Reddit OAuth app secret",
    ),
    "youtube_api_key": SecretConfig(
        name="youtube_api_key",
        env_var="YOUTUBE_API_KEY",
        required=False,
        description="Google Cloud YouTube Data API key",
    ),
    "gcp_service_account": SecretConfig(
        name="gcp_service_account",
        env_var="GCP_SERVICE_ACCOUNT_JSON",
        required=False,
        description="Google Cloud service account JSON for TTS and GCS",
    ),
    "gcs_bucket_name": SecretConfig(
        name="gcs_bucket_name",
        env_var="GCS_BUCKET_NAME",
        required=False,
        description="GCS bucket for audio files",
    ),
    "audio_base_url": SecretConfig(
        name="audio_base_url",
        env_var="AUDIO_BASE_URL",
        required=False,
        description="Public URL base for audio files",
    ),
    "claude_api_key": SecretConfig(
        name="claude_api_key",
        env_var="CLAUDE_API_KEY",
        required=False,
        description="Anthropic Claude API key for script generation",
    ),
    "google_api_key": SecretConfig(
        name="google_api_key",
        env_var="GOOGLE_API_KEY",
        required=False,
        description="Google API key for Gemini",
    ),
}


def get_secret(name: str, default: Optional[str] = None) -> Optional[str]:
    """
    Get a secret value from environment variables.

    This function is designed to work with:
    - GitHub Actions secrets (injected as env vars)
    - Local .env files (loaded by python-dotenv)
    - Direct environment variables

    Args:
        name: Secret name (matches keys in SECRETS dict)
        default: Default value if secret not found

    Returns:
        Secret value or default
    """
    config = SECRETS.get(name)

    if config:
        value = os.environ.get(config.env_var)
        if value:
            return value
        if config.required and default is None:
            logger.warning(
                f"Required secret '{name}' not found. "
                f"Set {config.env_var} environment variable."
            )
    else:
        # Try direct lookup by name as env var
        value = os.environ.get(name.upper())
        if value:
            return value

    return default


class SecretsManager:
    """
    Manager for application secrets.

    Supports:
    - Environment variables (GitHub Actions, local)
    - Local .env files
    - JSON secrets file (for development)
    """

    def __init__(self, env_file: Optional[str] = None, secrets_file: Optional[str] = None):
        """
        Initialize secrets manager.

        Args:
            env_file: Path to .env file (optional)
            secrets_file: Path to JSON secrets file (optional)
        """
        self._cache: Dict[str, str] = {}

        # Load from .env file if provided
        if env_file and os.path.exists(env_file):
            self._load_env_file(env_file)

        # Load from secrets file if provided
        if secrets_file and os.path.exists(secrets_file):
            self._load_secrets_file(secrets_file)

        # When running inside Databricks, load all secrets from the secret scope
        # into os.environ so the rest of the codebase can use os.environ.get() normally
        self._load_databricks_secrets()

    def _load_databricks_secrets(self) -> None:
        """
        Load secrets when running inside Databricks.

        Tries two sources in order:
        1. Databricks Widget parameters (set via job base_parameters — primary method)
        2. Databricks Secret Scope 'daily-podcast' (fallback for Premium workspaces)
        """
        try:
            import IPython

            dbutils = IPython.get_ipython().user_ns.get("dbutils")
            if dbutils is None:
                return

            secret_keys = [c.env_var for c in SECRETS.values()]
            loaded = 0

            # ── Source 1: Widget params (from job base_parameters) ──────────
            for key in secret_keys:
                if os.environ.get(key):
                    continue
                try:
                    value = dbutils.widgets.getArgument(key, "")
                    if value:
                        os.environ[key] = value
                        self._cache[key] = value
                        loaded += 1
                except Exception:
                    pass

            # ── Source 2: Databricks Secret Scope (Premium workspaces) ──────
            for key in secret_keys:
                if os.environ.get(key):
                    continue
                try:
                    value = dbutils.secrets.get(scope="daily-podcast", key=key)
                    if value:
                        os.environ[key] = value
                        self._cache[key] = value
                        loaded += 1
                except Exception:
                    pass

            if loaded:
                logger.info(f"Loaded {loaded} secrets from Databricks (widgets + secret scope)")

        except Exception:
            pass  # not running in Databricks, skip silently

    def _load_env_file(self, filepath: str) -> None:
        """Load secrets from .env file."""
        try:
            with open(filepath, "r") as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith("#") and "=" in line:
                        key, value = line.split("=", 1)
                        key = key.strip()
                        value = value.strip().strip('"').strip("'")
                        self._cache[key] = value
                        # Also set in environment
                        os.environ[key] = value

            logger.info(f"Loaded {len(self._cache)} secrets from {filepath}")

        except Exception as e:
            logger.warning(f"Error loading .env file: {e}")

    def _load_secrets_file(self, filepath: str) -> None:
        """Load secrets from JSON file."""
        try:
            with open(filepath, "r") as f:
                secrets = json.load(f)

            for key, value in secrets.items():
                if isinstance(value, str):
                    self._cache[key] = value
                    os.environ[key] = value

            logger.info(f"Loaded secrets from {filepath}")

        except Exception as e:
            logger.warning(f"Error loading secrets file: {e}")

    def get(self, name: str, default: Optional[str] = None) -> Optional[str]:
        """
        Get a secret value.

        Args:
            name: Secret name
            default: Default value

        Returns:
            Secret value or default
        """
        # Check cache first
        if name in self._cache:
            return self._cache[name]

        # Use global getter
        return get_secret(name, default)

    def get_all_available(self) -> Dict[str, bool]:
        """
        Check which secrets are configured.

        Returns:
            Dict mapping secret names to availability (True/False)
        """
        result = {}
        for name, config in SECRETS.items():
            value = os.environ.get(config.env_var)
            result[name] = value is not None and len(value) > 0
        return result

    def validate_required(self) -> bool:
        """
        Validate that all required secrets are configured.

        Returns:
            True if all required secrets are present
        """
        missing = []
        for name, config in SECRETS.items():
            if config.required:
                value = os.environ.get(config.env_var)
                if not value:
                    missing.append(name)

        if missing:
            logger.error(f"Missing required secrets: {', '.join(missing)}")
            return False

        return True

    def mask_value(self, value: str, show_chars: int = 4) -> str:
        """
        Mask a secret value for safe logging.

        Args:
            value: Secret value
            show_chars: Number of characters to show at start

        Returns:
            Masked value
        """
        if not value or len(value) <= show_chars:
            return "****"
        return value[:show_chars] + "*" * (len(value) - show_chars)

    def print_status(self) -> None:
        """Print status of all secrets (for debugging)."""
        print("\nSecrets Status:")
        print("-" * 50)

        for name, config in SECRETS.items():
            value = os.environ.get(config.env_var)
            status = "OK" if value else "MISSING"
            required = "(required)" if config.required else "(optional)"

            if value:
                masked = self.mask_value(value)
                print(f"  {name}: {status} {required} [{masked}]")
            else:
                print(f"  {name}: {status} {required}")

        print("-" * 50)


def load_dotenv(filepath: str = ".env") -> bool:
    """
    Load environment variables from .env file.

    Args:
        filepath: Path to .env file

    Returns:
        True if file was loaded successfully
    """
    if not os.path.exists(filepath):
        return False

    manager = SecretsManager(env_file=filepath)
    return True


def create_env_template(filepath: str = ".env.template") -> str:
    """
    Create a template .env file with all secrets.

    Args:
        filepath: Output file path

    Returns:
        File path
    """
    lines = [
        "# Daily Databricks Feed - Environment Variables",
        "# Copy this file to .env and fill in your values",
        "",
        "# Reddit API (https://www.reddit.com/prefs/apps)",
        "REDDIT_CLIENT_ID=",
        "REDDIT_CLIENT_SECRET=",
        "",
        "# YouTube Data API (https://console.cloud.google.com/apis/credentials)",
        "YOUTUBE_API_KEY=",
        "",
        "# Google Cloud (for TTS and GCS)",
        "# Paste the full JSON content of your service account key",
        "GCP_SERVICE_ACCOUNT_JSON=",
        "",
        "# GCS Bucket Configuration",
        "GCS_BUCKET_NAME=your-podcast-bucket",
        "AUDIO_BASE_URL=https://storage.googleapis.com/your-podcast-bucket",
        "",
        "# LLM APIs (at least one recommended)",
        "GROQ_API_KEY=",
        "ANTHROPIC_API_KEY=",
        "GOOGLE_API_KEY=",
        "",
    ]

    content = "\n".join(lines)

    with open(filepath, "w") as f:
        f.write(content)

    return filepath
