# Daily Databricks Digest

A fully automated daily podcast generator that aggregates news about Databricks, data engineering, and the lakehouse ecosystem from multiple sources, generates engaging scripts using LLMs, converts them to audio using text-to-speech, and publishes via RSS feed.

## Features

- **Multi-source news aggregation**: Hacker News, Reddit, YouTube, RSS feeds
- **Intelligent content filtering**: Automatically filters for Databricks-related content
- **LLM-powered scripts**: Uses Groq, Anthropic Claude, or Google Gemini for natural podcast scripts
- **Professional TTS**: Google Cloud Text-to-Speech with alternating male/female voices
- **Podcast distribution**: RSS 2.0 feed with iTunes extensions for major podcast platforms
- **Fully automated**: GitHub Actions runs daily at 6 AM UTC

## Architecture

```
GitHub Actions (6 AM UTC daily)
        ↓
[Bronze] Fetch APIs → JSON files
        ↓
[Silver] Clean, dedupe, extract keywords
        ↓
[Gold] Select top 10 stories
        ↓
[Script] Generate via LLM (Groq/Claude/Gemini)
        ↓
[Audio] Google Cloud TTS (alternating voices)
        ↓
[Publish] Upload to GCS, update RSS feed
```

## Quick Start

### 1. Clone the repository

```bash
git clone https://github.com/yourusername/daily-databricks-feed.git
cd daily-databricks-feed
```

### 2. Install dependencies

```bash
python -m pip install -r requirements.txt
pip install -e .
```

### 3. Set up environment variables

Copy the template and fill in your credentials:

```bash
cp .env.template .env
```

Required for full functionality:
- `REDDIT_CLIENT_ID` / `REDDIT_CLIENT_SECRET` - [Create Reddit App](https://www.reddit.com/prefs/apps)
- `YOUTUBE_API_KEY` - [Google Cloud Console](https://console.cloud.google.com/apis/credentials)
- `GCP_SERVICE_ACCOUNT_JSON` - Service account for TTS and GCS
- `GROQ_API_KEY` or `ANTHROPIC_API_KEY` - For script generation

### 4. Run locally

```bash
# Run the full pipeline
cd notebooks
python 01_bronze_ingestion.py
python 02_silver_transformation.py
python 03_gold_aggregation.py
python 04_script_generation.py
python 05_audio_generation.py
python 06_publish_podcast.py
```

## Project Structure

```
daily_databricks_feed/
├── databricks.yml              # Databricks Asset Bundle config
├── resources/
│   └── jobs.yml                # Workflow definition
├── src/daily_databricks_feed/
│   ├── data_sources/           # API integrations
│   │   ├── base.py             # Rate-limited base class
│   │   ├── hacker_news.py      # Algolia API
│   │   ├── reddit.py           # PRAW wrapper
│   │   ├── youtube.py          # YouTube Data API
│   │   └── rss_feeds.py        # feedparser
│   ├── transformations/
│   │   └── bronze_to_silver.py # Cleaning & deduplication
│   ├── aggregation/
│   │   └── script_generator.py # LLM prompt engineering
│   ├── podcast/
│   │   ├── tts_generator.py    # Google Cloud TTS
│   │   └── rss_publisher.py    # RSS 2.0 + iTunes
│   └── utils/
│       └── secrets.py          # Environment management
├── notebooks/                   # Pipeline notebooks (1-6)
├── tests/                       # Unit tests
├── .github/workflows/
│   ├── ci.yml                  # Lint, test, validate
│   └── deploy.yml              # Daily podcast generation
└── requirements.txt
```

## Data Sources

| Source | Auth | Rate Limit | Content |
|--------|------|------------|---------|
| Hacker News | None | 10K/hour | Tech news with Databricks mentions |
| Reddit | OAuth 2.0 | 100/min | r/databricks, r/dataengineering |
| YouTube | API Key | 10K units/day | Tutorials & announcements |
| RSS Feeds | None | Unlimited | Official blogs |

## LLM Providers (Free Tiers)

| Provider | Free Tier | Model |
|----------|-----------|-------|
| **Groq** (Recommended) | 14,400 req/day | Llama 3.1 70B |
| Anthropic Claude | Limited | Claude 3 Haiku |
| Google Gemini | 60 req/min | Gemini 1.5 Flash |
| Fallback | Unlimited | Template-based |

## GitHub Actions Setup

1. Go to your repository Settings → Secrets and variables → Actions
2. Add the following secrets:

| Secret | Description |
|--------|-------------|
| `REDDIT_CLIENT_ID` | Reddit OAuth client ID |
| `REDDIT_CLIENT_SECRET` | Reddit OAuth secret |
| `YOUTUBE_API_KEY` | YouTube Data API key |
| `GCP_SERVICE_ACCOUNT_JSON` | GCP service account JSON |
| `GCS_BUCKET_NAME` | GCS bucket name |
| `AUDIO_BASE_URL` | Public URL for audio files |
| `GROQ_API_KEY` | Groq API key |

3. The workflow runs automatically at 6 AM UTC daily, or trigger manually from Actions tab.

## Podcast Distribution

After your first episode is generated:

1. **Spotify**: Submit at [podcasters.spotify.com](https://podcasters.spotify.com)
2. **Apple Podcasts**: Submit at [podcastsconnect.apple.com](https://podcastsconnect.apple.com)
3. **Google Podcasts**: Submit via Google Podcasts Manager

All platforms use the same RSS feed URL.

## Voice Configuration

The podcast uses alternating voices for variety:

- **Intro/Outro**: Female voice (en-US-Neural2-F)
- **Odd stories (1, 3, 5...)**: Male voice (en-US-Neural2-D)
- **Even stories (2, 4, 6...)**: Female voice (en-US-Neural2-F)

## Development

```bash
# Install dev dependencies
pip install -e ".[dev]"

# Run tests
pytest tests/ -v

# Format code
black src/ --line-length 100

# Lint
flake8 src/

# Type check
mypy src/
```

## Cost Estimates (Monthly)

With daily runs:

| Service | Usage | Cost |
|---------|-------|------|
| GitHub Actions | ~300 min | Free (2000 min/month) |
| Groq API | 30 requests | Free |
| Google Cloud TTS | ~150K chars | ~$0.60 |
| GCS Storage | ~150 MB | ~$0.01 |
| **Total** | | **~$0.61/month** |

## Troubleshooting

### No news items fetched
- Check API credentials in environment variables
- Verify rate limits haven't been exceeded
- Try increasing `DAYS_BACK` for more results

### Script generation fails
- Ensure at least one LLM API key is configured
- Falls back to template-based generation automatically

### Audio generation fails
- Verify GCP service account has Text-to-Speech API enabled
- Set `USE_MOCK_TTS=true` for testing without GCP

### RSS feed not updating
- Check GCS bucket permissions
- Verify `AUDIO_BASE_URL` matches bucket configuration

## License

MIT License - see [LICENSE](LICENSE) for details.

## Contributing

Contributions welcome! Please read our contributing guidelines and submit PRs.

## Acknowledgments

- [Databricks](https://databricks.com) for the amazing data platform
- [Hacker News Algolia API](https://hn.algolia.com/api) for free search
- [Google Cloud TTS](https://cloud.google.com/text-to-speech) for natural voices
- [Groq](https://groq.com) for fast, free LLM inference
