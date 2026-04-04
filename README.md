# Daily Databricks Digest 🎙️

> A fully automated, end-to-end podcast pipeline that aggregates news from 9 sources across the Databricks and data engineering ecosystem, generates a natural-sounding script using LLMs, converts it to audio via Google Cloud Text-to-Speech, and publishes a podcast-platform-ready RSS feed — all running on **Databricks Serverless** on a daily schedule.

---

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Pipeline Walkthrough](#pipeline-walkthrough)
- [Data Sources](#data-sources)
- [LLM Providers](#llm-providers)
- [Requirements](#requirements)
- [Deploying on Databricks (Primary)](#deploying-on-databricks-primary)
- [Local Development](#local-development)
- [Project Structure](#project-structure)
- [Configuration Reference](#configuration-reference)
- [Secrets Reference](#secrets-reference)
- [Voice Configuration](#voice-configuration)
- [Podcast Distribution](#podcast-distribution)
- [Cost Estimates](#cost-estimates)
- [Troubleshooting](#troubleshooting)
- [Development Guide](#development-guide)
- [License](#license)

---

## Overview

**Daily Databricks Digest** runs every day at 06:00 UTC. Within ~20–30 minutes a new podcast episode is ready for listeners on Spotify, Apple Podcasts, and any RSS-compatible player.

### What it produces

- An **~850-word, ~5–6 minute podcast episode** covering the day's most relevant Databricks and data engineering news
- A valid **RSS 2.0 + iTunes feed** (`feed.xml`) hosted on Google Cloud Storage
- **Unity Catalog Delta tables** (Bronze / Silver / Gold) for dashboards, historical queries, and trend analysis

### Key design decisions

| Decision | Rationale |
|---|---|
| PySpark 4.0 Custom Data Sources | Each API is a first-class Spark streaming format — checkpoint-aware, stateful, no external coordination |
| `availableNow` trigger | Processes all new data, then stops — batch semantics on a streaming engine |
| Dual pipeline (DLT + JSON) | DLT branch serves analytics; JSON branch drives podcast — independent failure domains |
| Multi-LLM fallback chain | Groq → Claude → Gemini → template ensures the podcast never silently skips a day |
| Serverless-only | No cluster management, auto-scaling, pay-per-use |

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  Databricks Job — Daily Databricks Podcast Generator                        │
│  Schedule: 0 0 6 * * ? (06:00 UTC daily)                                    │
└───────────────────────────┬─────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  TASK 1 — Bronze Ingestion (01_bronze_ingestion.py)                         │
│                                                                             │
│  PySpark 4.0 Custom Data Sources (9 registered formats)                     │
│  Structured Streaming with availableNow trigger                             │
│                                                                             │
│  ┌──────────────┐  ┌──────────┐  ┌─────────┐  ┌────────┐  ┌──────────┐   │
│  │ Hacker News  │  │  Reddit  │  │ YouTube │  │  RSS   │  │  GitHub  │   │
│  │ (Algolia)    │  │  (PRAW)  │  │ Data v3 │  │ Feeds  │  │ Releases │   │
│  └──────────────┘  └──────────┘  └─────────┘  └────────┘  └──────────┘   │
│  ┌──────────┐  ┌───────────┐  ┌──────────────┐                            │
│  │  Dev.to  │  │  PyPI     │  │  Databricks  │                            │
│  │  (Forem) │  │  Releases │  │  Community   │                            │
│  └──────────┘  └───────────┘  └──────────────┘                            │
│              ↓ union all streams                                            │
│     bronze_raw_landing (Delta, Unity Catalog)  +  bronze_news_raw.json     │
└────────────────────────────┬──────────────────────────────────────────────-┘
                             │
              ┌──────────────┴──────────────┐
              │                             │
              ▼                             ▼
┌─────────────────────────┐   ┌─────────────────────────────────────────────┐
│  TASK 2 — DLT Refresh   │   │  TASK 3 — Silver Transformation             │
│  (ANALYTICS BRANCH)     │   │  (PODCAST PRODUCTION BRANCH)                │
│                         │   │                                             │
│  Spark Declarative      │   │  Reads: bronze_news_raw.json                │
│  Pipelines              │   │  • Strip HTML, normalize URLs               │
│                         │   │  • Keyword extraction (21 terms)            │
│  bronze_news (ST)       │   │  • Entity extraction (40+ names)            │
│      ↓                  │   │  • Sentiment scoring                        │
│  silver_news (ST)       │   │  • Quality scoring (0–1)                    │
│      ↓                  │   │  • Deduplication (content hash)             │
│  gold_top_stories (MV)  │   │                                             │
│      ↓                  │   │  Writes: silver_news_cleaned.json           │
│  gold_daily_summary(MV) │   │          silver_news_nb (Delta)             │
│                         │   └────────────────────┬────────────────────────┘
│  For: dashboards,       │                        │
│  historical queries,    │                        ▼
│  trend analysis         │   ┌─────────────────────────────────────────────┐
└─────────────────────────┘   │  TASK 4 — Gold Aggregation                  │
                              │                                             │
                              │  • Rank by quality score                    │
                              │  • Source diversity balancing               │
                              │  • Select top MAX_STORIES (default: 10)     │
                              │                                             │
                              │  Writes: script_input.json                  │
                              └────────────────────┬────────────────────────┘
                                                   │
                                                   ▼
                              ┌─────────────────────────────────────────────┐
                              │  TASK 5 — Script Generation                 │
                              │                                             │
                              │  LLM Provider (waterfall fallback):         │
                              │  1. Groq — Llama 3.3 70B  (primary)        │
                              │  2. Anthropic — Claude Sonnet 4.6           │
                              │  3. Google — Gemini 1.5 Flash               │
                              │  4. Template-based (guaranteed fallback)    │
                              │                                             │
                              │  Output: ~850 words, ~5–6 min episode       │
                              │  Intro → Stories (source-aware) → Outro     │
                              │  + SSML markup for TTS                      │
                              │                                             │
                              │  Writes: script_YYYY-MM-DD.json/.ssml       │
                              └────────────────────┬────────────────────────┘
                                                   │
                                                   ▼
                              ┌─────────────────────────────────────────────┐
                              │  TASK 6 — Audio Generation                  │
                              │                                             │
                              │  Google Cloud Text-to-Speech (Neural2)      │
                              │  • Intro/Outro: en-US-Neural2-F (female)    │
                              │  • Stories 1,3,5…: en-US-Neural2-D (male)  │
                              │  • Stories 2,4,6…: en-US-Neural2-F (female)│
                              │  • SSML pauses between segments             │
                              │  • Raw MP3 byte concatenation (no ffmpeg)   │
                              │                                             │
                              │  Writes: podcast_YYYY-MM-DD.mp3             │
                              └────────────────────┬────────────────────────┘
                                                   │
                                                   ▼
                              ┌─────────────────────────────────────────────┐
                              │  TASK 7 — Publish                           │
                              │                                             │
                              │  1. Upload MP3 → GCS bucket                 │
                              │  2. Generate RSS 2.0 + iTunes feed.xml      │
                              │  3. Upload feed.xml → GCS (public URL)      │
                              │                                             │
                              │  Podcast platforms poll feed URL:           │
                              │  Spotify · Apple Podcasts · Google Podcasts │
                              └─────────────────────────────────────────────┘
```

---

## Pipeline Walkthrough

### Task 1 — Bronze Ingestion

Every source is implemented as a **PySpark 4.0 Custom Data Source** (`spark.dataSource.register()`). This gives each API:

- A checkpoint-aware offset system (incremental reads — only fetches what's new since the last run)
- Structured Streaming semantics with the `availableNow` trigger (runs like batch, stops when caught up)
- A uniform schema (`NewsItem`) across all 9 sources

All streams are unioned and written to `news_pipeline.daily_databricks_feed.bronze_raw_landing` (a Unity Catalog Delta table) tagged with `_run_id` and `_pipeline_run_at`.

A bounded snapshot (last `DAYS_BACK + 1` days) is also exported to `bronze_news_raw.json` on the Unity Catalog Volume for the downstream notebook chain.

### Task 2 — DLT Pipeline (Analytics Branch)

Runs in **parallel** with Task 3 and is completely independent of the podcast production chain. Built with **Spark Declarative Pipelines** (`from pyspark import pipelines as dp`):

| Table | Type | Description |
|---|---|---|
| `bronze_news` | Streaming Table | Raw stream from `bronze_raw_landing` |
| `silver_news` | Streaming Table | Cleaned, keyword-enriched, quality-scored. Drops rows via `expect_or_drop` on title, URL, and minimum quality |
| `gold_top_stories` | Materialized View | Top 10 per `(_run_id, _ingestion_date)` — full history preserved |
| `gold_daily_summary` | Materialized View | One aggregate row per run with source distribution JSON |

Use these tables in Databricks AI/BI dashboards to track trending topics, source health, and episode quality over time.

### Task 3 — Silver Transformation (Podcast Branch)

Reads from the JSON snapshot and applies the full `BronzeToSilverTransformer`:

| Transform | Detail |
|---|---|
| HTML stripping | `<[^>]+>` regex, then whitespace normalization |
| URL normalization | Removes tracking parameters, standardizes schemes |
| Keyword extraction | Matches against 21 Databricks ecosystem terms |
| Entity extraction | Identifies 40+ known product/company names |
| Sentiment scoring | Positive/negative word lists → float score |
| Quality scoring | Weighted sum: title (0.2) + content length (0.3) + social score (0.2) + comment count (0.1) + keyword hits (0.2) → capped at 1.0 |
| Deduplication | SHA-256 hash of normalized title + URL |

### Task 4 — Gold Aggregation

Selects the best stories for the episode:
- Filters to `quality_score >= 0.2`
- Applies source diversity weighting (no single source dominates)
- Returns top `MAX_STORIES` (default: 10) ranked items
- Writes `script_input.json` — the exact payload the LLM receives

### Task 5 — Script Generation

The `ScriptGenerator` builds a structured prompt per story that is **source-aware**:

- GitHub releases → emphasizes version changes and migration notes
- Stack Overflow → frames as community questions with practical answers
- Reddit posts → conversational, community-opinion angle
- RSS/blog posts → authoritative, announcement-style
- YouTube → visual demo described in audio-friendly terms

The generated script has three segments:
1. **Intro** — date, episode number, top story teaser
2. **Stories** — one paragraph per story with the source, headline, key insight, and URL
3. **Outro** — call to subscribe, preview

Target: ~850 words (150 wpm → ~5–6 minutes). SSML is generated alongside for TTS pausing.

### Task 6 — Audio Generation

Google Cloud Text-to-Speech Neural2 voices are called per segment. The audio pipeline:

1. Generates SSML-wrapped text for each segment
2. Calls the TTS REST API with OAuth2 token (from service account JSON)
3. Receives base64-encoded MP3 bytes
4. Concatenates raw bytes in order (no ffmpeg dependency)
5. Writes the final MP3

A `MockTTSGenerator` is available for local testing (`USE_MOCK_TTS=true`) — generates a minimal valid MP3 without any GCP calls.

### Task 7 — Publish

1. Uploads the MP3 to your GCS bucket at `episodes/podcast-YYYY-MM-DD.mp3`
2. Builds an RSS 2.0 document with `<itunes:*>` extensions (title, description, duration, explicit, category, image)
3. Uploads `feed.xml` to GCS at the root of the bucket
4. Podcast platforms (Spotify, Apple, Google) poll the feed URL on their own schedule and automatically pick up new episodes

---

## Data Sources

| Source | Auth | Rate Limit | Lookback | Databricks filter |
|---|---|---|---|---|
| **Hacker News** (Algolia API) | None | 10K req/hour | `days_back` | keyword match |
| **RSS Feeds** (13 feeds) | None | Unlimited | `max(days_back, 7)` | keyword match |
| **GitHub Releases** | Optional `GITHUB_TOKEN` | 60/hr (anon) · 5K/hr (token) | `max(days_back, 7)` | tracked repos |
| **Databricks Community** (Discourse) | None | Public API | `days_back` | all posts |
| **Dev.to** (Forem API) | None | Public API | `days_back` | keyword match |
| **PyPI Releases** | None | Public API | `max(days_back, 7)` | tracked packages |
| **Stack Overflow** | Optional `STACK_EXCHANGE_API_KEY` | 300/day (anon) · 10K/day (key) | `days_back` | `databricks` tag |
| **Reddit** | OAuth 2.0 (Client ID + Secret) | 100 req/min | `days_back` | r/databricks + 4 others |
| **YouTube** | API Key | 10K units/day | `max(days_back, 7)` | keyword match |

**RSS feeds included by default:**
Databricks Engineering Blog, Databricks Product Blog, Databricks Medium, Data Engineering Weekly, The Sequence, Simon Willison's Blog, MLOps Community, Data Science Weekly, arXiv cs.DB, arXiv cs.LG, Towards Data Science (Medium), Analytics Vidhya, KDnuggets.

---

## LLM Providers

The pipeline tries providers in order and uses the first one that has a valid API key configured.

| Priority | Provider | Model | Free Tier | Notes |
|---|---|---|---|---|
| 1 | **Groq** (recommended) | `llama-3.3-70b-versatile` | 14,400 req/day | Fastest. Set `GROQ_API_KEY` |
| 2 | Anthropic Claude | `claude-sonnet-4-6` | Pay-per-use | Highest quality. Set `CLAUDE_API_KEY` |
| 3 | Google Gemini | `gemini-1.5-flash` | 60 req/min | Good fallback. Set `GOOGLE_API_KEY` |
| 4 | Template | Built-in | Always free | Guarantees an episode even with no API keys |

**Recommendation:** Get a free [Groq API key](https://console.groq.com). The daily quota (14,400 requests) vastly exceeds what this pipeline uses (1–2 requests per episode).

---

## Requirements

### Databricks Workspace (Required for production)

> **Databricks Community Edition will not work.** It lacks Unity Catalog, serverless compute, Spark Declarative Pipelines, and scheduled jobs.

You need one of:
- A **paid Databricks workspace** (AWS, Azure, or GCP)
- A **Databricks free trial** (14 days, includes all features)

| Databricks Feature | Min Version / Tier |
|---|---|
| Serverless compute | Standard tier or above |
| Unity Catalog | Standard tier or above |
| Spark Declarative Pipelines | DBR 16.0+ / Serverless CURRENT channel |
| PySpark 4.0 Custom Data Sources | DBR 15.0+ |
| Databricks Asset Bundles | Databricks CLI 0.210.0+ |
| Secret Scopes | Standard tier or above |

### External Services

| Service | Required | Free Tier |
|---|---|---|
| Google Cloud (TTS + GCS) | Yes (for audio + hosting) | TTS: 1M chars/month free · GCS: 5 GB free |
| Groq | No (but recommended) | 14,400 req/day free |
| Reddit API | No (skipped if missing) | Free with app registration |
| YouTube Data API | No (skipped if missing) | 10K units/day free |
| GitHub (token) | No | 5K req/hr free with token |

### Local Development Only

```
Python >= 3.9
```

---

## Deploying on Databricks (Primary)

### 1. Install the Databricks CLI

```bash
pip install databricks-cli
# or
brew install databricks
```

Verify: `databricks --version` (must be >= 0.210.0)

### 2. Configure your workspace profile

```bash
databricks configure --profile DEFAULT
# Enter: host (e.g. https://dbc-438ac3cd-0325.cloud.databricks.com)
#        token (personal access token from workspace Settings → Developer → Access tokens)
```

### 3. Create the Unity Catalog resources

Run these SQL statements in a Databricks SQL editor or notebook once:

```sql
-- Catalog and schema for all pipeline tables
CREATE CATALOG IF NOT EXISTS news_pipeline;
CREATE SCHEMA IF NOT EXISTS news_pipeline.daily_databricks_feed;

-- Volume for JSON files and audio artifacts
CREATE VOLUME IF NOT EXISTS news_pipeline.default.podcast_data;
```

### 4. Set up a Databricks Secret Scope (recommended)

```bash
databricks secrets create-scope daily-podcast

# Add each secret
databricks secrets put-secret daily-podcast REDDIT_CLIENT_ID
databricks secrets put-secret daily-podcast REDDIT_CLIENT_SECRET
databricks secrets put-secret daily-podcast YOUTUBE_API_KEY
databricks secrets put-secret daily-podcast GROQ_API_KEY
databricks secrets put-secret daily-podcast CLAUDE_API_KEY
databricks secrets put-secret daily-podcast GCP_SERVICE_ACCOUNT_JSON
databricks secrets put-secret daily-podcast GCS_BUCKET_NAME
databricks secrets put-secret daily-podcast AUDIO_BASE_URL
```

### 5. Clone and deploy

```bash
git clone <your-repo-url>
cd daily_databricks_feed

# Validate the bundle configuration
databricks bundle validate

# Deploy to dev (default target)
databricks bundle deploy

# Deploy to production
databricks bundle deploy -t prod \
  --var="groq_api_key=$GROQ_API_KEY" \
  --var="claude_api_key=$CLAUDE_API_KEY" \
  --var="gcp_service_account_json=$GCP_SERVICE_ACCOUNT_JSON" \
  --var="gcs_bucket_name=your-bucket-name" \
  --var="audio_base_url=https://storage.googleapis.com/your-bucket"
```

### 6. Run the pipeline manually (first test)

```bash
# Trigger one full run now
databricks bundle run daily_databricks_podcast -t prod

# Or via the UI: Jobs → Daily Databricks Podcast Generator → Run now
```

### 7. Verify deployment

```bash
# List deployed resources
databricks bundle summary

# Check job status
databricks jobs list | grep "Daily Databricks"

# View recent runs
databricks runs list --job-name "Daily Databricks Podcast Generator"
```

The job will then run automatically every day at 06:00 UTC.

---

## Local Development

Local development runs the notebook scripts as plain Python — no Spark required for most steps. Audio generation requires a GCP service account.

### 1. Clone and install

```bash
git clone <your-repo-url>
cd daily_databricks_feed

python -m pip install -e ".[dev,gcp]"
```

### 2. Configure environment

```bash
cp .env.template .env
# Edit .env with your credentials (see Secrets Reference below)
```

### 3. Run the pipeline locally

```bash
# Step by step
python notebooks/01_bronze_ingestion.py   # Fetches news, writes JSON
python notebooks/02_silver_transformation.py
python notebooks/03_gold_aggregation.py
python notebooks/04_script_generation.py   # Requires at least one LLM key
python notebooks/05_audio_generation.py    # Requires GCP service account
                                           # or set USE_MOCK_TTS=true
python notebooks/06_publish_podcast.py     # Requires GCS bucket
                                           # or set DRY_RUN=true
```

### 4. Skip optional steps

```bash
# Test script generation without audio/publish
USE_MOCK_TTS=true DRY_RUN=true python notebooks/05_audio_generation.py
```

### 5. Run tests

```bash
pytest tests/ -v
pytest tests/ -v --cov=src --cov-report=term-missing
```

---

## Project Structure

```
daily_databricks_feed/
│
├── databricks.yml                      # Databricks Asset Bundle root config
│                                       # Defines variables, targets (dev/staging/prod), artifacts
│
├── resources/
│   ├── jobs.yml                        # Job definition: 7 tasks, schedule, notifications, timeouts
│   └── pipeline.yml                    # Spark Declarative Pipelines config (serverless, CURRENT channel)
│
├── src/daily_databricks_feed/          # Installable Python package (built as .whl for Databricks)
│   │
│   ├── data_sources/
│   │   ├── base.py                     # NewsItem dataclass, RateLimiter (token bucket), BaseDataSource ABC
│   │   ├── hacker_news.py              # Algolia HN search API — 2 req/s
│   │   ├── reddit.py                   # PRAW wrapper — 1.5 req/s, 5 subreddits
│   │   ├── youtube.py                  # YouTube Data API v3 — 0.5 req/s
│   │   ├── rss_feeds.py                # feedparser — 13 default feeds, configurable
│   │   ├── devto.py                    # Forem/Dev.to public API
│   │   ├── discourse.py                # Databricks Community Forum (Discourse API)
│   │   ├── github_releases.py          # GitHub Releases API — tracks key OSS repos
│   │   ├── pypi_releases.py            # PyPI JSON API — tracks databricks-*, mlflow, pyspark
│   │   ├── stackoverflow.py            # Stack Exchange API — databricks tag
│   │   └── pyspark_sources/            # PySpark 4.0 Custom Data Source implementations
│   │       ├── base_source.py          # DataSourceV2 base with offset/checkpoint support
│   │       ├── hacker_news_source.py   # format: "hacker_news_news"
│   │       ├── reddit_source.py        # format: "reddit_news"
│   │       ├── youtube_source.py       # format: "youtube_news"
│   │       ├── rss_feed_source.py      # format: "rss_news"
│   │       ├── devto_source.py         # format: "devto_news"
│   │       ├── discourse_source.py     # format: "discourse_news"
│   │       ├── github_releases_source.py # format: "github_releases_news"
│   │       ├── pypi_source.py          # format: "pypi_releases_news"
│   │       └── stackoverflow_source.py # format: "stackoverflow_news"
│   │
│   ├── transformations/
│   │   └── bronze_to_silver.py         # SilverNewsItem, BronzeToSilverTransformer
│   │                                   # HTML clean → keyword extract → entity extract →
│   │                                   # sentiment → quality score → dedup
│   │
│   ├── aggregation/
│   │   └── script_generator.py         # GroqProvider, AnthropicProvider, GeminiProvider,
│   │                                   # FallbackProvider, ScriptGenerator
│   │                                   # Source-aware prompt engineering, SSML generation
│   │
│   ├── podcast/
│   │   ├── tts_generator.py            # TTSGenerator (Google Cloud TTS REST API)
│   │   │                               # MockTTSGenerator (for local testing)
│   │   └── rss_publisher.py            # RSSPublisher — RSS 2.0 + iTunes extensions
│   │                                   # GCS upload, episode numbering, feed management
│   │
│   └── utils/
│       └── secrets.py                  # SecretsManager: widget params → secret scope → env vars → .env
│                                       # All 9 secrets tracked with masking for safe logging
│
├── notebooks/                          # Pipeline notebooks (executed as serverless tasks)
│   ├── 01_bronze_ingestion.py          # Register 9 data sources, stream to Delta + JSON export
│   ├── 02_silver_transformation.py     # BronzeToSilverTransformer, write to JSON + Delta
│   ├── 03_gold_aggregation.py          # Top story selection, diversity balancing
│   ├── 04_script_generation.py         # LLM script + SSML generation
│   ├── 05_audio_generation.py          # TTS → MP3
│   ├── 06_publish_podcast.py           # GCS upload + RSS feed update
│   └── dlt_pipeline.py                 # Spark Declarative Pipeline (analytics branch only)
│
├── tests/
│   ├── test_data_sources.py            # NewsItem, RateLimiter, HackerNews, RSS unit tests
│   ├── test_transformations.py         # BronzeToSilverTransformer, quality scoring, dedup
│   ├── test_script_generator.py        # Script generation, provider fallback
│   └── test_podcast.py                 # TTS mock, RSS feed structure
│
├── setup.py                            # Package definition, extras: [gcp], [dev]
├── requirements.txt                    # Runtime dependencies
├── .env.template                       # Environment variable template
└── .gitignore
```

---

## Configuration Reference

Bundle variables are defined in `databricks.yml` and passed to tasks as widget parameters. Override any variable at deploy time with `--var="key=value"`.

| Variable | Default | Description |
|---|---|---|
| `data_path` | `/Volumes/news_pipeline/default/podcast_data` (prod) | Base path for all JSON and audio files |
| `volume_path` | `/Volumes/news_pipeline/default/podcast_data` | Unity Catalog Volume path |
| `podcast_title` | `Daily Databricks Digest` | Podcast name shown in RSS feed |
| `groq_api_key` | `` | Groq API key (primary LLM) |
| `claude_api_key` | `` | Anthropic Claude API key |
| `google_api_key` | `` | Google Gemini API key |
| `gcp_service_account_json` | `` | GCP service account JSON (single line, for TTS + GCS) |
| `gcs_bucket_name` | `` | GCS bucket name (without `gs://` prefix) |
| `audio_base_url` | `` | Public base URL for audio files (e.g. `https://storage.googleapis.com/your-bucket`) |
| `reddit_client_id` | `` | Reddit OAuth client ID |
| `reddit_client_secret` | `` | Reddit OAuth client secret |
| `youtube_api_key` | `` | YouTube Data API key |

**Job parameters** (settable per-run via `--param`):

| Parameter | Default | Description |
|---|---|---|
| `days_back` | `1` | How many days of news to look back when fetching |
| `max_stories` | `10` | Maximum stories to include in the episode |

---

## Secrets Reference

Secrets are resolved in this priority order:

1. **Databricks Widget parameters** — injected by the job's `base_parameters` (highest priority in production)
2. **Databricks Secret Scope** — `daily-podcast` scope, key = the env var name
3. **Environment variables** — set in your shell or `.env` file
4. **`.env` file** — loaded automatically if present in the project root

| Secret name | Env var | Required | Where to get it |
|---|---|---|---|
| `groq_api_key` | `GROQ_API_KEY` | Recommended | [console.groq.com](https://console.groq.com) |
| `claude_api_key` | `CLAUDE_API_KEY` | Optional | [console.anthropic.com](https://console.anthropic.com) |
| `google_api_key` | `GOOGLE_API_KEY` | Optional | Google Cloud Console → Credentials |
| `gcp_service_account` | `GCP_SERVICE_ACCOUNT_JSON` | Yes (for audio) | GCP IAM → Service Accounts → Create key (JSON) |
| `gcs_bucket_name` | `GCS_BUCKET_NAME` | Yes (for publish) | GCP Cloud Storage bucket name |
| `audio_base_url` | `AUDIO_BASE_URL` | Yes (for publish) | `https://storage.googleapis.com/<bucket>` |
| `reddit_client_id` | `REDDIT_CLIENT_ID` | Optional | [reddit.com/prefs/apps](https://www.reddit.com/prefs/apps) |
| `reddit_client_secret` | `REDDIT_CLIENT_SECRET` | Optional | Same as above |
| `youtube_api_key` | `YOUTUBE_API_KEY` | Optional | Google Cloud Console → YouTube Data API v3 |

**GCP Service Account minimum required roles:**
- `roles/texttospeech.user` — for Text-to-Speech
- `roles/storage.objectAdmin` — for GCS bucket read/write

---

## Voice Configuration

The podcast uses **Google Cloud Neural2** voices for natural-sounding speech.

| Segment | Voice | Gender |
|---|---|---|
| Intro | `en-US-Neural2-F` | Female |
| Story 1, 3, 5, 7, 9 | `en-US-Neural2-D` | Male |
| Story 2, 4, 6, 8, 10 | `en-US-Neural2-F` | Female |
| Outro | `en-US-Neural2-F` | Female |

SSML `<break time="500ms"/>` pauses are automatically inserted between story segments for a natural listening experience.

To change voices, update the `VOICE_CONFIGS` dict in `src/daily_databricks_feed/podcast/tts_generator.py`. Any [Google Cloud TTS voice](https://cloud.google.com/text-to-speech/docs/voices) is supported.

---

## Podcast Distribution

After your first episode is generated and `feed.xml` is live on GCS:

1. **Spotify for Podcasters** — Submit at `podcasters.spotify.com` → Add podcast via RSS URL
2. **Apple Podcasts Connect** — Submit at `podcastsconnect.apple.com` → Add Show → RSS Feed
3. **Amazon Music / Audible** — Submit at `podcasters.amazon.com`
4. **Pocket Casts, Overcast, etc.** — Any app that accepts an RSS URL works immediately

Your RSS feed URL will be:
```
https://storage.googleapis.com/<your-bucket>/feed.xml
```

All platforms poll this URL on their own schedule (typically every 1–24 hours). New episodes appear automatically without any manual action.

**iTunes metadata included in the feed:**
- `<itunes:author>`, `<itunes:category>`, `<itunes:explicit>no`
- `<itunes:image>` (add your cover art to GCS for Apple Podcasts compliance)
- `<itunes:duration>` per episode
- Episode number and season tracking

---

## Cost Estimates

### Databricks (monthly, daily runs)

| Resource | Usage | Estimate |
|---|---|---|
| Serverless compute (7 tasks × ~5 min) | ~35 DBU/day | Varies by tier — typically $2–8/month |
| Unity Catalog storage (Delta tables) | ~500 MB/month growth | Included in workspace |
| UC Volume storage (JSON + MP3 files) | ~150 MB/month | Included in workspace |

### External services (monthly)

| Service | Usage | Cost |
|---|---|---|
| Google Cloud TTS Neural2 | ~150K chars/month | ~$0.60 |
| Google Cloud Storage | ~150 MB/month | ~$0.003 |
| Groq API | ~30 requests/month | **Free** |
| Reddit API | ~30 calls/month | **Free** |
| YouTube Data API | ~30 units/month | **Free** |
| **Total external** | | **~$0.61/month** |

> GCP TTS free tier: 1M WaveNet characters/month. Neural2 voices are charged from the first character (~$0.000004/char). 150K chars/month ≈ $0.60.

---

## Troubleshooting

### Bronze: No news items fetched

```
Records written this run: 0
```

- Check that `DAYS_BACK` is at least `1`. Set to `7` for a richer first run.
- Verify API credentials are loaded: look for `SecretsManager status` log lines at startup.
- Reddit and YouTube sources are skipped (not failed) if their keys are missing — check for `WARNING: REDDIT_CLIENT_ID not set` in logs.
- Check API rate limits — wait an hour and retry if HN/Stack Overflow quotas are hit.

### Bronze: Checkpoint issues

```
StreamingQueryException: checkpoint is incompatible
```

Delete the checkpoint and run again (first run will re-fetch):

```bash
# Databricks CLI
databricks fs rm -r dbfs:/tmp/checkpoints/bronze_ingestion
# or delete the checkpoint directory in your Volume
```

### Silver: Empty output after transformation

- Ensure `bronze_news_raw.json` exists at `DATA_PATH`. The Volume path must match between tasks.
- If quality filtering is too aggressive, temporarily lower `min_quality` in `03_gold_aggregation.py` or increase `DAYS_BACK`.

### DLT Pipeline: `ColumnNotFound: _ingestion_date`

This column is added in `silver_news`. If you see this error on an existing pipeline:

```bash
# Do a full refresh to reprocess all bronze records
databricks pipelines start --full-refresh --pipeline-id <your-pipeline-id>
```

### Script generation: Falls back to template

```
WARNING: Using fallback template-based generation
```

- Confirm `GROQ_API_KEY` is set and being loaded (check `secrets.print_status()` output).
- Verify the key is valid at `console.groq.com`.
- Check the `script_generation` task logs for the actual API error response.

### Audio generation: `google.auth.exceptions.DefaultCredentialsError`

- Ensure `GCP_SERVICE_ACCOUNT_JSON` contains a valid single-line JSON string.
- Verify the service account has `roles/texttospeech.user`.
- For local testing without GCP: set `USE_MOCK_TTS=true`.

### Audio generation: `ffmpeg not found`

This pipeline does **not** use ffmpeg. MP3 segments are concatenated as raw bytes. If you see this error it is coming from `pydub` — ensure `pydub` is not imported anywhere in the audio path.

### Publish: RSS feed not updating

- Check that `GCS_BUCKET_NAME` and `AUDIO_BASE_URL` are set correctly.
- Verify the GCS bucket has public read access for `feed.xml` and the `episodes/` prefix.
- For a dry-run without uploading: set `DRY_RUN=true`.

### Databricks bundle deploy fails

```bash
# Validate config before deploying
databricks bundle validate -t prod

# Common fix: ensure Unity Catalog catalog/schema exist (see Step 3 above)
# Common fix: SERVICE_PRINCIPAL_NAME env var must be set for staging/prod run_as
```

---

## Development Guide

### Running tests

```bash
pip install -e ".[dev]"

# All tests
pytest tests/ -v

# With coverage
pytest tests/ -v --cov=src --cov-report=term-missing

# Single module
pytest tests/test_transformations.py -v
```

### Code style

```bash
# Format (line length 100)
black src/ notebooks/ tests/ --line-length 100

# Lint
flake8 src/ --max-line-length 100

# Type check
mypy src/
```

### Adding a new data source

1. Create `src/daily_databricks_feed/data_sources/my_source.py` implementing `BaseDataSource`
2. Create `src/daily_databricks_feed/data_sources/pyspark_sources/my_source_source.py` implementing `DataSource` + `DataSourceStreamReader`
3. Register in `notebooks/01_bronze_ingestion.py`:
   ```python
   from daily_databricks_feed.data_sources.pyspark_sources import MySource
   spark.dataSource.register(MySource)
   ```
4. Add a `spark.readStream.format("my_source_format").option(...).load()` block
5. Add tests in `tests/test_data_sources.py`

### Modifying the prompt / script structure

Edit `src/daily_databricks_feed/aggregation/script_generator.py`:
- `_build_prompt()` — controls the LLM instruction and story formatting per source
- `TARGET_WORD_COUNT` — adjust for longer/shorter episodes
- `WORDS_PER_MINUTE` — adjust estimated duration calculation

### Environment targets

| Target | Mode | Purpose |
|---|---|---|
| `dev` | development | Personal workspace path, your user identity, pipeline in development mode |
| `staging` | production | Shared workspace path, service principal, pipeline in production mode |
| `prod` | production | Shared workspace, service principal, Volume as data path |
| `local` | development | Local filesystem (`./data`) for running notebooks directly |

---

## License

MIT License — see [LICENSE](LICENSE) for details.

---

## Acknowledgments

- [Databricks](https://databricks.com) for the lakehouse platform this pipeline runs on
- [Groq](https://groq.com) for fast, free LLM inference (Llama 3.3 70B)
- [Hacker News Algolia API](https://hn.algolia.com/api) for free, reliable news search
- [Google Cloud Text-to-Speech](https://cloud.google.com/text-to-speech) for Neural2 voices
- [PRAW](https://praw.readthedocs.io) for the Reddit API wrapper
- [feedparser](https://feedparser.readthedocs.io) for RSS/Atom parsing
