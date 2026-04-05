# Daily Databricks Digest

A fully automated daily podcast pipeline that aggregates Databricks and data engineering news from multiple sources, generates a natural-sounding script using LLMs, converts it to audio via Google Cloud Text-to-Speech, and publishes a podcast-ready RSS feed — all running on **Databricks Serverless** on a daily schedule.

## Architecture

```
GitHub Actions (6 AM UTC daily)
        ↓
[01 Bronze]  Fetch from 7–9 APIs → raw JSON
        ↓
[02 Silver]  Clean, dedupe, extract keywords
        ↓
[03 Gold]    Select top 10 stories
        ↓
[04 Script]  Generate via LLM (Groq / Claude / Gemini)
        ↓
[05 Audio]   Google Cloud TTS → MP3
        ↓
[06 Publish] Upload to GCS, update RSS feed
```

## Project Structure

```
daily_databricks_feed/
├── databricks.yml              # Databricks Asset Bundle config
├── resources/
│   ├── jobs.yml                # Workflow & task definitions
│   └── pipeline.yml            # DLT pipeline (optional)
├── src/daily_databricks_feed/
│   ├── data_sources/           # API integrations (HN, Reddit, YouTube, RSS…)
│   ├── transformations/        # Bronze → Silver cleaning & deduplication
│   ├── aggregation/            # Story scoring & selection
│   ├── podcast/
│   │   ├── tts_generator.py    # Google Cloud TTS (Standard voices)
│   │   └── rss_publisher.py    # RSS 2.0 + iTunes extensions
│   └── utils/
│       └── secrets.py          # Multi-source credential loader
├── notebooks/                  # Pipeline notebooks 01–06
├── tests/
└── .github/workflows/
    ├── ci.yml                  # Lint, test, validate bundle
    └── deploy.yml              # Daily deploy + run
```

## Data Sources

| Source | How it fetches | Auth | Filter strategy |
|--------|---------------|------|-----------------|
| **Hacker News** | Algolia Search API — searches by date | None | Keyword match on title + content |
| **Reddit** | PRAW — polls `r/databricks` + `r/dataengineering` | OAuth 2.0 | Keyword filter on posts |
| **YouTube** | YouTube Data API v3 — searches relevant videos | API Key | Keyword match on title + description |
| **RSS Feeds** | feedparser — polls 5 official blogs (Databricks, Delta, MLflow, Spark, Medium) | None | Always relevant (curated feeds) |
| **GitHub Releases** | GitHub REST API — watches databricks-sdk, MLflow, Delta repos | Token (optional) | Always relevant (curated repos) |
| **PyPI** | PyPI JSON API — polls `databricks-*` package versions | None | Package name whitelist |
| **Stack Overflow** | Stack Exchange API — questions tagged `databricks` | Key (optional) | Tag-based, already filtered |
| **Dev.to** | Forem REST API — articles tagged `databricks` | None | Tag + keyword filter |
| **Databricks Community** | Discourse REST API — latest + top posts from community.databricks.com | None | Already on-topic domain |

Reddit and YouTube are optional — the pipeline runs with 7 sources if those credentials are missing.

### Keyword filter

All sources that pull from general feeds (Hacker News, Reddit, YouTube, Dev.to) run every item through a ~80-keyword filter before passing it downstream. Keywords cover:

- **Databricks platform**: delta lake, unity catalog, mlflow, lakeflow, photon, medallion architecture
- **Pipelines**: delta live tables (DLT), lakeflow pipelines, spark declarative pipelines (SDP), serverless DLT
- **Bundles**: databricks asset bundle (DAB), databricks automation bundle, declarative automation bundle
- **Latest releases**: lakebase, lakebridge, databricks apps, databricks connect, model serving, DBRX, AI functions, vector search
- **Open table formats**: Apache Iceberg, Apache Hudi, Apache Paimon
- **AI / LLM**: claude, anthropic, gpt, llama, mistral, rag, embeddings, fine-tuning, mosaic AI
- **Cloud data services**: Azure Synapse, Microsoft Fabric, AWS Glue, BigQuery, Snowflake
- **Data ecosystem**: dbt, Apache Kafka, Apache Flink, data mesh, data governance

## LLM Providers

| Provider | Free tier | Model used |
|----------|-----------|------------|
| **Groq** (recommended) | 14,400 req/day | Llama 3.1 70B |
| Anthropic Claude | Limited | Claude 3 Haiku |
| Google Gemini | 60 req/min | Gemini 1.5 Flash |
| Template fallback | Unlimited | Rule-based (no API needed) |

## Voice Configuration

Uses Google Cloud TTS **Standard voices** (free tier: 4 million characters/month).

| Segment | Voice |
|---------|-------|
| Intro / Outro | `en-US-Standard-F` (female) |
| Male host | `en-US-Standard-D` (male) |
| Female casual | `en-US-Standard-C` (female) |
| Male casual | `en-US-Standard-B` (male) |

## Setup

### 1. Prerequisites

- Databricks workspace with Unity Catalog enabled
- Databricks CLI v0.269+ installed and authenticated
- GCP project with billing account linked

### 2. Obtain API Credentials

**Google Cloud (TTS + GCS) — required**

1. Go to [console.cloud.google.com](https://console.cloud.google.com) → select your project
2. Enable **Cloud Text-to-Speech API**: APIs & Services → Enable APIs → search "Cloud Text-to-Speech API"
3. Create a service account: IAM & Admin → Service Accounts → Create → assign role **Cloud Text-to-Speech API User**
4. Download the JSON key: service account → Keys tab → Add Key → JSON
5. Enable **Cloud Storage API** if using GCS for audio hosting

**Reddit — optional**

1. Go to [reddit.com/prefs/apps](https://www.reddit.com/prefs/apps) → Create App
2. Type: **script** | Redirect URI: `http://localhost`
3. Note the `client_id` (under app name) and `client_secret`

**YouTube — optional**

1. In GCP Console → APIs & Services → Enable **YouTube Data API v3**
2. Credentials → Create Credentials → **API Key**
3. Restrict the key to YouTube Data API v3 (recommended)

**Groq — recommended for script generation**

1. Sign up at [console.groq.com](https://console.groq.com) → API Keys → Create

### 3. Store secrets in Databricks

Create the secret scope once:

```bash
databricks secrets create-scope daily-podcast --profile <your-profile>
```

Store each credential:

```bash
# GCP service account (paste the full JSON as a single value)
databricks secrets put-secret daily-podcast GCP_SERVICE_ACCOUNT_JSON \
  --string-value "$(cat /path/to/service-account-key.json)" --profile <your-profile>

databricks secrets put-secret daily-podcast REDDIT_CLIENT_ID     --string-value "..."  --profile <your-profile>
databricks secrets put-secret daily-podcast REDDIT_CLIENT_SECRET --string-value "..."  --profile <your-profile>
databricks secrets put-secret daily-podcast YOUTUBE_API_KEY      --string-value "..."  --profile <your-profile>
databricks secrets put-secret daily-podcast GROQ_API_KEY         --string-value "..."  --profile <your-profile>
databricks secrets put-secret daily-podcast GCS_BUCKET_NAME      --string-value "..."  --profile <your-profile>
databricks secrets put-secret daily-podcast AUDIO_BASE_URL       --string-value "..."  --profile <your-profile>
```

Verify:

```bash
databricks secrets list-secrets daily-podcast --profile <your-profile>
```

### 4. Add GitHub Actions secrets

Go to repository Settings → Secrets and variables → Actions, and add:

| Secret | Description |
|--------|-------------|
| `DATABRICKS_HOST` | e.g. `https://dbc-xxxxx.cloud.databricks.com` |
| `DATABRICKS_TOKEN` | Personal access token or service principal token |
| `GCP_SERVICE_ACCOUNT_JSON` | Full contents of the downloaded JSON key file |
| `GCS_BUCKET_NAME` | GCS bucket for audio + RSS hosting |
| `AUDIO_BASE_URL` | Public base URL for audio files |
| `GROQ_API_KEY` | Groq API key |
| `REDDIT_CLIENT_ID` | Reddit app client ID |
| `REDDIT_CLIENT_SECRET` | Reddit app secret |
| `YOUTUBE_API_KEY` | YouTube Data API key |
| `CLAUDE_API_KEY` | Anthropic API key (optional fallback) |
| `GOOGLE_API_KEY` | Google Gemini API key (optional fallback) |

### 5. Deploy and run

```bash
# Deploy the bundle
databricks bundle deploy --target prod --profile <your-profile>

# Trigger a manual run
databricks bundle run daily_databricks_podcast --target prod --profile <your-profile>
```

The pipeline also runs automatically at **6 AM UTC daily** via GitHub Actions, or trigger it manually from the Actions tab.

## Cost Estimate (monthly, 30 daily runs)

| Service | Usage | Cost |
|---------|-------|------|
| GitHub Actions | ~300 min/month | Free (2,000 min included) |
| Databricks Serverless | ~30 runs × ~10 min | Varies by DBU rate |
| Groq API | 30 requests | Free |
| Google Cloud TTS (Standard) | ~150K chars | **Free** (under 4M limit) |
| GCS Storage | ~150 MB | ~$0.01 |

## Troubleshooting

**Checkpoint source count mismatch (bronze streaming)**
The Spark checkpoint was saved with a different number of active sources. Delete the checkpoint to reset:
```bash
# In a Databricks notebook
dbutils.fs.rm("/Volumes/news_pipeline/default/podcast_data/checkpoints/bronze_ingestion", recurse=True)
```

**MockTTSGenerator used instead of real TTS**
The `GCP_SERVICE_ACCOUNT_JSON` secret is missing or empty. Verify with `databricks secrets list-secrets daily-podcast` and re-run the put-secret command.

**Reddit / YouTube streams skipped**
Expected if those API keys are not configured. The pipeline continues with the remaining 7 sources.

**Script generation fails**
At least one LLM key (`GROQ_API_KEY`, `CLAUDE_API_KEY`, or `GOOGLE_API_KEY`) must be set. The pipeline falls back to template-based generation if all LLM calls fail.

## Development

```bash
pip install -e ".[dev]"
pytest tests/ -v
black src/ --line-length 100
flake8 src/
```

## License

MIT — see [LICENSE](LICENSE) for details.
