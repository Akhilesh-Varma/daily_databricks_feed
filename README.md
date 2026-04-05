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

### What each source fetches

| Source | Fields fetched | What becomes `content` |
|--------|---------------|------------------------|
| Hacker News | Title, URL, score, comment count | Story text or linked article excerpt |
| Reddit | Title, selftext, score, comment count | Post body text |
| **YouTube** | Title, description, view/like/comment counts | **Video description only — no transcript** |
| RSS Feeds | Title, summary, published date | Article summary / first paragraphs |
| GitHub Releases | Tag name, release notes, published date | Release notes body |
| PyPI | Package name, version, release date | Package description |
| Stack Overflow | Question title, body, tags, vote count | Question body |
| Dev.to | Title, description, tags, reactions | Article description |
| Databricks Community | Topic title, excerpt, reply/view counts | Post excerpt |

> **YouTube limitation**: The YouTube Data API v3 does not provide video transcripts. Only the title and description box are available. To ingest full transcripts, `youtube-transcript-api` would need to be added as a separate enrichment step.

### Fetch timeline

Each source has two modes that determine how far back it looks:

**Mode 1 — Incremental (normal daily runs)**
Spark Structured Streaming passes the exact Unix timestamp of the last checkpoint to each source. Only content published after that point is fetched — no duplicates, no gaps.

**Mode 2 — Backfill (first run or after checkpoint reset)**
When no checkpoint exists, each source falls back to a fixed `days_back` window. This is also overridable at runtime via `--params days_back=7`.

| Source | Default `days_back` | Rationale |
|--------|-------------------|-----------|
| Hacker News | 1 day | High volume — daily cadence is sufficient |
| Reddit | 1 day | Active community, posts age quickly |
| YouTube | 7 days | Videos published less frequently |
| RSS Feeds | 7 days | Blog posts are infrequent |
| GitHub Releases | 7 days | Releases don't happen daily |
| PyPI | 7 days | Package releases are infrequent |
| Stack Overflow | 1 day | High question volume |
| Dev.to | 1 day | Active daily publishing |
| Databricks Community | 1 day | Forum is high-traffic |

> After deleting the checkpoint or on first run, use `--params days_back=7` to backfill a full week across all sources.

### Keyword filter

Sources pulling from general feeds (Hacker News, Reddit, YouTube, Dev.to) run every item through an ~80-keyword filter before passing it downstream. Curated sources (RSS, GitHub, PyPI, Discourse, Stack Overflow by tag) skip this filter as they are already on-topic.

Keywords cover:

- **Databricks platform**: delta lake, unity catalog, mlflow, lakeflow, photon, medallion architecture
- **Pipelines**: delta live tables (DLT), lakeflow pipelines, spark declarative pipelines (SDP), serverless DLT, declarative pipelines
- **Bundles**: databricks asset bundle (DAB), databricks automation bundle, declarative automation bundle
- **Latest releases**: lakebase, lakebridge, databricks apps, databricks connect, model serving, DBRX, AI functions, vector search
- **Open table formats**: Apache Iceberg, Apache Hudi, Apache Paimon
- **AI / LLM**: claude, anthropic, gpt, llama, mistral, rag, embeddings, fine-tuning, mosaic AI
- **Cloud data services**: Azure Synapse, Microsoft Fabric, AWS Glue, BigQuery, Snowflake
- **Data ecosystem**: dbt, Apache Kafka, Apache Flink, data mesh, data governance

## Story Selection & Scoring

### How sources are combined

There is no traditional join. Every source outputs records with the same flat schema (`id`, `source`, `title`, `url`, `content`, `score`, `comments_count`, `published_at`). All 9 sources are merged into a single flat list, written to the `bronze_raw_landing` Delta table, and processed identically through the Silver and Gold layers.

### Quality score

Each record is assigned a `quality_score` (0.0–1.0) in the Silver layer. This is the primary sort key used to select the top stories.

| Component | Max | How it's calculated |
|-----------|-----|---------------------|
| Title length | 0.20 | +0.1 if >20 chars, +0.1 if >50 chars |
| Content length | 0.20 | +0.1 if >100 chars, +0.1 if >500 chars |
| Keyword density | 0.20 | `min(keyword_count / 5, 1.0) × 0.2` |
| Social proof | 0.20 | Log scale of upvotes + comments |
| Source credibility | 0.20 | Hard-coded per source (see below) |

### Source credibility (implicit priority order)

| Source | Credibility score | Why |
|--------|------------------|-----|
| RSS Feeds (official blogs) | **0.20** | Primary announcements from Databricks, Delta, MLflow |
| Hacker News | 0.15 | High-quality community curation |
| YouTube | 0.15 | Official tutorials and announcements |
| Reddit | 0.10 | Community discussion |
| All others (PyPI, GitHub, Stack Overflow, Dev.to, Discourse) | 0.10 | Supplementary signals |

A highly upvoted Reddit post can still outscore a low-engagement blog post via social proof — the credibility score is a starting advantage, not a ceiling.

### Diversity penalty

After scoring, `select_top_stories()` applies a diversity penalty of `0.3 × times_source_already_selected` to prevent one source from dominating all slots. A story's effective score is:

```
effective_score = quality_score − (0.3 × times_source_already_picked)
```

A story is included if `effective_score > 0.2`, or until at least `max_stories / 2` slots are filled (guarantees content on slow news days).

### Podcast flow ordering

Selected stories are reordered before being sent to the LLM for a natural podcast flow:

1. **Releases** — GitHub releases, PyPI packages
2. **Official content** — RSS blog posts
3. **Community discussion** — Hacker News, Reddit, Stack Overflow, Databricks Community
4. **Tutorials & video** — Dev.to, YouTube

## LLM Providers

Providers are tried in order — first available one is used.

| Priority | Provider | Model | Free tier |
|----------|----------|-------|-----------|
| 1 | **Anthropic Claude** (primary) | claude-sonnet-4-6 | Limited free tier |
| 2 | Groq | Llama 3.1 70B | 14,400 req/day |
| 3 | Google Gemini | Gemini 1.5 Flash | 60 req/min |
| 4 | Template fallback | Rule-based | Unlimited (no API needed) |

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
