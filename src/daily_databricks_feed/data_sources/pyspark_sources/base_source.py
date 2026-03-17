"""Shared schema, partition type, and base stream reader for PySpark 4.0 Custom Data Source API.

All four news stream readers (Hacker News, Reddit, YouTube, RSS) inherit from
BaseNewsStreamReader.

Offset model
────────────
Offsets are Unix epoch integers.  The checkpoint stores the end-epoch of the
last successfully processed window.  On the next `availableNow` run, Spark
passes that as `start`; `latestOffset()` returns the current time as `end`.

Each `_fetch_items(start_epoch, end_epoch)` implementation MUST pass `start_epoch`
directly to the underlying API as the 'published after' timestamp.  This ensures
the checkpoint boundary is honoured and prevents the same article from appearing
in more than one micro-batch.

`read()` applies a secondary published_at window filter and deduplicates by item
ID so that minor API clock skew or pagination overlap cannot cause cross-batch
duplicates.

BRONZE_SCHEMA column order MUST match the tuple order yielded by item_to_tuple.
"""

import time
from dataclasses import dataclass
from datetime import datetime, timezone

from pyspark.sql.datasource import DataSourceStreamReader, InputPartition
from pyspark.sql.types import LongType, StringType, StructField, StructType

# ---------------------------------------------------------------------------
# Canonical schema shared by all bronze news sources
# ---------------------------------------------------------------------------
BRONZE_SCHEMA = StructType(
    [
        StructField("id", StringType(), nullable=False),
        StructField("source", StringType(), nullable=False),
        StructField("title", StringType(), nullable=False),
        StructField("url", StringType(), nullable=False),
        StructField("content", StringType(), nullable=True),
        StructField("author", StringType(), nullable=True),
        StructField("published_at", StringType(), nullable=True),
        StructField("fetched_at", StringType(), nullable=False),
        StructField("score", LongType(), nullable=True),
        StructField("comments_count", LongType(), nullable=True),
        StructField("tags", StringType(), nullable=True),  # JSON array
        StructField("metadata", StringType(), nullable=True),  # JSON object
        StructField("_ingested_at", StringType(), nullable=False),
        StructField("_ingestion_date", StringType(), nullable=False),
    ]
)


# ---------------------------------------------------------------------------
# Partition
# ---------------------------------------------------------------------------
@dataclass
class TimeRangePartition(InputPartition):
    """Half-open time window [start_epoch, end_epoch) for a single API fetch."""

    start_epoch: int
    end_epoch: int


# ---------------------------------------------------------------------------
# Base stream reader
# ---------------------------------------------------------------------------
class BaseNewsStreamReader(DataSourceStreamReader):
    """
    Common offset / partition logic for all API-backed news stream readers.

    Subclasses must implement:
        _fetch_items(start_epoch, end_epoch) -> Iterator[NewsItem]

    The implementation MUST pass start_epoch as the 'published after' timestamp
    to the underlying API so that only content newer than the checkpoint is
    requested.  read() enforces the window boundary and deduplicates by ID as a
    safety net against API clock skew or pagination overlap.
    """

    _DEFAULT_DAYS_BACK: int = 1

    def __init__(self, options: dict) -> None:
        self.options = options
        self._days_back = int(options.get("days_back", self._DEFAULT_DAYS_BACK))

    # ── Offset contract ──────────────────────────────────────────────────────

    def initialOffset(self) -> dict:
        """On first run (no checkpoint), go back `days_back` days."""
        return {"epoch": int(time.time()) - self._days_back * 86_400}

    def latestOffset(self) -> dict:
        """Latest available data is always 'right now'."""
        return {"epoch": int(time.time())}

    def partitions(self, start: dict, end: dict) -> list:
        """Return a single partition covering [start, end)."""
        if start["epoch"] >= end["epoch"]:
            return []
        return [TimeRangePartition(start_epoch=start["epoch"], end_epoch=end["epoch"])]

    def commit(self, end: dict) -> None:
        """Checkpoint advancement is handled by Spark; nothing extra needed here."""

    # ── Read ─────────────────────────────────────────────────────────────────

    def read(self, partition: InputPartition):
        """
        Called per partition (potentially on an executor).

        Delegates to _fetch_items, then applies two safety filters before
        yielding BRONZE_SCHEMA-ordered tuples:

        1. Window filter  — drops items whose published_at falls outside
           [start_epoch, end_epoch).  Items with no timestamp are kept
           (conservative: never silently discard content).
        2. ID dedup       — skips any item ID already seen in this batch,
           preventing duplicates from overlapping API queries.
        """
        assert isinstance(partition, TimeRangePartition)
        now_str = datetime.now(timezone.utc).isoformat()
        today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        seen_ids: set = set()

        for item in self._fetch_items(partition.start_epoch, partition.end_epoch):
            # ── Intra-batch dedup ─────────────────────────────────────────
            if item.id in seen_ids:
                continue

            # ── Checkpoint window filter ──────────────────────────────────
            # Items without published_at pass through (no data to filter on).
            if item.published_at is not None:
                ts = item.published_at.timestamp()
                if ts < partition.start_epoch or ts >= partition.end_epoch:
                    continue

            seen_ids.add(item.id)
            yield item_to_tuple(item, now_str, today_str)

    def _fetch_items(self, start_epoch: int, end_epoch: int):
        """
        Fetch NewsItem objects for the half-open window [start_epoch, end_epoch).

        MUST pass start_epoch to the underlying API as the minimum publish
        timestamp (i.e. 'published after') so that only content newer than the
        previous checkpoint is requested.  end_epoch is provided for APIs that
        support an upper bound, but at minimum start_epoch must be honoured.
        """
        raise NotImplementedError


# ---------------------------------------------------------------------------
# Shared helper
# ---------------------------------------------------------------------------
def item_to_tuple(item, now_str: str, today_str: str) -> tuple:
    """
    Convert a NewsItem to a BRONZE_SCHEMA-ordered tuple.
    Called by BaseNewsStreamReader.read() after window filtering and dedup.
    """
    import json

    d = item.to_dict()
    return (
        str(d.get("id", "")),
        str(d.get("source", "")),
        str(d.get("title", "")),
        str(d.get("url", "")),
        d.get("content"),
        d.get("author"),
        str(d["published_at"]) if d.get("published_at") else None,
        str(d.get("fetched_at") or now_str),
        int(d.get("score", 0) or 0),
        int(d.get("comments_count", 0) or 0),
        json.dumps(d.get("tags", [])),
        json.dumps(d.get("metadata", {})),
        now_str,
        today_str,
    )
