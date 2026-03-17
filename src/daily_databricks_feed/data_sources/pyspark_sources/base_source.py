"""Shared schema, partition type, and base stream reader for PySpark 4.0 Custom Data Source API.

All four news stream readers (Hacker News, Reddit, YouTube, RSS) inherit from
BaseNewsStreamReader.

Offset model
────────────
Offsets are Unix epoch integers.  The checkpoint stores the end-epoch of the
last successfully processed window.  On the next `availableNow` run, Spark
passes that as `start`; `latestOffset()` returns the current time as `end`.
This gives clean incremental ingestion — each run fetches only content
published since the previous run.

BRONZE_SCHEMA column order MUST match the tuple order yielded by every
reader's `_fetch_rows` implementation.
"""

import math
import time
from dataclasses import dataclass

from pyspark.sql.datasource import DataSourceStreamReader, InputPartition
from pyspark.sql.types import LongType, StringType, StructField, StructType

# ---------------------------------------------------------------------------
# Canonical schema shared by all bronze news sources
# ---------------------------------------------------------------------------
BRONZE_SCHEMA = StructType(
    [
        StructField("id",              StringType(), nullable=False),
        StructField("source",          StringType(), nullable=False),
        StructField("title",           StringType(), nullable=False),
        StructField("url",             StringType(), nullable=False),
        StructField("content",         StringType(), nullable=True),
        StructField("author",          StringType(), nullable=True),
        StructField("published_at",    StringType(), nullable=True),
        StructField("fetched_at",      StringType(), nullable=False),
        StructField("score",           LongType(),   nullable=True),
        StructField("comments_count",  LongType(),   nullable=True),
        StructField("tags",            StringType(), nullable=True),   # JSON array
        StructField("metadata",        StringType(), nullable=True),   # JSON object
        StructField("_ingested_at",    StringType(), nullable=False),
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
    end_epoch:   int


# ---------------------------------------------------------------------------
# Base stream reader
# ---------------------------------------------------------------------------
class BaseNewsStreamReader(DataSourceStreamReader):
    """
    Common offset / partition logic for all API-backed news stream readers.

    Subclasses must implement:
        _fetch_rows(start_epoch, end_epoch, days_back) -> Iterator[tuple]

    Each yielded tuple must follow BRONZE_SCHEMA column ordering exactly.
    """

    _DEFAULT_DAYS_BACK: int = 1

    def __init__(self, options: dict) -> None:
        self.options  = options
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
        Derives `days_back` from the time window, then delegates to `_fetch_rows`.
        """
        assert isinstance(partition, TimeRangePartition)
        days_back = max(1, math.ceil(
            (partition.end_epoch - partition.start_epoch) / 86_400
        ))
        yield from self._fetch_rows(partition.start_epoch, partition.end_epoch, days_back)

    def _fetch_rows(self, start_epoch: int, end_epoch: int, days_back: int):
        """Yield BRONZE_SCHEMA-ordered tuples for the given time window."""
        raise NotImplementedError


# ---------------------------------------------------------------------------
# Shared helper
# ---------------------------------------------------------------------------
def item_to_tuple(item, now_str: str, today_str: str) -> tuple:
    """
    Convert a NewsItem to a BRONZE_SCHEMA-ordered tuple.
    Shared by all four readers so column ordering is guaranteed to be consistent.
    """
    import json

    d = item.to_dict()
    return (
        str(d.get("id",             "")),
        str(d.get("source",         "")),
        str(d.get("title",          "")),
        str(d.get("url",            "")),
        d.get("content"),
        d.get("author"),
        str(d["published_at"]) if d.get("published_at") else None,
        str(d.get("fetched_at") or now_str),
        int(d.get("score",          0) or 0),
        int(d.get("comments_count", 0) or 0),
        json.dumps(d.get("tags",     [])),
        json.dumps(d.get("metadata", {})),
        now_str,
        today_str,
    )
