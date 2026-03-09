"""Base utilities for PySpark Data Sources."""

from typing import List

# Databricks-related keywords for filtering
DATABRICKS_KEYWORDS = [
    "databricks",
    "delta lake",
    "delta-lake",
    "deltalake",
    "spark",
    "apache spark",
    "lakehouse",
    "mlflow",
    "unity catalog",
    "dbx",
    "data engineering",
    "data lakehouse",
    "photon",
    "databricks sql",
    "delta sharing",
    "delta live tables",
    "dlt",
    "medallion architecture",
]


def is_databricks_related(text: str) -> bool:
    """
    Check if text contains Databricks-related keywords.

    Args:
        text: Text to check

    Returns:
        True if contains relevant keywords
    """
    if not text:
        return False
    text_lower = text.lower()
    return any(keyword in text_lower for keyword in DATABRICKS_KEYWORDS)


def extract_keywords(text: str) -> List[str]:
    """
    Extract Databricks-related keywords from text.

    Args:
        text: Text to search

    Returns:
        List of found keywords
    """
    if not text:
        return []
    text_lower = text.lower()
    return [kw for kw in DATABRICKS_KEYWORDS if kw in text_lower]


# Common schema for news items
NEWS_SCHEMA = """
    id STRING,
    source STRING,
    title STRING,
    url STRING,
    content STRING,
    author STRING,
    published_at TIMESTAMP,
    fetched_at TIMESTAMP,
    score INT,
    comments_count INT,
    keywords ARRAY<STRING>,
    metadata STRING
"""
