"""Data ingestion module for GitHub Archive Lakehouse.

This module provides utilities for downloading and loading
GH Archive data files into the lakehouse.
"""

from github_lakehouse.ingestion.downloader import (
    ArchiveFile,
    GHArchiveDownloader,
)

__all__ = [
    "ArchiveFile",
    "GHArchiveDownloader",
]
