"""GH Archive file downloader.

This module provides utilities for downloading GitHub Archive
hourly data files from https://www.gharchive.org/
"""

import gzip
import logging
import time
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from github_lakehouse.config import GHArchiveConfig

logger = logging.getLogger(__name__)


@dataclass
class ArchiveFile:
    """Represents a downloaded GH Archive file.

    Attributes:
        archive_date: Date of the archive.
        hour: Hour of the archive (0-23).
        filename: Name of the archive file.
        url: URL the file was downloaded from.
        local_path: Local file path (if downloaded).
        content: Decompressed content (if loaded).
        size_bytes: Size of compressed file in bytes.
    """

    archive_date: date
    hour: int
    filename: str
    url: str
    local_path: Path | None = None
    content: str | None = None
    size_bytes: int = 0

    @property
    def datetime_hour(self) -> datetime:
        """Get datetime for this archive hour.

        Returns:
            datetime: Archive datetime (UTC).
        """
        return datetime(
            self.archive_date.year,
            self.archive_date.month,
            self.archive_date.day,
            self.hour,
        )


class GHArchiveDownloader:
    """Downloads and decompresses GH Archive hourly data files.

    This class handles fetching gzip-compressed JSON files from
    GH Archive and provides them as decompressed content.

    Attributes:
        config: GH Archive configuration.
        session: Requests session with retry logic.
    """

    def __init__(self, config: GHArchiveConfig) -> None:
        """Initialize the downloader.

        Args:
            config: GH Archive configuration.
        """
        self.config = config
        self.session = self._create_session()

    def _create_session(self) -> requests.Session:
        """Create a requests session with retry logic.

        Returns:
            requests.Session: Configured session.
        """
        session = requests.Session()
        retry_strategy = Retry(
            total=self.config.max_retries,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        return session

    def generate_archive_urls(
        self,
        start_date: date,
        end_date: date | None = None,
    ) -> Iterator[ArchiveFile]:
        """Generate ArchiveFile objects for a date range.

        Args:
            start_date: Start date (inclusive).
            end_date: End date (inclusive), defaults to today.

        Yields:
            ArchiveFile: Archive file metadata for each hour.
        """
        if end_date is None:
            end_date = date.today()

        current_date = start_date
        while current_date <= end_date:
            for hour in range(24):
                filename = f"{current_date.isoformat()}-{hour}.json.gz"
                url = self.config.get_archive_url(current_date, hour)
                yield ArchiveFile(
                    archive_date=current_date,
                    hour=hour,
                    filename=filename,
                    url=url,
                )
            current_date += timedelta(days=1)

    def download_file(self, archive: ArchiveFile) -> ArchiveFile:
        """Download and decompress a single archive file.

        Args:
            archive: Archive file metadata.

        Returns:
            ArchiveFile: Updated with content and size.

        Raises:
            requests.RequestException: If download fails.
            gzip.BadGzipFile: If decompression fails.
        """
        logger.info(f"Downloading {archive.url}")
        start_time = time.time()

        response = self.session.get(
            archive.url,
            timeout=self.config.timeout_seconds,
            stream=True,
        )
        response.raise_for_status()

        compressed_data = response.content
        archive.size_bytes = len(compressed_data)

        decompressed = gzip.decompress(compressed_data)
        archive.content = decompressed.decode("utf-8")

        duration = time.time() - start_time
        logger.info(
            f"Downloaded {archive.filename}: "
            f"{archive.size_bytes:,} bytes compressed, "
            f"{len(archive.content):,} chars decompressed, "
            f"{duration:.2f}s"
        )

        return archive

    def download_to_path(
        self,
        archive: ArchiveFile,
        output_dir: Path | None = None,
    ) -> ArchiveFile:
        """Download archive file to local filesystem.

        Args:
            archive: Archive file metadata.
            output_dir: Output directory (uses config default if None).

        Returns:
            ArchiveFile: Updated with local_path.

        Raises:
            requests.RequestException: If download fails.
        """
        if output_dir is None:
            output_dir = Path(self.config.download_path)

        output_dir.mkdir(parents=True, exist_ok=True)
        local_path = output_dir / archive.filename

        logger.info(f"Downloading {archive.url} to {local_path}")

        response = self.session.get(
            archive.url,
            timeout=self.config.timeout_seconds,
            stream=True,
        )
        response.raise_for_status()

        with open(local_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)

        archive.local_path = local_path
        archive.size_bytes = local_path.stat().st_size
        logger.info(f"Saved {archive.filename}: {archive.size_bytes:,} bytes")

        return archive

    def load_from_path(self, archive: ArchiveFile) -> ArchiveFile:
        """Load and decompress archive file from local path.

        Args:
            archive: Archive file with local_path set.

        Returns:
            ArchiveFile: Updated with content.

        Raises:
            FileNotFoundError: If local_path doesn't exist.
            gzip.BadGzipFile: If decompression fails.
        """
        if archive.local_path is None:
            raise ValueError("local_path is not set")

        with gzip.open(archive.local_path, "rt", encoding="utf-8") as f:
            archive.content = f.read()

        return archive

    def download_date_range(
        self,
        start_date: date,
        end_date: date | None = None,
        to_memory: bool = True,
    ) -> Iterator[ArchiveFile]:
        """Download all archive files for a date range.

        Args:
            start_date: Start date (inclusive).
            end_date: End date (inclusive), defaults to today.
            to_memory: If True, load content to memory. If False, save to disk.

        Yields:
            ArchiveFile: Downloaded archive files.
        """
        for archive in self.generate_archive_urls(start_date, end_date):
            try:
                if to_memory:
                    yield self.download_file(archive)
                else:
                    downloaded = self.download_to_path(archive)
                    yield self.load_from_path(downloaded)
            except requests.RequestException as e:
                logger.error(f"Failed to download {archive.url}: {e}")
                continue
            except gzip.BadGzipFile as e:
                logger.error(f"Failed to decompress {archive.filename}: {e}")
                continue

    def check_availability(self, archive_date: date, hour: int) -> bool:
        """Check if an archive file is available.

        Args:
            archive_date: Date to check.
            hour: Hour to check (0-23).

        Returns:
            bool: True if file exists and is accessible.
        """
        url = self.config.get_archive_url(archive_date, hour)
        try:
            response = self.session.head(url, timeout=10)
            return response.status_code == 200
        except requests.RequestException:
            return False
