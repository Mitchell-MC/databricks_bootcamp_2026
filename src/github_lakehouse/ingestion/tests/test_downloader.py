"""Unit tests for GH Archive downloader."""

from datetime import date, datetime
from unittest.mock import MagicMock, patch

import pytest

from github_lakehouse.config import GHArchiveConfig
from github_lakehouse.ingestion.downloader import ArchiveFile, GHArchiveDownloader


class TestArchiveFile:
    """Tests for ArchiveFile dataclass."""

    def test_datetime_hour_property(self) -> None:
        """Test datetime_hour returns correct datetime."""
        archive = ArchiveFile(
            archive_date=date(2024, 1, 15),
            hour=10,
            filename="2024-01-15-10.json.gz",
            url="https://data.gharchive.org/2024-01-15-10.json.gz",
        )

        result = archive.datetime_hour

        assert result == datetime(2024, 1, 15, 10, 0, 0)

    def test_default_values(self) -> None:
        """Test default values are set correctly."""
        archive = ArchiveFile(
            archive_date=date(2024, 1, 15),
            hour=0,
            filename="test.json.gz",
            url="https://example.com/test.json.gz",
        )

        assert archive.local_path is None
        assert archive.content is None
        assert archive.size_bytes == 0


class TestGHArchiveConfig:
    """Tests for GHArchiveConfig."""

    def test_get_archive_url_formats_correctly(self) -> None:
        """Test URL generation for archive files."""
        config = GHArchiveConfig(start_date=date(2024, 1, 1))

        url = config.get_archive_url(date(2024, 1, 15), 10)

        assert url == "https://data.gharchive.org/2024-01-15-10.json.gz"

    def test_get_archive_url_hour_zero(self) -> None:
        """Test URL generation for hour 0."""
        config = GHArchiveConfig(start_date=date(2024, 1, 1))

        url = config.get_archive_url(date(2024, 1, 15), 0)

        assert url == "https://data.gharchive.org/2024-01-15-0.json.gz"

    def test_get_archive_url_hour_23(self) -> None:
        """Test URL generation for hour 23."""
        config = GHArchiveConfig(start_date=date(2024, 1, 1))

        url = config.get_archive_url(date(2024, 1, 15), 23)

        assert url == "https://data.gharchive.org/2024-01-15-23.json.gz"

    def test_get_archive_url_invalid_hour_raises(self) -> None:
        """Test that invalid hour raises ValueError."""
        config = GHArchiveConfig(start_date=date(2024, 1, 1))

        with pytest.raises(ValueError, match="Hour must be 0-23"):
            config.get_archive_url(date(2024, 1, 15), 24)

        with pytest.raises(ValueError, match="Hour must be 0-23"):
            config.get_archive_url(date(2024, 1, 15), -1)


class TestGHArchiveDownloader:
    """Tests for GHArchiveDownloader."""

    @pytest.fixture
    def config(self) -> GHArchiveConfig:
        """Create test configuration."""
        return GHArchiveConfig(
            start_date=date(2024, 1, 1),
            end_date=date(2024, 1, 1),
            max_retries=1,
            timeout_seconds=10,
        )

    @pytest.fixture
    def downloader(self, config: GHArchiveConfig) -> GHArchiveDownloader:
        """Create test downloader."""
        return GHArchiveDownloader(config)

    def test_generate_archive_urls_single_day(
        self,
        downloader: GHArchiveDownloader,
    ) -> None:
        """Test URL generation for a single day."""
        archives = list(
            downloader.generate_archive_urls(
                start_date=date(2024, 1, 15),
                end_date=date(2024, 1, 15),
            )
        )

        assert len(archives) == 24  # 24 hours
        assert archives[0].hour == 0
        assert archives[23].hour == 23
        assert all(a.archive_date == date(2024, 1, 15) for a in archives)

    def test_generate_archive_urls_multiple_days(
        self,
        downloader: GHArchiveDownloader,
    ) -> None:
        """Test URL generation for multiple days."""
        archives = list(
            downloader.generate_archive_urls(
                start_date=date(2024, 1, 15),
                end_date=date(2024, 1, 16),
            )
        )

        assert len(archives) == 48  # 2 days * 24 hours

    def test_generate_archive_urls_filenames(
        self,
        downloader: GHArchiveDownloader,
    ) -> None:
        """Test generated filenames are correct."""
        archives = list(
            downloader.generate_archive_urls(
                start_date=date(2024, 1, 15),
                end_date=date(2024, 1, 15),
            )
        )

        assert archives[0].filename == "2024-01-15-0.json.gz"
        assert archives[10].filename == "2024-01-15-10.json.gz"
        assert archives[23].filename == "2024-01-15-23.json.gz"

    @patch("requests.Session.head")
    def test_check_availability_returns_true_on_200(
        self,
        mock_head: MagicMock,
        downloader: GHArchiveDownloader,
    ) -> None:
        """Test availability check returns True for 200 response."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_head.return_value = mock_response

        result = downloader.check_availability(date(2024, 1, 15), 10)

        assert result is True
        mock_head.assert_called_once()

    @patch("requests.Session.head")
    def test_check_availability_returns_false_on_404(
        self,
        mock_head: MagicMock,
        downloader: GHArchiveDownloader,
    ) -> None:
        """Test availability check returns False for 404 response."""
        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_head.return_value = mock_response

        result = downloader.check_availability(date(2024, 1, 15), 10)

        assert result is False

    @patch("requests.Session.head")
    def test_check_availability_returns_false_on_error(
        self,
        mock_head: MagicMock,
        downloader: GHArchiveDownloader,
    ) -> None:
        """Test availability check returns False on request error."""
        import requests

        mock_head.side_effect = requests.RequestException("Connection error")

        result = downloader.check_availability(date(2024, 1, 15), 10)

        assert result is False

    @patch("requests.Session.get")
    def test_download_file_decompresses_content(
        self,
        mock_get: MagicMock,
        downloader: GHArchiveDownloader,
    ) -> None:
        """Test that download_file decompresses gzip content."""
        import gzip

        original_content = '{"id": "123", "type": "PushEvent"}'
        compressed = gzip.compress(original_content.encode("utf-8"))

        mock_response = MagicMock()
        mock_response.content = compressed
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response

        archive = ArchiveFile(
            archive_date=date(2024, 1, 15),
            hour=10,
            filename="2024-01-15-10.json.gz",
            url="https://data.gharchive.org/2024-01-15-10.json.gz",
        )

        result = downloader.download_file(archive)

        assert result.content == original_content
        assert result.size_bytes == len(compressed)

    def test_load_from_path_raises_without_path(
        self,
        downloader: GHArchiveDownloader,
    ) -> None:
        """Test load_from_path raises if local_path not set."""
        archive = ArchiveFile(
            archive_date=date(2024, 1, 15),
            hour=10,
            filename="test.json.gz",
            url="https://example.com/test.json.gz",
        )

        with pytest.raises(ValueError, match="local_path is not set"):
            downloader.load_from_path(archive)
