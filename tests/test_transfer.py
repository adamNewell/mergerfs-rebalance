"""Tests for transfer module functions."""

import pytest

from mergerfs_balance.transfer import format_bytes, parse_rsync_progress


class TestFormatBytes:
    """Tests for the format_bytes function."""

    def test_bytes(self):
        """Test formatting bytes."""
        assert format_bytes(0) == "0.0B"
        assert format_bytes(100) == "100.0B"
        assert format_bytes(1023) == "1023.0B"

    def test_kilobytes(self):
        """Test formatting kilobytes."""
        assert format_bytes(1024) == "1.0KB"
        assert format_bytes(1536) == "1.5KB"
        assert format_bytes(10 * 1024) == "10.0KB"

    def test_megabytes(self):
        """Test formatting megabytes."""
        assert format_bytes(1024 ** 2) == "1.0MB"
        assert format_bytes(1.5 * 1024 ** 2) == "1.5MB"
        assert format_bytes(100 * 1024 ** 2) == "100.0MB"

    def test_gigabytes(self):
        """Test formatting gigabytes."""
        assert format_bytes(1024 ** 3) == "1.0GB"
        assert format_bytes(2.5 * 1024 ** 3) == "2.5GB"

    def test_terabytes(self):
        """Test formatting terabytes."""
        assert format_bytes(1024 ** 4) == "1.0TB"
        assert format_bytes(10 * 1024 ** 4) == "10.0TB"

    def test_petabytes(self):
        """Test formatting petabytes."""
        assert format_bytes(1024 ** 5) == "1.0PB"

    def test_negative_values(self):
        """Test formatting negative values (absolute value is used)."""
        assert format_bytes(-100) == "-100.0B"
        assert format_bytes(-1024) == "-1.0KB"


class TestParseRsyncProgress:
    """Tests for the parse_rsync_progress function."""

    def test_basic_progress_line(self):
        """Test parsing a basic rsync progress line."""
        line = "  1,234,567  50%   12.34MB/s    0:01:23"
        progress = parse_rsync_progress(line)

        assert progress is not None
        assert progress.bytes_transferred == 1234567
        assert progress.percent == 50
        assert progress.speed_bytes_per_sec == pytest.approx(12.34 * 1024 ** 2)
        assert progress.eta_seconds == 83  # 1*60 + 23

    def test_progress_with_kb_speed(self):
        """Test parsing progress with KB/s speed."""
        line = "  500,000  25%   512.0KB/s    0:05:00"
        progress = parse_rsync_progress(line)

        assert progress is not None
        assert progress.bytes_transferred == 500000
        assert progress.percent == 25
        assert progress.speed_bytes_per_sec == pytest.approx(512.0 * 1024)
        assert progress.eta_seconds == 300  # 5*60

    def test_progress_with_gb_speed(self):
        """Test parsing progress with GB/s speed."""
        line = "  10,000,000,000  75%   1.5GB/s    0:00:30"
        progress = parse_rsync_progress(line)

        assert progress is not None
        assert progress.bytes_transferred == 10000000000
        assert progress.percent == 75
        assert progress.speed_bytes_per_sec == pytest.approx(1.5 * 1024 ** 3)
        assert progress.eta_seconds == 30

    def test_progress_with_hours(self):
        """Test parsing progress with hours in ETA."""
        line = "  1,000,000  10%   100.0KB/s    1:30:45"
        progress = parse_rsync_progress(line)

        assert progress is not None
        assert progress.eta_seconds == 1 * 3600 + 30 * 60 + 45

    def test_progress_bytes_speed(self):
        """Test parsing progress with B/s speed."""
        line = "  1,000  1%   500.0B/s    0:00:10"
        progress = parse_rsync_progress(line)

        assert progress is not None
        assert progress.speed_bytes_per_sec == pytest.approx(500.0)

    def test_non_progress_line_returns_none(self):
        """Test that non-progress lines return None."""
        assert parse_rsync_progress("sending incremental file list") is None
        assert parse_rsync_progress("") is None
        assert parse_rsync_progress("some random text") is None
        assert parse_rsync_progress("total size is 12345") is None

    def test_100_percent_complete(self):
        """Test parsing 100% complete progress."""
        line = "  5,000,000  100%   50.00MB/s    0:00:00"
        progress = parse_rsync_progress(line)

        assert progress is not None
        assert progress.percent == 100
        assert progress.eta_seconds == 0

    def test_progress_no_eta(self):
        """Test parsing progress with no ETA field.

        Note: rsync --info=progress2 always includes ETA, so lines without
        ETA don't match the expected progress format and return None.
        """
        line = "  1,234,567  50%   12.34MB/s"
        progress = parse_rsync_progress(line)

        # Lines without ETA don't match rsync's progress2 format
        assert progress is None
