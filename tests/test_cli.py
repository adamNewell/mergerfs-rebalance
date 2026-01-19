"""Tests for CLI argument parsing."""

import pytest

from mergerfs_balance.cli import parse_size


class TestParseSize:
    """Tests for the parse_size function."""

    def test_bytes_no_unit(self):
        """Test parsing raw bytes with no unit."""
        assert parse_size("1024") == 1024
        assert parse_size("0") == 0
        assert parse_size("999999") == 999999

    def test_bytes_with_b_unit(self):
        """Test parsing bytes with B unit."""
        assert parse_size("100B") == 100
        assert parse_size("100b") == 100

    def test_kilobytes(self):
        """Test parsing kilobytes."""
        assert parse_size("1K") == 1024
        assert parse_size("1k") == 1024
        assert parse_size("10K") == 10 * 1024
        assert parse_size("1KB") == 1024
        assert parse_size("1kb") == 1024
        assert parse_size("1KiB") == 1024
        assert parse_size("1kib") == 1024

    def test_megabytes(self):
        """Test parsing megabytes."""
        assert parse_size("1M") == 1024 ** 2
        assert parse_size("1m") == 1024 ** 2
        assert parse_size("100M") == 100 * 1024 ** 2
        assert parse_size("1MB") == 1024 ** 2
        assert parse_size("1mb") == 1024 ** 2
        assert parse_size("1MiB") == 1024 ** 2
        assert parse_size("1mib") == 1024 ** 2

    def test_gigabytes(self):
        """Test parsing gigabytes."""
        assert parse_size("1G") == 1024 ** 3
        assert parse_size("1g") == 1024 ** 3
        assert parse_size("5G") == 5 * 1024 ** 3
        assert parse_size("1GB") == 1024 ** 3
        assert parse_size("1gb") == 1024 ** 3
        assert parse_size("1GiB") == 1024 ** 3
        assert parse_size("1gib") == 1024 ** 3

    def test_terabytes(self):
        """Test parsing terabytes."""
        assert parse_size("1T") == 1024 ** 4
        assert parse_size("1t") == 1024 ** 4
        assert parse_size("2T") == 2 * 1024 ** 4
        assert parse_size("1TB") == 1024 ** 4
        assert parse_size("1tb") == 1024 ** 4
        assert parse_size("1TiB") == 1024 ** 4
        assert parse_size("1tib") == 1024 ** 4

    def test_petabytes(self):
        """Test parsing petabytes."""
        assert parse_size("1P") == 1024 ** 5
        assert parse_size("1PB") == 1024 ** 5
        assert parse_size("1PIB") == 1024 ** 5

    def test_decimal_values(self):
        """Test parsing decimal values."""
        assert parse_size("1.5G") == int(1.5 * 1024 ** 3)
        assert parse_size("0.5M") == int(0.5 * 1024 ** 2)
        assert parse_size("2.5K") == int(2.5 * 1024)
        assert parse_size("1.5GB") == int(1.5 * 1024 ** 3)

    def test_with_whitespace(self):
        """Test parsing values with whitespace."""
        assert parse_size("  100M  ") == 100 * 1024 ** 2
        assert parse_size("  1G") == 1024 ** 3
        assert parse_size("1G  ") == 1024 ** 3
        assert parse_size(" 100 MB ") == 100 * 1024 ** 2

    def test_empty_string_raises(self):
        """Test that empty string raises ValueError."""
        with pytest.raises(ValueError, match="Empty size string"):
            parse_size("")
        with pytest.raises(ValueError, match="Empty size string"):
            parse_size("   ")

    def test_invalid_format_raises(self):
        """Test that invalid format raises ValueError."""
        with pytest.raises(ValueError, match="Invalid size format"):
            parse_size("abc")
        with pytest.raises(ValueError, match="Invalid size format"):
            parse_size("M100")
        with pytest.raises(ValueError, match="Invalid size format"):
            parse_size("--100M")

    def test_negative_values_raises(self):
        """Test that negative values raise ValueError."""
        with pytest.raises(ValueError, match="Invalid size format"):
            parse_size("-100M")

    def test_unknown_unit_raises(self):
        """Test that unknown units raise ValueError."""
        with pytest.raises(ValueError, match="Unknown size unit"):
            parse_size("100X")
        with pytest.raises(ValueError, match="Unknown size unit"):
            parse_size("100ZB")
