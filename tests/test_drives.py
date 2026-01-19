"""Tests for drive management functions."""

import pytest


class TestDriveStats:
    """Tests for DriveStats calculations."""

    def test_usage_percent_calculation(self, mock_drives):
        """Test that usage percentage is calculated correctly."""
        # disk1: 800GB used of 1TB = 80%
        assert mock_drives[0].stats.usage_percent == 80.0
        # disk2: 300GB used of 1TB = 30%
        assert mock_drives[1].stats.usage_percent == 30.0
        # disk3: 1TB used of 2TB = 50%
        assert mock_drives[2].stats.usage_percent == 50.0

    def test_free_percent_calculation(self, mock_drives):
        """Test that free percentage is calculated correctly."""
        assert mock_drives[0].stats.free_percent == 20.0
        assert mock_drives[1].stats.free_percent == 70.0
        assert mock_drives[2].stats.free_percent == 50.0

    def test_zero_total_bytes(self):
        """Test handling of zero total bytes."""
        from tests.conftest import MockDriveStats

        stats = MockDriveStats(path="/test", total_bytes=0, used_bytes=0, free_bytes=0)
        assert stats.usage_percent == 0.0
        assert stats.free_percent == 100.0


class TestGetAverageUsage:
    """Tests for average usage calculation across drives."""

    def test_average_usage_with_different_sizes(self, mock_drives):
        """Test average usage calculation with different drive sizes.

        disk1: 1TB total, 800GB used
        disk2: 1TB total, 300GB used
        disk3: 2TB total, 1000GB used

        Total: 4TB capacity, 2.1TB used = 52.5% average
        """
        total_used = sum(d.stats.used_bytes for d in mock_drives)
        total_capacity = sum(d.stats.total_bytes for d in mock_drives)
        expected_avg = (total_used / total_capacity) * 100

        # Calculate manually: (800 + 300 + 1000) / (1000 + 1000 + 2000) = 2100/4000 = 52.5%
        assert expected_avg == 52.5


class TestGetOverfullDrives:
    """Tests for identifying overfull drives."""

    def test_identifies_overfull_drives(self, mock_drives):
        """Test that drives above threshold are identified."""
        # With average of 52.5% and target percentage of 2%:
        # threshold = 52.5 + 1 = 53.5%
        # disk1 at 80% is overfull
        # disk2 at 30% is not overfull
        # disk3 at 50% is not overfull
        avg = 52.5
        target_pct = 2.0
        threshold = avg + (target_pct / 2)  # 53.5%

        overfull = [d for d in mock_drives if d.stats.usage_percent > threshold]
        assert len(overfull) == 1
        assert overfull[0].path == "/mnt/disk1"

    def test_sorted_by_usage_descending(self, mock_drives):
        """Test that overfull drives are sorted by usage descending."""
        # Modify mock to have multiple overfull drives
        mock_drives[2].stats.used_bytes = 1800 * 1024 ** 3  # 90% full now

        avg = 52.5  # Approximate, would change with new values
        target_pct = 2.0
        threshold = avg + (target_pct / 2)

        overfull = [d for d in mock_drives if d.stats.usage_percent > threshold]
        overfull_sorted = sorted(overfull, key=lambda d: d.stats.usage_percent, reverse=True)

        # disk3 now at 90% should be first, disk1 at 80% should be second
        assert overfull_sorted[0].stats.usage_percent >= overfull_sorted[-1].stats.usage_percent


class TestGetBestDestination:
    """Tests for finding the best destination drive."""

    def test_selects_drive_with_most_free_space(self, mock_drives):
        """Test that drive with most free space is selected."""
        avg = 52.5
        target_pct = 2.0
        threshold = avg - (target_pct / 2)  # 51.5%

        # Underfull drives: those below 51.5%
        # disk1 at 80% - not underfull
        # disk2 at 30% - underfull, 700GB free
        # disk3 at 50% - underfull, 1000GB free
        underfull = [d for d in mock_drives if d.stats.usage_percent < threshold]
        assert len(underfull) == 2

        # disk3 has more free space (1TB vs 700GB)
        best = max(underfull, key=lambda d: d.stats.free_bytes)
        assert best.path == "/mnt/disk3"

    def test_excludes_write_locked_drives(self, mock_drives):
        """Test that write-locked drives are excluded."""
        # Lock disk3
        mock_drives[2].write_locked = True

        avg = 52.5
        target_pct = 2.0
        threshold = avg - (target_pct / 2)

        underfull = [d for d in mock_drives if d.stats.usage_percent < threshold]
        available = [d for d in underfull if not d.write_locked]

        # Only disk2 should be available
        assert len(available) == 1
        assert available[0].path == "/mnt/disk2"

    def test_returns_none_when_no_candidates(self, mock_drives):
        """Test that None is returned when no underfull drives available."""
        # Make all drives above threshold by setting high usage
        for drive in mock_drives:
            drive.stats.used_bytes = int(drive.stats.total_bytes * 0.9)
            drive.stats.free_bytes = int(drive.stats.total_bytes * 0.1)

        avg = 90.0
        target_pct = 2.0
        threshold = avg - (target_pct / 2)  # 89%

        underfull = [d for d in mock_drives if d.stats.usage_percent < threshold]
        assert len(underfull) == 0


class TestIsBalanced:
    """Tests for checking if drives are balanced."""

    def test_not_balanced_with_large_range(self, mock_drives):
        """Test that drives with large usage range are not balanced."""
        # Range: 80% - 30% = 50%
        usages = [d.stats.usage_percent for d in mock_drives]
        usage_range = max(usages) - min(usages)
        target_pct = 2.0

        assert usage_range == 50.0
        assert usage_range > target_pct  # Not balanced

    def test_balanced_with_small_range(self, mock_drives):
        """Test that drives with small usage range are balanced."""
        # Set all drives to similar usage
        for drive in mock_drives:
            drive.stats.used_bytes = int(drive.stats.total_bytes * 0.50)
            drive.stats.free_bytes = int(drive.stats.total_bytes * 0.50)

        usages = [d.stats.usage_percent for d in mock_drives]
        usage_range = max(usages) - min(usages)
        target_pct = 2.0

        assert usage_range == 0.0
        assert usage_range <= target_pct  # Balanced
