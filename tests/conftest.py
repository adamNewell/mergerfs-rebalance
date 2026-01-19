"""Pytest fixtures for mergerfs-balance tests."""

import os
import tempfile
from dataclasses import dataclass

import pytest


@dataclass
class MockDriveStats:
    """Mock drive statistics for testing."""

    path: str
    total_bytes: int
    used_bytes: int
    free_bytes: int

    @property
    def usage_percent(self) -> float:
        if self.total_bytes == 0:
            return 0.0
        return (self.used_bytes / self.total_bytes) * 100

    @property
    def free_percent(self) -> float:
        return 100.0 - self.usage_percent


@dataclass
class MockDrive:
    """Mock drive for testing."""

    path: str
    stats: MockDriveStats
    write_locked: bool = False

    def refresh_stats(self) -> None:
        pass


@pytest.fixture
def mock_drives():
    """Create a set of mock drives with varying usage levels."""
    return [
        MockDrive(
            path="/mnt/disk1",
            stats=MockDriveStats(
                path="/mnt/disk1",
                total_bytes=1000 * 1024 ** 3,  # 1TB
                used_bytes=800 * 1024 ** 3,  # 80% full
                free_bytes=200 * 1024 ** 3,
            ),
        ),
        MockDrive(
            path="/mnt/disk2",
            stats=MockDriveStats(
                path="/mnt/disk2",
                total_bytes=1000 * 1024 ** 3,  # 1TB
                used_bytes=300 * 1024 ** 3,  # 30% full
                free_bytes=700 * 1024 ** 3,
            ),
        ),
        MockDrive(
            path="/mnt/disk3",
            stats=MockDriveStats(
                path="/mnt/disk3",
                total_bytes=2000 * 1024 ** 3,  # 2TB
                used_bytes=1000 * 1024 ** 3,  # 50% full
                free_bytes=1000 * 1024 ** 3,
            ),
        ),
    ]


@pytest.fixture
def temp_dir():
    """Create a temporary directory for tests."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield tmpdir


@pytest.fixture
def temp_files(temp_dir):
    """Create temporary test files in a directory structure."""
    # Create subdirectories
    subdir1 = os.path.join(temp_dir, "subdir1")
    subdir2 = os.path.join(temp_dir, "subdir2")
    os.makedirs(subdir1)
    os.makedirs(subdir2)

    # Create test files
    files = {
        "small.txt": 100,  # 100 bytes
        "medium.txt": 1024 * 100,  # 100KB
        "large.txt": 1024 * 1024,  # 1MB
        "subdir1/nested.txt": 500,
        "subdir2/another.txt": 750,
    }

    created_files = {}
    for rel_path, size in files.items():
        full_path = os.path.join(temp_dir, rel_path)
        with open(full_path, "wb") as f:
            f.write(b"x" * size)
        created_files[rel_path] = full_path

    return temp_dir, created_files
