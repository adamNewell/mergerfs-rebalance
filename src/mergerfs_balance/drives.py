"""Drive discovery and management for mergerfs filesystems."""

import os
import re
import shutil
import subprocess
import threading
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional


@dataclass
class DriveStats:
    """Statistics for a single drive."""

    path: str
    total_bytes: int
    used_bytes: int
    free_bytes: int

    @property
    def usage_percent(self) -> float:
        """Return usage as a percentage."""
        if self.total_bytes == 0:
            return 0.0
        return (self.used_bytes / self.total_bytes) * 100

    @property
    def free_percent(self) -> float:
        """Return free space as a percentage."""
        return 100.0 - self.usage_percent


@dataclass
class Drive:
    """Represents a single drive in the mergerfs pool."""

    path: str
    stats: DriveStats
    write_locked: bool = False
    lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    def refresh_stats(self) -> None:
        """Refresh drive statistics."""
        self.stats = get_drive_stats(self.path)


def get_drive_stats(path: str) -> DriveStats:
    """Get disk usage statistics for a path."""
    usage = shutil.disk_usage(path)
    return DriveStats(
        path=path,
        total_bytes=usage.total,
        used_bytes=usage.used,
        free_bytes=usage.free,
    )


def discover_mergerfs_drives(mount_point: str) -> list[str]:
    """Discover underlying drives from a mergerfs mount point.

    Reads the mergerfs configuration from /proc/mounts or xattr to find
    the source drives that make up the pool.
    """
    drives = []

    # Method 1: Try reading from mergerfs xattr
    try:
        xattr_path = os.path.join(mount_point, '.mergerfs')
        if os.path.exists(xattr_path):
            # Read srcmounts from mergerfs control file
            srcmounts_path = os.path.join(xattr_path, 'srcmounts')
            if os.path.exists(srcmounts_path):
                with open(srcmounts_path, 'r') as f:
                    content = f.read().strip()
                    if content:
                        drives = [d.strip() for d in content.split(':') if d.strip()]
                        if drives:
                            return drives
    except (OSError, PermissionError):
        pass

    # Method 2: Try getfattr command
    try:
        result = subprocess.run(
            ['getfattr', '-n', 'user.mergerfs.srcmounts', '--only-values', mount_point],
            capture_output=True,
            text=True,
            timeout=5
        )
        if result.returncode == 0 and result.stdout.strip():
            drives = [d.strip() for d in result.stdout.strip().split(':') if d.strip()]
            if drives:
                return drives
    except (subprocess.SubprocessError, FileNotFoundError, subprocess.TimeoutExpired):
        pass

    # Method 3: Parse /proc/mounts
    try:
        with open('/proc/mounts', 'r') as f:
            for line in f:
                parts = line.split()
                if len(parts) >= 3 and parts[1] == mount_point and parts[2] == 'fuse.mergerfs':
                    # First field contains the source paths
                    source = parts[0]
                    # mergerfs sources are colon-separated or use glob patterns
                    if ':' in source:
                        drives = [d.strip() for d in source.split(':') if d.strip()]
                    else:
                        drives = [source]
                    if drives:
                        return drives
    except (OSError, PermissionError):
        pass

    # Method 4: Try /etc/fstab as fallback
    try:
        with open('/etc/fstab', 'r') as f:
            for line in f:
                line = line.strip()
                if line.startswith('#') or not line:
                    continue
                parts = line.split()
                if len(parts) >= 3 and parts[1] == mount_point and 'mergerfs' in parts[2]:
                    source = parts[0]
                    if ':' in source:
                        drives = [d.strip() for d in source.split(':') if d.strip()]
                    else:
                        drives = [source]
                    if drives:
                        return drives
    except (OSError, PermissionError):
        pass

    return drives


def expand_glob_paths(paths: list[str]) -> list[str]:
    """Expand glob patterns in paths (e.g., /mnt/disk* -> /mnt/disk1, /mnt/disk2)."""
    expanded = []
    for path in paths:
        if '*' in path or '?' in path:
            parent = Path(path).parent
            pattern = Path(path).name
            if parent.exists():
                for match in sorted(parent.glob(pattern)):
                    if match.is_dir():
                        expanded.append(str(match))
        else:
            expanded.append(path)
    return expanded


class DriveManager:
    """Manages drives in a mergerfs pool, tracking usage and write locks."""

    def __init__(
        self,
        mount_point: str,
        source_drives: Optional[list[str]] = None,
        dest_drives: Optional[list[str]] = None,
    ):
        self.mount_point = mount_point
        self._lock = threading.Lock()

        # Discover all drives
        discovered = discover_mergerfs_drives(mount_point)
        discovered = expand_glob_paths(discovered)

        if not discovered:
            raise ValueError(f"Could not discover drives for mount point: {mount_point}")

        # Filter to configured drives if specified
        if source_drives:
            source_drives = expand_glob_paths(source_drives)
            self._source_paths = [d for d in discovered if d in source_drives]
        else:
            self._source_paths = discovered[:]

        if dest_drives:
            dest_drives = expand_glob_paths(dest_drives)
            self._dest_paths = [d for d in discovered if d in dest_drives]
        else:
            self._dest_paths = discovered[:]

        # Create Drive objects for all unique drives
        all_paths = set(self._source_paths) | set(self._dest_paths)
        self._drives: dict[str, Drive] = {}
        for path in all_paths:
            stats = get_drive_stats(path)
            self._drives[path] = Drive(path=path, stats=stats)

    @property
    def all_drives(self) -> list[Drive]:
        """Return all drives."""
        return list(self._drives.values())

    @property
    def source_drives(self) -> list[Drive]:
        """Return drives that can be used as sources."""
        return [self._drives[p] for p in self._source_paths]

    @property
    def dest_drives(self) -> list[Drive]:
        """Return drives that can be used as destinations."""
        return [self._drives[p] for p in self._dest_paths]

    def refresh_all_stats(self) -> None:
        """Refresh statistics for all drives."""
        for drive in self._drives.values():
            drive.refresh_stats()

    def refresh_drive_stats(self, path: str) -> None:
        """Refresh statistics for a specific drive."""
        if path in self._drives:
            self._drives[path].refresh_stats()

    def get_average_usage(self) -> float:
        """Calculate average usage percentage across all drives."""
        if not self._drives:
            return 0.0
        total = sum(d.stats.usage_percent for d in self._drives.values())
        return total / len(self._drives)

    def get_usage_range(self) -> float:
        """Get the range between highest and lowest usage percentages."""
        if not self._drives:
            return 0.0
        usages = [d.stats.usage_percent for d in self._drives.values()]
        return max(usages) - min(usages)

    def is_balanced(self, target_percentage: float) -> bool:
        """Check if all drives are within the target percentage of each other."""
        return self.get_usage_range() <= target_percentage

    def get_overfull_drives(self, target_percentage: float) -> list[Drive]:
        """Get drives that are more than target_percentage above average, sorted by usage descending."""
        avg = self.get_average_usage()
        threshold = avg + (target_percentage / 2)
        overfull = [d for d in self.source_drives if d.stats.usage_percent > threshold]
        return sorted(overfull, key=lambda d: d.stats.usage_percent, reverse=True)

    def get_underfull_drives(self, target_percentage: float) -> list[Drive]:
        """Get drives that are more than target_percentage below average, sorted by free space descending."""
        avg = self.get_average_usage()
        threshold = avg - (target_percentage / 2)
        underfull = [d for d in self.dest_drives if d.stats.usage_percent < threshold]
        return sorted(underfull, key=lambda d: d.stats.free_bytes, reverse=True)

    def get_best_destination(self, exclude_busy: bool = True) -> Optional[Drive]:
        """Get the destination drive with most free space that isn't write-locked."""
        candidates = self.dest_drives
        if exclude_busy:
            candidates = [d for d in candidates if not d.write_locked]
        if not candidates:
            return None
        return max(candidates, key=lambda d: d.stats.free_bytes)

    def acquire_write_lock(self, path: str) -> bool:
        """Try to acquire a write lock on a drive. Returns True if successful."""
        if path not in self._drives:
            return False

        drive = self._drives[path]
        with drive.lock:
            if drive.write_locked:
                return False
            drive.write_locked = True
            return True

    def release_write_lock(self, path: str) -> None:
        """Release a write lock on a drive."""
        if path in self._drives:
            drive = self._drives[path]
            with drive.lock:
                drive.write_locked = False

    def get_relative_path(self, absolute_path: str) -> Optional[tuple[str, str]]:
        """Given an absolute path, return (drive_path, relative_path) if it's on a known drive."""
        for drive_path in self._drives:
            if absolute_path.startswith(drive_path + os.sep) or absolute_path == drive_path:
                rel = os.path.relpath(absolute_path, drive_path)
                return (drive_path, rel)
        return None
