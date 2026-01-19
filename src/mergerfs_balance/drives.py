"""Drive discovery and management for mergerfs filesystems."""

import ctypes
import errno
import os
import shutil
import threading
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

# ctypes interface to lgetxattr (same method as mergerfs.balance)
_libc = ctypes.CDLL("libc.so.6", use_errno=True)
_lgetxattr = _libc.lgetxattr
_lgetxattr.argtypes = [ctypes.c_char_p, ctypes.c_char_p, ctypes.c_void_p, ctypes.c_size_t]


def lgetxattr(path: str, name: str) -> Optional[str]:
    """Read an extended attribute from a file using lgetxattr syscall."""
    path_bytes = path.encode(errors='backslashreplace')
    name_bytes = name.encode(errors='backslashreplace')

    length = 64
    while True:
        buf = ctypes.create_string_buffer(length)
        res = _lgetxattr(path_bytes, name_bytes, buf, ctypes.c_size_t(length))
        if res >= 0:
            return buf.raw[0:res].decode(errors='backslashreplace')
        else:
            err = ctypes.get_errno()
            if err == errno.ERANGE:
                length *= 2
            elif err == errno.ENODATA:
                return None
            else:
                raise OSError(err, os.strerror(err), path)


def ismergerfs(path: str) -> bool:
    """Check if a path is on a mergerfs filesystem."""
    try:
        lgetxattr(path, 'user.mergerfs.version')
        return True
    except OSError:
        return False


def mergerfs_control_file(basedir: str) -> Optional[str]:
    """Find the .mergerfs control file by walking up the directory tree."""
    current = basedir
    while current != '/':
        ctrlfile = os.path.join(current, '.mergerfs')
        if os.path.exists(ctrlfile):
            return ctrlfile
        current = os.path.dirname(current)
    return None


def mergerfs_srcmounts(ctrlfile: str) -> list[str]:
    """Get the source mounts from a mergerfs control file."""
    srcmounts = lgetxattr(ctrlfile, 'user.mergerfs.srcmounts')
    if srcmounts:
        return srcmounts.split(':')
    return []


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


def discover_mergerfs_drives(mount_point: str) -> tuple[list[str], str]:
    """Discover underlying drives from a mergerfs mount point.

    Uses the same method as mergerfs.balance: reads user.mergerfs.srcmounts
    xattr from the .mergerfs control file.

    Returns:
        Tuple of (srcmounts list, subpath relative to mergerfs root)
        The subpath is used to walk only the relevant subdirectory on each drive.
    """
    mount_point = os.path.realpath(mount_point)

    # Find the control file
    ctrlfile = mergerfs_control_file(mount_point)
    if not ctrlfile:
        raise ValueError(f"Could not find .mergerfs control file for: {mount_point}")

    # Verify it's a mergerfs mount
    if not ismergerfs(ctrlfile):
        raise ValueError(f"{mount_point} is not a mergerfs mount")

    # Get source mounts
    srcmounts = mergerfs_srcmounts(ctrlfile)
    if not srcmounts:
        raise ValueError(f"Could not read srcmounts from: {ctrlfile}")

    # Calculate subpath: relative path from mergerfs root to user-specified path
    # Control file is at <mergerfs_root>/.mergerfs, so root is its parent
    mergerfs_root = os.path.dirname(ctrlfile)
    if mount_point.startswith(mergerfs_root):
        subpath = os.path.relpath(mount_point, mergerfs_root)
        if subpath == '.':
            subpath = ''
    else:
        subpath = ''

    return srcmounts, subpath


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

        # Discover all drives and the subpath to walk
        discovered, self.subpath = discover_mergerfs_drives(mount_point)
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

    def get_walk_path(self, drive: Drive) -> str:
        """Get the path to walk for a drive, including subpath if specified."""
        if self.subpath:
            return os.path.join(drive.path, self.subpath)
        return drive.path

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
        """Calculate target usage percentage based on total bytes across all drives.

        This calculates (total used bytes / total capacity bytes) * 100, which gives
        the correct target percentage when drives have different sizes.
        """
        if not self._drives:
            return 0.0
        total_used = sum(d.stats.used_bytes for d in self._drives.values())
        total_capacity = sum(d.stats.total_bytes for d in self._drives.values())
        if total_capacity == 0:
            return 0.0
        return (total_used / total_capacity) * 100

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

    def get_best_destination(
        self, target_percentage: float, exclude_busy: bool = True
    ) -> Optional[Drive]:
        """Get the best underfull destination drive that isn't write-locked.

        Only considers drives that are below the target threshold (underfull).
        Among underfull drives, returns the one with most free space.
        """
        avg = self.get_average_usage()
        threshold = avg - (target_percentage / 2)

        # Only consider underfull drives as destinations
        candidates = [d for d in self.dest_drives if d.stats.usage_percent < threshold]
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
