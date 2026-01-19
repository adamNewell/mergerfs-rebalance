"""Transfer worker using rsync for file operations."""

import os
import re
import subprocess
import threading
from dataclasses import dataclass
from enum import Enum
from typing import Callable, Optional


class TransferStatus(Enum):
    """Status of a transfer operation."""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass
class TransferProgress:
    """Progress information for an active transfer."""

    bytes_transferred: int = 0
    total_bytes: int = 0
    percent: float = 0.0
    speed_bytes_per_sec: float = 0.0
    eta_seconds: Optional[int] = None

    @property
    def speed_human(self) -> str:
        """Return speed in human-readable format."""
        return format_bytes(self.speed_bytes_per_sec) + "/s"


@dataclass
class TransferResult:
    """Result of a completed transfer operation."""

    source_path: str
    dest_path: str
    status: TransferStatus
    bytes_transferred: int = 0
    error_message: Optional[str] = None
    duration_seconds: float = 0.0


def format_bytes(num_bytes: float) -> str:
    """Format bytes into human-readable string."""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if abs(num_bytes) < 1024.0:
            return f"{num_bytes:.1f}{unit}"
        num_bytes /= 1024.0
    return f"{num_bytes:.1f}PB"


def parse_rsync_progress(line: str) -> Optional[TransferProgress]:
    """Parse rsync --info=progress2 output line.

    Example line: "  1,234,567  50%   12.34MB/s    0:01:23"
    """
    # Match rsync progress format
    pattern = r'^\s*([\d,]+)\s+(\d+)%\s+([\d.]+)([KMG]?B)/s\s+(\d+:\d+:\d+|\d+:\d+)?'
    match = re.match(pattern, line)
    if not match:
        return None

    bytes_str = match.group(1).replace(',', '')
    percent = int(match.group(2))
    speed_num = float(match.group(3))
    speed_unit = match.group(4)
    eta_str = match.group(5)

    # Convert speed to bytes/sec
    unit_multipliers = {'B': 1, 'KB': 1024, 'MB': 1024**2, 'GB': 1024**3}
    speed = speed_num * unit_multipliers.get(speed_unit, 1)

    # Parse ETA
    eta_seconds = None
    if eta_str:
        parts = eta_str.split(':')
        if len(parts) == 2:
            eta_seconds = int(parts[0]) * 60 + int(parts[1])
        elif len(parts) == 3:
            eta_seconds = int(parts[0]) * 3600 + int(parts[1]) * 60 + int(parts[2])

    return TransferProgress(
        bytes_transferred=int(bytes_str),
        total_bytes=0,  # rsync doesn't always report this in progress2
        percent=percent,
        speed_bytes_per_sec=speed,
        eta_seconds=eta_seconds,
    )


class TransferWorker:
    """Executes file transfers using rsync."""

    def __init__(
        self,
        source_path: str,
        dest_path: str,
        file_size: int = 0,
        dry_run: bool = False,
        progress_callback: Optional[Callable[[TransferProgress], None]] = None,
        on_complete: Optional[Callable[["TransferResult"], None]] = None,
    ):
        self.source_path = source_path
        self.dest_path = dest_path
        self.file_size = file_size
        self.dry_run = dry_run
        self.progress_callback = progress_callback
        self.on_complete = on_complete

        self.status = TransferStatus.PENDING
        self.progress = TransferProgress(total_bytes=file_size)
        self.result: Optional[TransferResult] = None

        self._process: Optional[subprocess.Popen] = None
        self._cancelled = threading.Event()
        self._lock = threading.Lock()

    def run(self) -> TransferResult:
        """Execute the transfer and return the result."""
        import time
        start_time = time.time()

        with self._lock:
            if self._cancelled.is_set():
                self.status = TransferStatus.CANCELLED
                result = TransferResult(
                    source_path=self.source_path,
                    dest_path=self.dest_path,
                    status=TransferStatus.CANCELLED,
                )
                if self.on_complete:
                    self.on_complete(result)
                return result
            self.status = TransferStatus.RUNNING

        try:
            if self.dry_run:
                # Simulate transfer for dry run
                self.progress.percent = 100.0
                self.progress.bytes_transferred = self.file_size
                result = TransferResult(
                    source_path=self.source_path,
                    dest_path=self.dest_path,
                    status=TransferStatus.COMPLETED,
                    bytes_transferred=self.file_size,
                    duration_seconds=time.time() - start_time,
                )
            else:
                result = self._run_rsync()
                result.duration_seconds = time.time() - start_time

            with self._lock:
                self.status = result.status
                self.result = result

            if self.on_complete:
                self.on_complete(result)
            return result

        except Exception as e:
            result = TransferResult(
                source_path=self.source_path,
                dest_path=self.dest_path,
                status=TransferStatus.FAILED,
                error_message=str(e),
                duration_seconds=time.time() - start_time,
            )
            with self._lock:
                self.status = TransferStatus.FAILED
                self.result = result
            if self.on_complete:
                self.on_complete(result)
            return result

    def _run_rsync(self) -> TransferResult:
        """Execute rsync command and parse progress."""
        # Ensure destination directory exists
        dest_dir = os.path.dirname(self.dest_path)
        os.makedirs(dest_dir, exist_ok=True)

        # Build rsync command
        cmd = [
            'rsync',
            '-a',                    # archive mode
            '--remove-source-files', # delete source after successful transfer
            '--info=progress2',      # show progress info
            '--no-inc-recursive',    # don't use incremental recursion
            self.source_path,
            self.dest_path,
        ]

        try:
            self._process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                bufsize=1,
            )

            # Collect stderr in a background thread to prevent deadlock
            # if stderr output exceeds the 64KB pipe buffer
            stderr_lines: list[str] = []

            def drain_stderr():
                """Read stderr in background to prevent pipe buffer deadlock."""
                try:
                    for line in self._process.stderr:
                        stderr_lines.append(line)
                except (ValueError, OSError):
                    # Pipe closed, ignore
                    pass

            stderr_thread = threading.Thread(target=drain_stderr, daemon=True)
            stderr_thread.start()

            # Read progress from stdout
            while True:
                if self._cancelled.is_set():
                    self._process.terminate()
                    try:
                        self._process.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        self._process.kill()
                    stderr_thread.join(timeout=1)
                    return TransferResult(
                        source_path=self.source_path,
                        dest_path=self.dest_path,
                        status=TransferStatus.CANCELLED,
                    )

                line = self._process.stdout.readline()
                if not line:
                    break

                progress = parse_rsync_progress(line.strip())
                if progress:
                    progress.total_bytes = self.file_size
                    self.progress = progress
                    if self.progress_callback:
                        self.progress_callback(progress)

            # Wait for process to complete
            returncode = self._process.wait()
            stderr_thread.join(timeout=5)
            stderr = ''.join(stderr_lines)

            if returncode == 0:
                # Clean up empty parent directories on source
                self._cleanup_empty_dirs(os.path.dirname(self.source_path))

                return TransferResult(
                    source_path=self.source_path,
                    dest_path=self.dest_path,
                    status=TransferStatus.COMPLETED,
                    bytes_transferred=self.file_size,
                )
            else:
                return TransferResult(
                    source_path=self.source_path,
                    dest_path=self.dest_path,
                    status=TransferStatus.FAILED,
                    error_message=stderr.strip() if stderr else f"rsync exited with code {returncode}",
                )

        except FileNotFoundError:
            return TransferResult(
                source_path=self.source_path,
                dest_path=self.dest_path,
                status=TransferStatus.FAILED,
                error_message="rsync not found. Please install rsync.",
            )

    def _cleanup_empty_dirs(self, dir_path: str) -> None:
        """Remove empty directories up the tree."""
        try:
            while dir_path:
                if not os.listdir(dir_path):
                    os.rmdir(dir_path)
                    dir_path = os.path.dirname(dir_path)
                else:
                    break
        except OSError:
            pass  # Ignore errors during cleanup

    def cancel(self) -> None:
        """Cancel the transfer."""
        self._cancelled.set()
        with self._lock:
            if self._process and self._process.poll() is None:
                self._process.terminate()


class TransferPool:
    """Manages a pool of concurrent transfers using ThreadPoolExecutor."""

    def __init__(self, max_workers: int = 1):
        from concurrent.futures import ThreadPoolExecutor

        self.max_workers = max_workers
        self._executor = ThreadPoolExecutor(max_workers=max_workers)
        self._workers: list[TransferWorker] = []
        self._completed: list[TransferResult] = []
        self._in_flight_paths: set[str] = set()
        self._lock = threading.Lock()

    @property
    def active_count(self) -> int:
        """Return number of currently active transfers."""
        with self._lock:
            return sum(1 for w in self._workers if w.status == TransferStatus.RUNNING)

    @property
    def has_capacity(self) -> bool:
        """Check if pool can accept more transfers."""
        return self.active_count < self.max_workers

    @property
    def active_workers(self) -> list[TransferWorker]:
        """Return list of active workers."""
        with self._lock:
            return [w for w in self._workers if w.status == TransferStatus.RUNNING]

    def is_path_in_flight(self, path: str) -> bool:
        """Check if a source path is already being transferred."""
        with self._lock:
            return path in self._in_flight_paths

    def submit(self, worker: TransferWorker) -> bool:
        """Submit a worker for execution. Returns False if pool is full or path already in flight."""
        if not self.has_capacity:
            return False

        with self._lock:
            # Check if this file is already being transferred
            if worker.source_path in self._in_flight_paths:
                return False
            self._in_flight_paths.add(worker.source_path)
            self._workers.append(worker)

        def run_and_collect():
            result = worker.run()
            with self._lock:
                self._completed.append(result)
                # Remove from in-flight tracking
                self._in_flight_paths.discard(worker.source_path)
                # Clean up completed workers
                self._workers[:] = [w for w in self._workers if w.status == TransferStatus.RUNNING]

        self._executor.submit(run_and_collect)
        return True

    def wait_for_any(self, timeout: Optional[float] = None) -> Optional[TransferResult]:
        """Wait for any transfer to complete and return its result."""
        import time
        start = time.time()

        while True:
            with self._lock:
                if self._completed:
                    return self._completed.pop(0)

            if timeout is not None and (time.time() - start) >= timeout:
                return None

            time.sleep(0.1)

    def wait_for_all(self) -> list[TransferResult]:
        """Wait for all transfers to complete."""
        self._executor.shutdown(wait=True)
        # Re-create executor for potential future use
        from concurrent.futures import ThreadPoolExecutor
        self._executor = ThreadPoolExecutor(max_workers=self.max_workers)

        with self._lock:
            results = self._completed[:]
            self._completed.clear()
            self._workers.clear()
            self._in_flight_paths.clear()

        return results

    def cancel_all(self) -> None:
        """Cancel all active transfers."""
        with self._lock:
            workers = self._workers[:]

        for worker in workers:
            worker.cancel()
