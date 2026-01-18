"""Balance coordinator that orchestrates file transfers across drives."""

import fnmatch
import os
import signal
import sys
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator, Optional

from .cli import BalanceConfig
from .drives import Drive, DriveManager
from .transfer import TransferPool, TransferResult, TransferStatus, TransferWorker, format_bytes


@dataclass
class BalanceStats:
    """Statistics for the balance operation."""

    files_moved: int = 0
    bytes_transferred: int = 0
    errors: int = 0

    def add_result(self, result: TransferResult) -> None:
        """Update stats with a transfer result."""
        if result.status == TransferStatus.COMPLETED:
            self.files_moved += 1
            self.bytes_transferred += result.bytes_transferred
        elif result.status == TransferStatus.FAILED:
            self.errors += 1


class FileSelector:
    """Selects files for transfer based on filters."""

    def __init__(
        self,
        include_patterns: Optional[list[str]] = None,
        exclude_patterns: Optional[list[str]] = None,
        min_size: Optional[int] = None,
        max_size: Optional[int] = None,
    ):
        self.include_patterns = include_patterns or []
        self.exclude_patterns = exclude_patterns or []
        self.min_size = min_size
        self.max_size = max_size

    def matches_patterns(self, filename: str) -> bool:
        """Check if filename matches include/exclude patterns."""
        # If no include patterns, include everything by default
        if self.include_patterns:
            if not any(fnmatch.fnmatch(filename, p) for p in self.include_patterns):
                return False

        # Check exclude patterns
        if self.exclude_patterns:
            if any(fnmatch.fnmatch(filename, p) for p in self.exclude_patterns):
                return False

        return True

    def matches_size(self, size: int) -> bool:
        """Check if file size is within bounds."""
        if self.min_size is not None and size < self.min_size:
            return False
        if self.max_size is not None and size > self.max_size:
            return False
        return True

    def get_valid_file_size(self, path: str) -> Optional[int]:
        """Check if a file should be considered for transfer and return its size.

        Returns file size if valid, None otherwise.
        """
        if not os.path.isfile(path):
            return None

        filename = os.path.basename(path)
        if not self.matches_patterns(filename):
            return None

        try:
            size = os.path.getsize(path)
            if not self.matches_size(size):
                return None
            return size
        except OSError:
            return None

    def walk_drive(self, drive_path: str) -> Iterator[tuple[str, int]]:
        """Walk a drive and yield (file_path, file_size) for valid files.

        Uses depth-first traversal.
        """
        for root, dirs, files in os.walk(drive_path):
            # Skip hidden directories
            dirs[:] = [d for d in dirs if not d.startswith('.')]

            for filename in files:
                # Skip hidden files
                if filename.startswith('.'):
                    continue

                file_path = os.path.join(root, filename)
                size = self.get_valid_file_size(file_path)
                if size is not None:
                    yield file_path, size


class BalanceCoordinator:
    """Orchestrates the balance operation across drives."""

    def __init__(self, config: BalanceConfig):
        self.config = config
        self.stats = BalanceStats()
        self._shutdown = threading.Event()
        self._display = None
        self._consecutive_errors = 0
        self._error_paused = False

        # Open error log file if specified
        self._error_log_file = None
        if config.error_log:
            self._error_log_file = open(config.error_log, 'a')

        # Initialize drive manager
        self.drive_manager = DriveManager(
            mount_point=config.mount_point,
            source_drives=config.source_drives if config.source_drives else None,
            dest_drives=config.dest_drives if config.dest_drives else None,
        )

        # Initialize file selector
        self.file_selector = FileSelector(
            include_patterns=config.include_patterns,
            exclude_patterns=config.exclude_patterns,
            min_size=config.min_size,
            max_size=config.max_size,
        )

        # Calculate worker count: auto (0) means min(overfull, underfull) drives
        if config.parallel == 0:
            overfull = self.drive_manager.get_overfull_drives(config.percentage)
            underfull = self.drive_manager.get_underfull_drives(config.percentage)
            max_workers = max(1, min(len(overfull), len(underfull)))
        else:
            max_workers = config.parallel

        # Initialize transfer pool
        self.transfer_pool = TransferPool(max_workers=max_workers)

        # Setup signal handlers
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _signal_handler(self, signum, frame) -> None:
        """Handle shutdown signals."""
        self._shutdown.set()
        self.transfer_pool.cancel_all()

    def run(self) -> int:
        """Run the balance operation. Returns exit code."""
        if self.config.verbose >= 2:
            from .display import ProgressDisplay
            self._display = ProgressDisplay(
                self.drive_manager, self.transfer_pool, self.stats, self.config.percentage
            )
            self._display.start()

        try:
            return self._balance_loop()
        finally:
            if self._display:
                self._display.stop()
            if self._error_log_file:
                self._error_log_file.close()

    def _balance_loop(self) -> int:
        """Main balance loop."""
        iteration = 0

        while not self._shutdown.is_set():
            # Refresh drive stats
            self.drive_manager.refresh_all_stats()

            # Check if balanced
            if self.drive_manager.is_balanced(self.config.percentage):
                self._log_info("All drives are balanced.")
                break

            # Find source drives (overfull)
            sources = self.drive_manager.get_overfull_drives(self.config.percentage)
            if not sources:
                self._log_info("No overfull source drives found.")
                break

            # Try to start transfers
            transfers_started = 0

            for source_drive in sources:
                if self._shutdown.is_set():
                    break

                if not self.transfer_pool.has_capacity:
                    break

                # Find best destination
                dest_drive = self.drive_manager.get_best_destination(exclude_busy=True)
                if not dest_drive:
                    # All destinations are busy, wait for a transfer
                    break

                # Don't transfer to same drive
                if dest_drive.path == source_drive.path:
                    continue

                # Find a file to transfer
                file_info = self._find_file_to_transfer(source_drive)
                if not file_info:
                    continue

                source_path, file_size = file_info

                # Calculate destination path
                rel_path = os.path.relpath(source_path, source_drive.path)
                dest_path = os.path.join(dest_drive.path, rel_path)

                # Acquire write lock on destination
                if not self.drive_manager.acquire_write_lock(dest_drive.path):
                    continue

                if self.config.dry_run:
                    self._log_info(f"[DRY RUN] Would move: {source_path} -> {dest_path}")
                    self.drive_manager.release_write_lock(dest_drive.path)
                    self.stats.files_moved += 1
                    self.stats.bytes_transferred += file_size
                    transfers_started += 1
                else:
                    self._log_verbose(f"Starting transfer: {source_path} -> {dest_path}")

                    # Create completion callback
                    def on_complete(result: TransferResult, dest_path: str = dest_drive.path):
                        self.drive_manager.release_write_lock(dest_path)
                        self.stats.add_result(result)
                        self._handle_transfer_result(result)

                    # Create and submit transfer with completion callback
                    worker = TransferWorker(
                        source_path=source_path,
                        dest_path=dest_path,
                        file_size=file_size,
                        dry_run=self.config.dry_run,
                        on_complete=on_complete,
                    )

                    if not self.transfer_pool.submit(worker):
                        self.drive_manager.release_write_lock(dest_drive.path)
                        continue

                    transfers_started += 1

            # If no transfers started and none running, we're done
            if transfers_started == 0 and self.transfer_pool.active_count == 0:
                # Check if we need more iterations
                self.drive_manager.refresh_all_stats()
                if self.drive_manager.is_balanced(self.config.percentage):
                    self._log_info("All drives are balanced.")
                    break
                else:
                    # No progress possible
                    self._log_info("No more files can be moved.")
                    break

            # Wait for at least one transfer to complete before next iteration
            if self.transfer_pool.active_count > 0:
                result = self.transfer_pool.wait_for_any(timeout=1.0)

            iteration += 1

            # Update display
            if self._display:
                self._display.update()

        # Wait for remaining transfers
        if self.transfer_pool.active_count > 0:
            self._log_info("Waiting for remaining transfers to complete...")
            self.transfer_pool.wait_for_all()

        # Final summary
        self._print_summary()

        return 0 if self.stats.errors == 0 else 1

    def _find_file_to_transfer(self, source_drive: Drive) -> Optional[tuple[str, int]]:
        """Find the first file on source drive that can be transferred."""
        for file_path, file_size in self.file_selector.walk_drive(source_drive.path):
            # Check if destination has enough space
            dest = self.drive_manager.get_best_destination(exclude_busy=True)
            if dest and dest.stats.free_bytes > file_size:
                return file_path, file_size
        return None

    def _log_info(self, message: str) -> None:
        """Log an info message."""
        if not self.config.quiet and not self._display:
            print(message)

    def _log_verbose(self, message: str) -> None:
        """Log a verbose message."""
        if self.config.verbose >= 1 and not self.config.quiet and not self._display:
            print(message)

    def _log_error(self, message: str) -> None:
        """Log an error message to stderr and optionally to file."""
        from datetime import datetime
        timestamp = datetime.now().isoformat()
        formatted = f"ERROR: {message}"

        # Always log to stderr (even with rich display)
        print(formatted, file=sys.stderr)

        # Log to file if specified
        if self._error_log_file:
            self._error_log_file.write(f"[{timestamp}] {formatted}\n")
            self._error_log_file.flush()

    def _handle_transfer_result(self, result: TransferResult) -> None:
        """Handle a transfer result, tracking consecutive errors."""
        if result.status == TransferStatus.COMPLETED:
            self._consecutive_errors = 0  # Reset on success
            self._log_verbose(f"Completed: {result.source_path}")
        elif result.status == TransferStatus.FAILED:
            self._consecutive_errors += 1
            self._log_error(f"Failed: {result.source_path} - {result.error_message}")
            self._check_error_threshold()

    def _check_error_threshold(self) -> None:
        """Check if consecutive errors have reached threshold."""
        if self._consecutive_errors < self.config.error_threshold:
            return

        if self.config.abort_on_error:
            print(f"\nAborting: {self._consecutive_errors} consecutive errors", file=sys.stderr)
            self._shutdown.set()
            self.transfer_pool.cancel_all()
        else:
            self._prompt_continue()

    def _prompt_continue(self) -> None:
        """Prompt user to continue after consecutive errors."""
        if self._error_paused:
            return  # Already paused, waiting for input

        self._error_paused = True

        # Stop rich display temporarily if active
        if self._display:
            self._display.stop()

        print(f"\n{'='*50}", file=sys.stderr)
        print(f"WARNING: {self._consecutive_errors} consecutive errors", file=sys.stderr)
        print(f"{'='*50}", file=sys.stderr)

        try:
            response = input("Continue? [y/N]: ").strip().lower()
            if response != 'y':
                print("Aborting by user request.", file=sys.stderr)
                self._shutdown.set()
                self.transfer_pool.cancel_all()
            else:
                self._consecutive_errors = 0  # Reset counter
                if self._display:
                    self._display.start()
        except EOFError:
            # Non-interactive, abort
            print("\nNon-interactive mode, aborting.", file=sys.stderr)
            self._shutdown.set()
            self.transfer_pool.cancel_all()
        finally:
            self._error_paused = False

    def _print_summary(self) -> None:
        """Print final summary."""
        if self.config.quiet:
            return

        if self._display:
            self._display.stop()

        print()
        print("=" * 50)
        print("Balance Summary")
        print("=" * 50)
        print(f"Files moved:       {self.stats.files_moved}")
        print(f"Data transferred:  {format_bytes(self.stats.bytes_transferred)}")
        print(f"Errors:            {self.stats.errors}")
        print()

        # Print final drive status
        self.drive_manager.refresh_all_stats()
        print("Drive Status:")
        for drive in sorted(self.drive_manager.all_drives, key=lambda d: d.path):
            print(f"  {drive.path}: {drive.stats.usage_percent:.1f}% used")

        print()
        if self.drive_manager.is_balanced(self.config.percentage):
            print(f"All drives are within {self.config.percentage}% of each other.")
        else:
            range_pct = self.drive_manager.get_usage_range()
            print(f"Drives have a usage range of {range_pct:.1f}%")
