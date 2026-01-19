"""Rich progress display for balance operations."""

import os
import threading
import time
from typing import TYPE_CHECKING, Optional

from rich.console import Console
from rich.live import Live
from rich.panel import Panel
from rich.table import Table

from .transfer import format_bytes


def format_duration(seconds: int) -> str:
    """Format seconds into human-readable duration like '2h 15m' or '45m'."""
    if seconds < 60:
        return "< 1m"

    minutes = seconds // 60
    hours = minutes // 60
    days = hours // 24

    if days > 0:
        remaining_hours = hours % 24
        return f"{days}d {remaining_hours}h"
    elif hours > 0:
        remaining_minutes = minutes % 60
        return f"{hours}h {remaining_minutes}m"
    else:
        return f"{minutes}m"


class ETATracker:
    """Tracks transfer speeds and estimates time to balance completion."""

    def __init__(self, drive_manager: "DriveManager"):
        self.drive_manager = drive_manager
        self._samples: list[tuple[float, float]] = []  # (timestamp, bytes_per_sec)
        self._sample_window = 30  # seconds

    def get_bytes_remaining(self) -> int:
        """Calculate total bytes that need to move to reach balance."""
        target_pct = self.drive_manager.get_average_usage() / 100
        total_to_move = 0
        for drive in self.drive_manager.all_drives:
            target_bytes = drive.stats.total_bytes * target_pct
            excess = drive.stats.used_bytes - target_bytes
            if excess > 0:
                total_to_move += excess
        return int(total_to_move)

    def add_sample(self, bytes_per_sec: float) -> None:
        """Add speed sample, prune old samples."""
        now = time.time()
        self._samples.append((now, bytes_per_sec))
        cutoff = now - self._sample_window
        self._samples = [(t, s) for t, s in self._samples if t > cutoff]

    def get_average_speed(self) -> float:
        """Get rolling average speed in bytes per second."""
        if not self._samples:
            return 0.0
        return sum(s for _, s in self._samples) / len(self._samples)

    def get_eta_seconds(self) -> "Optional[int]":
        """Return estimated seconds to completion, or None if unknown."""
        remaining = self.get_bytes_remaining()
        avg_speed = self.get_average_speed()
        if avg_speed <= 0:
            return None
        return int(remaining / avg_speed)


if TYPE_CHECKING:
    from .balance import BalanceStats
    from .drives import DriveManager
    from .transfer import TransferPool


class ProgressDisplay:
    """Rich terminal UI for displaying balance progress."""

    def __init__(
        self,
        drive_manager: "DriveManager",
        transfer_pool: "TransferPool",
        stats: "BalanceStats",
        target_percentage: float = 2.0,
    ):
        self.drive_manager = drive_manager
        self.transfer_pool = transfer_pool
        self.stats = stats
        self.target_percentage = target_percentage
        self.eta_tracker = ETATracker(drive_manager)

        self.console = Console()
        self._live: Live | None = None
        self._stop_event = threading.Event()
        self._update_thread: threading.Thread | None = None

    def start(self) -> None:
        """Start the progress display."""
        self._stop_event.clear()
        self._live = Live(
            self._render(),
            console=self.console,
            refresh_per_second=4,
            transient=True,
        )
        self._live.start()

        # Start background update thread
        self._update_thread = threading.Thread(target=self._update_loop, daemon=True)
        self._update_thread.start()

    def stop(self) -> None:
        """Stop the progress display."""
        self._stop_event.set()
        if self._update_thread:
            self._update_thread.join(timeout=1.0)
        if self._live:
            self._live.stop()
            self._live = None

    def update(self) -> None:
        """Trigger a display update."""
        if self._live:
            self._live.update(self._render())

    def _update_loop(self) -> None:
        """Background loop to update display."""
        while not self._stop_event.is_set():
            self._update_speed_samples()
            self.update()
            time.sleep(0.25)

    def _update_speed_samples(self) -> None:
        """Aggregate current transfer speeds into ETA tracker."""
        total_speed = 0.0
        for worker in self.transfer_pool.active_workers:
            total_speed += worker.progress.speed_bytes_per_sec
        if total_speed > 0:
            self.eta_tracker.add_sample(total_speed)

    def _render(self) -> Panel:
        """Render the complete display."""
        # Create main table
        table = Table.grid(padding=(0, 1))
        table.add_column()

        # Target info
        table.add_row(f"[bold]Target:[/bold] all drives within {self.target_percentage}% of each other")
        table.add_row("")

        # Drive usage section
        table.add_row("[bold]Drive Usage:[/bold]")
        table.add_row(self._render_drives())
        table.add_row("")

        # Active transfers section (always show fixed slots)
        active = self.transfer_pool.active_workers
        max_workers = self.transfer_pool.max_workers
        table.add_row(f"[bold]Active Transfers ({len(active)}/{max_workers}):[/bold]")
        table.add_row(self._render_transfers())
        table.add_row("")

        # ETA section
        remaining_bytes = self.eta_tracker.get_bytes_remaining()
        eta_seconds = self.eta_tracker.get_eta_seconds()
        avg_speed = self.eta_tracker.get_average_speed()

        if remaining_bytes > 0:
            remaining_str = format_bytes(remaining_bytes)
            if avg_speed > 0:
                speed_str = f"{format_bytes(int(avg_speed))}/s"
                if eta_seconds is not None:
                    eta_str = format_duration(eta_seconds)
                    table.add_row(f"[bold]Remaining:[/bold] ~{remaining_str} to move | ETA: ~{eta_str} (at {speed_str})")
                else:
                    table.add_row(f"[bold]Remaining:[/bold] ~{remaining_str} to move | ETA: calculating...")
            else:
                table.add_row(f"[bold]Remaining:[/bold] ~{remaining_str} to move | ETA: calculating...")
        else:
            table.add_row("[bold]Remaining:[/bold] finishing active transfers...")

        table.add_row("")

        # Progress summary
        bytes_str = format_bytes(self.stats.bytes_transferred)
        table.add_row(
            f"[bold]Progress:[/bold] {self.stats.files_moved} files moved, "
            f"{bytes_str} transferred"
            + (f", {self.stats.errors} errors" if self.stats.errors > 0 else "")
        )

        return Panel(
            table,
            title="[bold blue]mergerfs-balance[/bold blue]",
            border_style="blue",
        )

    def _render_drives(self) -> Table:
        """Render drive usage table."""
        table = Table.grid(padding=(0, 2))
        table.add_column(width=20)  # Drive path
        table.add_column(width=30)  # Progress bar
        table.add_column(width=8)   # Percentage
        table.add_column(width=15)  # Status

        drives = sorted(self.drive_manager.all_drives, key=lambda d: d.path)

        for drive in drives:
            # Create progress bar
            usage = drive.stats.usage_percent
            bar = self._make_bar(usage)

            # Determine status
            if drive.write_locked:
                status = "[yellow](dest: busy)[/yellow]"
            elif drive in self.drive_manager.source_drives:
                if usage > self.drive_manager.get_average_usage():
                    status = "[red](source)[/red]"
                else:
                    status = ""
            else:
                status = "[green](dest)[/green]"

            # Truncate drive path if needed
            drive_name = os.path.basename(drive.path) or drive.path
            if len(drive_name) > 18:
                drive_name = drive_name[:15] + "..."

            table.add_row(
                f"  {drive_name}",
                bar,
                f"{usage:.1f}%",
                status,
            )

        return table

    def _make_bar(self, percent: float, width: int = 20) -> str:
        """Create a text-based progress bar."""
        filled = int(width * percent / 100)
        empty = width - filled

        # Color based on usage
        if percent >= 90:
            color = "red"
        elif percent >= 75:
            color = "yellow"
        else:
            color = "green"

        return f"[{color}]{'█' * filled}[/{color}][dim]{'░' * empty}[/dim]"

    def _render_transfers(self) -> Table:
        """Render active transfers table with fixed slots."""
        table = Table.grid(padding=(0, 1))
        table.add_column(width=4)   # Index
        table.add_column(width=50)  # Transfer info
        table.add_column(width=20)  # Progress

        workers = self.transfer_pool.active_workers
        max_workers = self.transfer_pool.max_workers

        # Create lookup of active workers by slot index
        active_by_slot: dict[int, Optional[object]] = {}
        for i, worker in enumerate(workers):
            active_by_slot[i] = worker

        # Always render max_workers rows for stable UI
        for slot in range(max_workers):
            worker = active_by_slot.get(slot)
            if worker:
                # Get source and dest drive names
                source_name = os.path.basename(os.path.dirname(worker.source_path))
                dest_name = os.path.basename(os.path.dirname(worker.dest_path))
                filename = os.path.basename(worker.source_path)

                # Truncate filename if too long
                if len(filename) > 25:
                    filename = filename[:22] + "..."

                # Format size
                size_str = format_bytes(worker.file_size)

                # Progress info
                progress = worker.progress
                bar = self._make_bar(progress.percent, width=12)
                speed = progress.speed_human if progress.speed_bytes_per_sec > 0 else "..."

                table.add_row(
                    f"  [{slot + 1}]",
                    f"{source_name} → {dest_name}: {filename} ({size_str})",
                    f"{bar} {progress.percent:.0f}% {speed}",
                )
            else:
                # Empty placeholder for unused slot
                table.add_row(f"  [{slot + 1}]", "[dim]—[/dim]", "")

        return table
