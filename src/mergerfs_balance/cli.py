"""Command-line interface for mergerfs-balance."""

import argparse
import os
import re
import sys
from dataclasses import dataclass, field
from typing import Optional


def parse_size(size_str: str) -> int:
    """Parse a size string like '100M', '50GB', or '1TiB' into bytes.

    Supports:
        - Raw bytes: '1024'
        - Single-char units: '100K', '50M', '1G', '2T', '1P'
        - Two-char units: '100KB', '50MB', '1GB', '2TB', '1PB'
        - IEC units: '100KiB', '50MiB', '1GiB', '2TiB', '1PiB'
        - Decimal values: '1.5G', '2.5TB'
        - Whitespace: ' 100 MB '
    """
    size_str = size_str.strip().upper()

    if not size_str:
        raise ValueError("Empty size string")

    # Pattern: optional number (int or float), optional whitespace, optional unit
    pattern = r'^(\d+(?:\.\d+)?)\s*([A-Z]*)$'
    match = re.match(pattern, size_str)

    if not match:
        raise ValueError(f"Invalid size format: {size_str}")

    number_str = match.group(1)
    unit_str = match.group(2)

    try:
        value = float(number_str)
    except ValueError as e:
        raise ValueError(f"Invalid size format: {size_str}") from e

    # Map unit strings to multipliers (binary, 1024-based)
    unit_multipliers = {
        '': 1,          # No unit = bytes
        'B': 1,         # Bytes
        'K': 1024,      # Kilobytes
        'KB': 1024,
        'KIB': 1024,
        'M': 1024 ** 2,  # Megabytes
        'MB': 1024 ** 2,
        'MIB': 1024 ** 2,
        'G': 1024 ** 3,  # Gigabytes
        'GB': 1024 ** 3,
        'GIB': 1024 ** 3,
        'T': 1024 ** 4,  # Terabytes
        'TB': 1024 ** 4,
        'TIB': 1024 ** 4,
        'P': 1024 ** 5,  # Petabytes
        'PB': 1024 ** 5,
        'PIB': 1024 ** 5,
    }

    if unit_str not in unit_multipliers:
        raise ValueError(f"Unknown size unit: {unit_str}")

    return int(value * unit_multipliers[unit_str])


@dataclass
class BalanceConfig:
    """Configuration for the balance operation."""

    # Mount point (positional argument)
    mount_point: str

    # mergerfs.balance compatible options
    percentage: float = 2.0
    include_patterns: list[str] = field(default_factory=list)
    exclude_patterns: list[str] = field(default_factory=list)
    min_size: Optional[int] = None
    max_size: Optional[int] = None

    # New enhancement options
    parallel: int = 0  # 0 = auto (min of source/dest drives needing balance)
    source_drives: list[str] = field(default_factory=list)
    dest_drives: list[str] = field(default_factory=list)
    dry_run: bool = False
    verbose: int = 0  # 0=normal, 1=verbose, 2=very verbose (rich)
    quiet: bool = False
    config_file: Optional[str] = None
    abort_on_error: bool = False  # Abort immediately after error_threshold consecutive errors
    error_threshold: int = 5  # Consecutive errors before pausing/aborting
    error_log: Optional[str] = None  # File to log errors to

    def validate(self) -> list[str]:
        """Validate configuration, return list of errors."""
        errors = []

        if not os.path.isdir(self.mount_point):
            errors.append(f"Mount point does not exist: {self.mount_point}")

        if self.percentage <= 0:
            errors.append(f"Percentage must be positive: {self.percentage}")

        if self.parallel < 0:
            errors.append(f"Parallel must be 0 (auto) or positive: {self.parallel}")

        if self.min_size is not None and self.max_size is not None:
            if self.min_size > self.max_size:
                errors.append(f"Min size ({self.min_size}) cannot be greater than max size ({self.max_size})")

        for drive in self.source_drives:
            if not os.path.isdir(drive):
                errors.append(f"Source drive does not exist: {drive}")

        for drive in self.dest_drives:
            if not os.path.isdir(drive):
                errors.append(f"Destination drive does not exist: {drive}")

        if self.config_file and not os.path.isfile(self.config_file):
            errors.append(f"Config file does not exist: {self.config_file}")

        return errors


def create_parser() -> argparse.ArgumentParser:
    """Create the argument parser with mergerfs.balance compatible interface."""
    parser = argparse.ArgumentParser(
        prog='mergerfs-balance',
        description='Balance files across mergerfs drives with parallel transfers.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s /mnt/storage
  %(prog)s /mnt/storage -p 2.0 -i "*.mkv" -e "*.tmp"
  %(prog)s /mnt/storage --parallel 4 --dry-run -vv
  %(prog)s /mnt/storage --source /mnt/disk1 --dest /mnt/disk2 --dest /mnt/disk3
"""
    )

    # Positional argument
    parser.add_argument(
        'mount_point',
        help='mergerfs mount point to balance'
    )

    # mergerfs.balance compatible options
    parser.add_argument(
        '-p', '--percentage',
        type=float,
        default=2.0,
        metavar='PCT',
        help='target percentage range for balance (default: 2.0)'
    )

    parser.add_argument(
        '-i', '--include',
        action='append',
        default=[],
        metavar='PATTERN',
        dest='include_patterns',
        help='include files matching glob pattern (repeatable)'
    )

    parser.add_argument(
        '-e', '--exclude',
        action='append',
        default=[],
        metavar='PATTERN',
        dest='exclude_patterns',
        help='exclude files matching glob pattern (repeatable)'
    )

    parser.add_argument(
        '-s', '--min-size',
        type=parse_size,
        default=None,
        metavar='SIZE',
        help='minimum file size (e.g., 100M, 1G)'
    )

    parser.add_argument(
        '-S', '--max-size',
        type=parse_size,
        default=None,
        metavar='SIZE',
        help='maximum file size (e.g., 50G)'
    )

    # New enhancement options
    parser.add_argument(
        '--parallel',
        type=int,
        default=0,
        metavar='N',
        help='concurrent transfers; 0=auto based on drives needing balance (default: 0)'
    )

    parser.add_argument(
        '--source',
        action='append',
        default=[],
        metavar='PATH',
        dest='source_drives',
        help='limit source drives (repeatable)'
    )

    parser.add_argument(
        '--dest',
        action='append',
        default=[],
        metavar='PATH',
        dest='dest_drives',
        help='limit destination drives (repeatable)'
    )

    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='preview without moving files'
    )

    verbosity = parser.add_mutually_exclusive_group()
    verbosity.add_argument(
        '-v', '--verbose',
        action='count',
        default=0,
        help='increase verbosity (-v for verbose, -vv for rich progress)'
    )

    verbosity.add_argument(
        '-q', '--quiet',
        action='store_true',
        help='suppress non-error output'
    )

    parser.add_argument(
        '--config',
        metavar='FILE',
        dest='config_file',
        help='configuration file (YAML format)'
    )

    parser.add_argument(
        '--abort-on-error',
        action='store_true',
        help='abort after consecutive errors (default: pause and prompt)'
    )

    parser.add_argument(
        '--error-threshold',
        type=int,
        default=5,
        metavar='N',
        help='consecutive errors before pausing/aborting (default: 5)'
    )

    parser.add_argument(
        '--error-log',
        metavar='FILE',
        help='file to log errors to'
    )

    parser.add_argument(
        '--version',
        action='version',
        version='%(prog)s 0.1.0'
    )

    return parser


def parse_args(args: Optional[list[str]] = None) -> BalanceConfig:
    """Parse command-line arguments and return a BalanceConfig."""
    parser = create_parser()
    parsed = parser.parse_args(args)

    config = BalanceConfig(
        mount_point=parsed.mount_point,
        percentage=parsed.percentage,
        include_patterns=parsed.include_patterns,
        exclude_patterns=parsed.exclude_patterns,
        min_size=parsed.min_size,
        max_size=parsed.max_size,
        parallel=parsed.parallel,
        source_drives=parsed.source_drives,
        dest_drives=parsed.dest_drives,
        dry_run=parsed.dry_run,
        verbose=parsed.verbose,
        quiet=parsed.quiet,
        config_file=parsed.config_file,
        abort_on_error=parsed.abort_on_error,
        error_threshold=parsed.error_threshold,
        error_log=parsed.error_log,
    )

    return config


def main_cli() -> int:
    """Main CLI entry point. Returns exit code."""
    config = parse_args()

    # Load config file if specified (merges with CLI args)
    if config.config_file:
        from .config import load_config, merge_configs
        file_config = load_config(config.config_file)
        config = merge_configs(file_config, config)

    # Validate configuration
    errors = config.validate()
    if errors:
        for error in errors:
            print(f"Error: {error}", file=sys.stderr)
        return 1

    # Run the balance operation
    from .balance import BalanceCoordinator

    coordinator = BalanceCoordinator(config)
    try:
        return coordinator.run()
    except KeyboardInterrupt:
        if not config.quiet:
            print("\nInterrupted by user", file=sys.stderr)
        return 130
