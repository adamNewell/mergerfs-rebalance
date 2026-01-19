"""Configuration file handling for mergerfs-balance."""

import os
from typing import Any, Optional

from .cli import BalanceConfig, parse_size


def _ensure_list(value: Any) -> list:
    """Ensure value is a list (convert single string to list)."""
    if isinstance(value, str):
        return [value]
    return list(value)


def load_config(config_path: str) -> BalanceConfig:
    """Load configuration from a YAML file.

    Requires PyYAML to be installed (optional dependency).
    """
    try:
        import yaml
    except ImportError as e:
        raise ImportError(
            "PyYAML is required to use config files. "
            "Install it with: pip install mergerfs-balance[yaml]"
        ) from e

    with open(config_path) as f:
        data = yaml.safe_load(f) or {}

    return _parse_config_dict(data, config_path)


def _parse_config_dict(data: dict[str, Any], config_path: str) -> BalanceConfig:
    """Parse a configuration dictionary into BalanceConfig."""
    # Get mount point (required)
    mount_point = data.get('mount_point', '')
    if not mount_point:
        raise ValueError(f"Config file must specify 'mount_point': {config_path}")

    # Parse optional fields
    config = BalanceConfig(mount_point=mount_point)

    if 'percentage' in data:
        config.percentage = float(data['percentage'])

    if 'include' in data:
        config.include_patterns = _ensure_list(data['include'])

    if 'exclude' in data:
        config.exclude_patterns = _ensure_list(data['exclude'])

    if 'min_size' in data:
        value = data['min_size']
        if isinstance(value, str):
            config.min_size = parse_size(value)
        else:
            config.min_size = int(value)

    if 'max_size' in data:
        value = data['max_size']
        if isinstance(value, str):
            config.max_size = parse_size(value)
        else:
            config.max_size = int(value)

    if 'parallel' in data:
        config.parallel = int(data['parallel'])

    if 'source_drives' in data:
        config.source_drives = _ensure_list(data['source_drives'])

    if 'dest_drives' in data:
        config.dest_drives = _ensure_list(data['dest_drives'])

    if 'dry_run' in data:
        config.dry_run = bool(data['dry_run'])

    if 'verbose' in data:
        config.verbose = int(data['verbose'])

    if 'quiet' in data:
        config.quiet = bool(data['quiet'])

    if 'abort_on_error' in data:
        config.abort_on_error = bool(data['abort_on_error'])

    if 'error_threshold' in data:
        config.error_threshold = int(data['error_threshold'])

    if 'error_log' in data:
        config.error_log = str(data['error_log'])

    return config


def merge_configs(file_config: BalanceConfig, cli_config: BalanceConfig) -> BalanceConfig:
    """Merge file config with CLI config. CLI takes precedence for explicitly set values.

    The file config provides defaults, and CLI arguments override them.
    """
    # Start with file config as base
    merged = BalanceConfig(
        mount_point=cli_config.mount_point or file_config.mount_point,
        percentage=file_config.percentage,
        include_patterns=file_config.include_patterns[:],
        exclude_patterns=file_config.exclude_patterns[:],
        min_size=file_config.min_size,
        max_size=file_config.max_size,
        parallel=file_config.parallel,
        source_drives=file_config.source_drives[:],
        dest_drives=file_config.dest_drives[:],
        dry_run=file_config.dry_run,
        verbose=file_config.verbose,
        quiet=file_config.quiet,
        config_file=cli_config.config_file,
        abort_on_error=file_config.abort_on_error,
        error_threshold=file_config.error_threshold,
        error_log=file_config.error_log,
    )

    # Override with CLI values if they differ from defaults
    # Percentage: CLI overrides if not default (2.0)
    if cli_config.percentage != 2.0:
        merged.percentage = cli_config.percentage

    # Include/exclude patterns: CLI adds to file patterns
    if cli_config.include_patterns:
        merged.include_patterns.extend(cli_config.include_patterns)
    if cli_config.exclude_patterns:
        merged.exclude_patterns.extend(cli_config.exclude_patterns)

    # Size limits: CLI overrides if set
    if cli_config.min_size is not None:
        merged.min_size = cli_config.min_size
    if cli_config.max_size is not None:
        merged.max_size = cli_config.max_size

    # Parallel: CLI overrides if not default (0 = auto)
    if cli_config.parallel != 0:
        merged.parallel = cli_config.parallel

    # Drives: CLI overrides if set
    if cli_config.source_drives:
        merged.source_drives = cli_config.source_drives[:]
    if cli_config.dest_drives:
        merged.dest_drives = cli_config.dest_drives[:]

    # Boolean flags: CLI overrides
    if cli_config.dry_run:
        merged.dry_run = True
    if cli_config.verbose > 0:
        merged.verbose = cli_config.verbose
    if cli_config.quiet:
        merged.quiet = True

    # Error handling: CLI overrides
    if cli_config.abort_on_error:
        merged.abort_on_error = True
    if cli_config.error_threshold != 5:
        merged.error_threshold = cli_config.error_threshold
    if cli_config.error_log:
        merged.error_log = cli_config.error_log

    return merged


def get_default_config_paths() -> list[str]:
    """Return list of default config file locations to check."""
    config_home = os.environ.get('XDG_CONFIG_HOME', os.path.expanduser('~/.config'))

    return [
        # Current directory
        'mergerfs-balance.yaml',
        'mergerfs-balance.yml',
        '.mergerfs-balance.yaml',
        '.mergerfs-balance.yml',
        # User config directory
        os.path.join(config_home, 'mergerfs-balance', 'config.yaml'),
        os.path.join(config_home, 'mergerfs-balance', 'config.yml'),
        # /etc
        '/etc/mergerfs-balance.yaml',
        '/etc/mergerfs-balance.yml',
        '/etc/mergerfs-balance/config.yaml',
        '/etc/mergerfs-balance/config.yml',
    ]


def find_config_file() -> Optional[str]:
    """Find the first existing config file from default locations."""
    for path in get_default_config_paths():
        if os.path.isfile(path):
            return path
    return None


# Example config file content for documentation
EXAMPLE_CONFIG = """
# mergerfs-balance configuration file
# Save as ~/.config/mergerfs-balance/config.yaml

# Required: mergerfs mount point
mount_point: /mnt/storage

# Target percentage range (default: 2.0)
percentage: 2.0

# File filters
include:
  - "*.mkv"
  - "*.mp4"
  - "*.avi"

exclude:
  - "*.tmp"
  - "*.partial"

# Size limits (supports K, M, G, T suffixes)
min_size: 100M
max_size: 50G

# Parallel transfers (0=auto based on drives needing balance, default: 0)
parallel: 0

# Limit to specific drives
source_drives:
  - /mnt/disk1
  - /mnt/disk2

dest_drives:
  - /mnt/disk3
  - /mnt/disk4

# Other options
dry_run: false
verbose: 1  # 0=normal, 1=verbose, 2=rich display
quiet: false
"""
