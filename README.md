# mergerfs-balance

A parallel drive balancer for mergerfs filesystems with a rich terminal UI.

## Features

- **Parallel transfers**: Move multiple files simultaneously across drives
- **Smart balancing**: Calculates target percentage based on total bytes, handling mixed drive sizes correctly
- **Rich progress display**: Real-time visualization of drive usage, active transfers, and ETA
- **Flexible filtering**: Include/exclude patterns and size filters
- **Error handling**: Configurable error thresholds with pause/abort options
- **Dry-run mode**: Preview what would be moved without making changes

## Installation

```bash
pip install .

# Or for development
pip install -e .
```

Requires Python 3.9+ and Linux (uses mergerfs xattrs).

## Usage

```bash
# Basic usage - balance all drives within 2% of each other
mergerfs-balance /mnt/storage

# Balance a subdirectory
mergerfs-balance /mnt/storage/photos

# With rich progress display
mergerfs-balance /mnt/storage -vv

# Custom target percentage
mergerfs-balance /mnt/storage -p 5.0

# Filter by file type and size
mergerfs-balance /mnt/storage -i "*.mkv" -i "*.mp4" -s 100M

# Limit source and destination drives
mergerfs-balance /mnt/storage --source /mnt/disk1 --dest /mnt/disk2

# Preview without moving files
mergerfs-balance /mnt/storage --dry-run -vv

# Run with 4 parallel transfers
mergerfs-balance /mnt/storage --parallel 4
```

## Options

| Option                  | Description                                        |
| ----------------------- | -------------------------------------------------- |
| `-p, --percentage PCT`  | Target usage range between drives (default: 2.0)   |
| `-i, --include PATTERN` | Include files matching glob pattern (repeatable)   |
| `-e, --exclude PATTERN` | Exclude files matching glob pattern (repeatable)   |
| `-s, --min-size SIZE`   | Minimum file size (e.g., 100M, 1G)                 |
| `-S, --max-size SIZE`   | Maximum file size                                  |
| `--parallel N`          | Concurrent transfers; 0=auto (default: 0)          |
| `--source PATH`         | Limit source drives (repeatable)                   |
| `--dest PATH`           | Limit destination drives (repeatable)              |
| `--dry-run`             | Preview without moving files                       |
| `-v, --verbose`         | Increase verbosity (-vv for rich UI)               |
| `-q, --quiet`           | Suppress non-error output                          |
| `--config FILE`         | Configuration file (YAML format)                   |
| `--abort-on-error`      | Abort after consecutive errors                     |
| `--error-threshold N`   | Consecutive errors before pause/abort (default: 5) |
| `--error-log FILE`      | File to log errors to                              |

## How It Works

1. **Discovery**: Reads mergerfs srcmounts via xattr to find underlying drives
2. **Analysis**: Calculates target percentage as `(total used / total capacity) * 100`
3. **Classification**:
   - **Overfull drives** (sources): usage > target + (percentage / 2)
   - **Underfull drives** (destinations): usage < target - (percentage / 2)
4. **Transfer**: Moves files from overfull to underfull drives using rsync
5. **Repeat**: Continues until all drives are within the target percentage range

## Configuration File

Create a YAML config file for persistent settings:

```yaml
percentage: 2.0
parallel: 4
include:
  - "*.mkv"
  - "*.mp4"
exclude:
  - "*.tmp"
  - "*.part"
min_size: 100M
error_threshold: 3
abort_on_error: true
```

Use with: `mergerfs-balance /mnt/storage --config balance.yaml`

## Requirements

- Python 3.9+
- Linux with mergerfs mounted
- rsync installed
- Root privileges (for moving files across drives)

## Running with sudo

Since the tool needs to move files between drives, it typically requires root:

```bash
# Preserve PATH to find the installed command
sudo env "PATH=$PATH" mergerfs-balance /mnt/storage -vv

# Or install system-wide
sudo pip install .
sudo mergerfs-balance /mnt/storage -vv
```

## License

MIT
