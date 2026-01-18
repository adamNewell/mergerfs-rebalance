"""Entry point for mergerfs-balance when run as a module."""

import sys

from .cli import main_cli


def main() -> None:
    """Main entry point."""
    sys.exit(main_cli())


if __name__ == "__main__":
    main()
