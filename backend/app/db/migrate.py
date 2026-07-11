from __future__ import annotations

import argparse

from app.db.migrations import (
    current_revision,
    downgrade_database,
    stamp_legacy_database,
    upgrade_database,
)


def main() -> int:
    """Run explicit database migration operations from the command line."""

    parser = argparse.ArgumentParser(description="Keiba AI Studio database migration utility")
    subparsers = parser.add_subparsers(dest="command", required=True)
    upgrade_parser = subparsers.add_parser("upgrade")
    upgrade_parser.add_argument("revision", nargs="?", default="head")
    downgrade_parser = subparsers.add_parser("downgrade")
    downgrade_parser.add_argument("revision")
    subparsers.add_parser("current")
    subparsers.add_parser("stamp-legacy")
    args = parser.parse_args()

    if args.command == "upgrade":
        upgrade_database(args.revision)
    elif args.command == "downgrade":
        downgrade_database(args.revision)
    elif args.command == "stamp-legacy":
        stamp_legacy_database()
    else:
        print(current_revision() or "unversioned")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

