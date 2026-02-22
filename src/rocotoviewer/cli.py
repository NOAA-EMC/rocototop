"""
.. note:: warning: "If you modify features, API, or usage, you MUST update the documentation immediately."
"""
from __future__ import annotations

import argparse
import os
import sys

from rocotoviewer.app import RocotoApp


def main() -> None:
    """
    Main entry point for the RocotoViewer CLI.

    This function parses command-line arguments and launches the Textual application.
    """
    parser = argparse.ArgumentParser(
        description="RocotoViewer — Textual interface to Rocoto"
    )
    parser.add_argument(
        "-w",
        "--workflow",
        required=True,
        help="Rocoto XML workflow file",
    )
    parser.add_argument(
        "-d",
        "--database",
        required=True,
        help="Rocoto SQLite database file",
    )

    args = parser.parse_args()

    if not os.path.exists(args.workflow):
        print(f"Error: Workflow file not found: {args.workflow}")
        sys.exit(1)
    if not os.path.exists(args.database):
        print(f"Error: Database file not found: {args.database}")
        sys.exit(1)

    app = RocotoApp(workflow_file=args.workflow, database_file=args.database)
    app.run()


if __name__ == "__main__":
    main()
