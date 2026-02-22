import argparse
import os
from rocotoviewer.app import RocotoApp

def main():
    parser = argparse.ArgumentParser(description="RocotoViewer — Textual interface to Rocoto")
    parser.add_argument(
        "-w", "--workflow",
        required=True,
        help="Rocoto XML workflow file",
    )
    parser.add_argument(
        "-d", "--database",
        required=True,
        help="Rocoto SQLite database file",
    )

    args = parser.parse_args()

    if not os.path.exists(args.workflow):
        print(f"Error: Workflow file not found: {args.workflow}")
        return
    if not os.path.exists(args.database):
        print(f"Error: Database file not found: {args.database}")
        return

    app = RocotoApp(workflow_file=args.workflow, database_file=args.database)
    app.run()

if __name__ == "__main__":
    main()
