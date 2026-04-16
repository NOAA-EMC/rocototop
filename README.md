[![License: CC0-1.0](https://img.shields.io/badge/License-CC0_1.0-lightgrey.svg)](LICENSE)
[![Docs](https://img.shields.io/badge/docs-GitHub%20Pages-blue)](https://NOAA-EMC.github.io/rocototop/)
[![Disclaimer](https://img.shields.io/badge/disclaimer-read%20first-yellow)](DISCLAIMER.md)

# RocotoTop

RocotoTop is a powerful Textual-based Terminal User Interface (TUI) for monitoring and interacting with Rocoto workflows in real-time.

![Overview](docs/screenshots/overview.svg)

## Features

- **Real-time Monitoring**: View the status of your Rocoto tasks and cycles in a hierarchical tree view.
- **Log Tailing**: View and follow task logs in real-time within the TUI.
- **Workflow Inspection**: Quickly see task states, exit statuses, and durations.
- **Modern TUI**: Built with [Textual](https://textual.textualize.io/) for a smooth and responsive terminal experience.
- **Easy Integration**: Simple command-line interface for specifying workflows and databases.

## Installation

### Prerequisites

- Python 3.9 or higher
- pip package manager

### From Source

1. Clone the repository:
   ```bash
   git clone https://github.com/rocototop/rocototop.git
   cd rocototop
   ```

2. Install the package:
   ```bash
   pip install .
   ```

## Usage

Start RocotoTop by providing the workflow XML and the database file:

```bash
rocototop -w my_workflow.xml -d my_database.db
```

## Key Bindings

Key bindings are compatible with [NOAA rocoto_viewer.py](https://github.com/NOAA-EMC/global-workflow) for easy migration.

| Key | Action |
| --- | --- |
| `c` | Check task (rocotocheck) |
| `b` | Boot task (rocotoboot) |
| `r` | Rewind task (rocotorewind) |
| `R` | Run workflow (rocotorun) |
| `C` | Mark task complete |
| `W` | Rewind entire cycle |
| `→`/`←` | Next/Previous cycle |
| `x` | Expand/collapse metatask |
| `l` | Reload status data |
| `F` | Find last running cycle |
| `t` | Toggle Details/Log tabs |
| `f` | Toggle Log Follow mode |
| `h` | Help |
| `/` | Search log (vi-style) |
| `n`/`N` | Next/Previous search match |
| `q`/`Q` | Quit |

## License

Apache-2.0
