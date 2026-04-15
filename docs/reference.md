# Reference

## Command Line Interface

### `rocototop`

The primary command to launch the TUI.

**Options:**

- `-w, --workflow FILE`: **(Required)** Path to the Rocoto XML workflow definition.
- `-d, --database FILE`: **(Required)** Path to the Rocoto SQLite database file.
- `-h, --help`: Show the help message and exit.

## Architecture Overview

RocotoTop is built with a separation of concerns:

- **`cli.py`**: Handles argument parsing and application entry.
- **`app.py`**: Contains the Textual application logic, widget definitions, and event handling.
- **`parser.py`**: Manages the logic for parsing Rocoto XML, querying the SQLite database, and resolving cycle strings.

## API Documentation

Below is the automatically generated documentation for the core classes.

::: rocototop.app.RocotoApp
::: rocototop.parser.RocotoParser
