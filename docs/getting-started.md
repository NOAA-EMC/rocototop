# Getting Started

This guide will help you get RocotoTop up and running on your system.

## Prerequisites

Before installing RocotoTop, ensure you have the following:

- **Python**: Version 3.9 or higher.
- **Rocoto**: You should have a Rocoto workflow XML file and its corresponding SQLite database.

## Installation

### From Source

1.  **Clone the repository**:
    ```bash
    git clone https://github.com/rocototop/rocototop.git
    cd rocototop
    ```

2.  **Install the package**:
    It is recommended to use a virtual environment.
    ```bash
    pip install .
    ```

## Running RocotoTop

To start RocotoTop, you need to provide the path to your workflow XML file and the SQLite database file:

```bash
rocototop -w my_workflow.xml -d my_database.db
```

### Command Line Arguments

- `-w`, `--workflow`: Path to the Rocoto XML workflow file (required).
- `-d`, `--database`: Path to the Rocoto SQLite database file (required).

Once launched, the TUI will open, and you can start monitoring your workflow!
