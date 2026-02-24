# User Guide

This guide provides a detailed walkthrough of the RocotoViewer interface and its features.

![RocotoViewer Interface](screenshots/collapsed.svg)

## Interface Overview

RocotoViewer's interface is divided into three main sections:

1.  **Sidebar (Cycle Tree)**: Displays a hierarchical view of workflow cycles and their tasks. Each cycle can be expanded to see its tasks with color-coded status.
2.  **Status Table**: A central table showing all tasks for the selected cycle(s). It includes columns for Task name, Job ID, State, Exit code, Tries, and Duration.
3.  **Details Panel**: Displays comprehensive information about the currently selected task, including command, log paths, and dependencies.
4.  **Log Panel**: An optional panel (toggled with `l`) that shows a live tail of the selected task's log file.
5.  **Status Bar**: Displays the "Path" to the currently selected item (e.g., Workflow > Cycle > Task).

## Key Bindings

| Key | Action | Description |
| :--- | :--- | :--- |
| `q` | Quit | Exit the application. |
| `r` | Refresh | Manually trigger a data refresh from the XML and database. |
| `b` | Boot | Execute `rocotoboot` for the selected task (Placeholder). |
| `w` | Rewind | Execute `rocotorewind` for the selected task (Placeholder). |
| `c` | Complete | Mark the selected task as complete (Placeholder). |
| `l` | Toggle Log | Show or hide the log tailing panel. |
| `f` | Follow Log | Toggle automatic scrolling to the bottom of the log (Follow mode). |

## Task Filtering

You can quickly find specific tasks using the filter input at the top of the main content area. Type any part of a task name to filter the visible rows in the Status Table.

![Task Filtering](screenshots/filtering.svg)

## Auto-Refresh

By default, RocotoViewer automatically refreshes the workflow status every 30 seconds. This ensures you have the latest information without manual intervention.

## Inspecting Task Details

When you select a row in the Status Table, the Details Panel at the bottom updates with specific information for that task instance. This includes:

- **Resolved Paths**: Log paths and commands are automatically resolved based on the cycle.
- **Resource Usage**: Account, Queue, Walltime, and Memory requirements if specified.
- **Dependencies**: A list of task dependencies and their current status.

## Log Viewing

RocotoViewer allows you to view task logs directly within the TUI. When a task is selected, you can press `l` to toggle the log panel. If the log file exists, it will be loaded and tailed in real-time.

![Log Viewer](screenshots/details_log.svg)

By default, **Follow mode** is enabled, meaning the view will automatically scroll as new content is added to the log. You can toggle this behavior by pressing `f`.
