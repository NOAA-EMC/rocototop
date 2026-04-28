# .. note:: warning: "If you modify features, API, or usage, you MUST update the documentation immediately."

"""
Textual application for viewing Rocoto workflows.
"""

from __future__ import annotations

import asyncio
import logging
import os
import re
from collections import defaultdict
from datetime import datetime
from typing import Any

import aiofiles
from rich.markup import escape
from rich.table import Table
from rich.text import Text
from textual import on, work
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Container, Horizontal, Vertical
from textual.reactive import reactive
from textual.screen import ModalScreen
from textual.widgets import (
    DataTable,
    Footer,
    Header,
    Input,
    RichLog,
    Static,
    TabbedContent,
    TabPane,
    Tree,
)

from rocototop.parser import CycleStatus, RocotoParser, TaskStatus

logger = logging.getLogger(__name__)


class HelpScreen(ModalScreen):
    """A modal screen that displays help for the application."""

    BINDINGS = [
        Binding("escape", "dismiss", "Close"),
        Binding("q", "dismiss", "Close"),
        Binding("h", "dismiss", "Close"),
    ]

    def compose(self) -> ComposeResult:
        yield Vertical(
            Static("RocotoTop Help", id="help_title"),
            Static(id="help_content"),
            Static("Press ESC, q, or h to close", id="help_footer"),
            id="help_dialog",
        )

    def on_mount(self) -> None:
        table = Table(show_header=True, header_style="bold magenta", expand=True)
        table.add_column("Key", style="dim", width=10)
        table.add_column("Action")

        # Define key bindings to show in help
        bindings = [
            ("c", "Check Task (rocotocheck)"),
            ("b", "Boot Task (rocotoboot)"),
            ("r", "Rewind Task (rocotorewind)"),
            ("R", "Run Workflow (rocotorun)"),
            ("C", "Mark Task Complete"),
            ("W", "Rewind Entire Cycle"),
            ("right", "Next Cycle"),
            ("left", "Previous Cycle"),
            ("x", "Expand/Collapse"),
            ("l", "Reload Status Data"),
            ("F", "Find Last Running Cycle"),
            ("s", "Toggle Hide Succeeded"),
            ("E", "Expand All Cycles"),
            ("X", "Collapse All Cycles"),
            ("g", "Jump to Top of Tree"),
            ("G", "Jump to Bottom of Tree"),
            ("t", "Toggle Details/Log Tabs"),
            ("f", "Toggle Log Follow Mode"),
            ("/", "Search/Filter (vi-style)"),
            ("n/N", "Next/Prev Search Match"),
            ("h", "Show this Help"),
            ("q/Q", "Quit"),
        ]

        for key, action in bindings:
            table.add_row(key, action)

        self.query_one("#help_content", Static).update(table)


class RocotoApp(App[None]):
    """
    The main Textual application for RocotoTop.

    Parameters
    ----------
    workflow_file : str
        Path to the Rocoto workflow XML file.
    database_file : str
        Path to the Rocoto SQLite database file.
    refresh_interval : int, optional
        Refresh interval in seconds (default: 30).
    **kwargs : Any
        Additional keyword arguments passed to the Textual App constructor.

    Attributes
    ----------
    REFRESH_INTERVAL : int
        Default refresh interval.
    SIDEBAR_WIDTH : str
        Width of the sidebar.
    MAIN_CONTENT_WIDTH : str
        Width of the main content area.
    STATUS_TABLE_HEIGHT : str
        Height of the status table.
    MAX_LOG_READ_SIZE : int
        Maximum size of log file to read in bytes.
    parser : RocotoParser
        The workflow parser instance.
    refresh_interval : int
        The actual refresh interval being used.
    all_data : list[CycleStatus]
        The status data for all cycles and tasks.
    last_selected_task : dict[str, Any] | None
        Data of the last selected task.
    last_selected_cycle : str | None
        Name of the last selected cycle.
    log_follow : bool
        Whether to follow the log.
    current_log_file : str | None
        Path to the currently viewed log file.
    """

    REFRESH_INTERVAL = 30
    SIDEBAR_WIDTH = "25%"
    MAIN_CONTENT_WIDTH = "75%"
    STATUS_TABLE_HEIGHT = "15%"
    MAX_LOG_READ_SIZE = 1_000_000  # 1MB

    # Key bindings aligned with NOAA rocoto_viewer.py for easy migration:
    #   c=check, b=boot, r=rewind, R=run, Q=quit, arrows=cycle nav,
    #   l=reload, F=find running, x=expand/collapse, /=search
    BINDINGS = [
        Binding("q,Q", "quit", "Quit"),
        Binding("c", "check", "Check Task", show=True),
        Binding("b", "boot", "Boot Task", show=True),
        Binding("r", "rewind", "Rewind Task", show=True),
        Binding("R", "run", "Run (rocotorun)", show=True),
        Binding("W", "rewind_cycle", "Rewind Cycle", show=True),
        Binding("C", "complete", "Mark Complete", show=True),
        Binding("l", "reload", "Reload Data", show=True),
        Binding("right", "next_cycle", "Next Cycle", show=True),
        Binding("left", "prev_cycle", "Prev Cycle", show=True),
        Binding("F", "find_running", "Find Running", show=True),
        Binding("s", "toggle_succeeded", "Hide Succeeded", show=True),
        Binding("E", "expand_all", "Expand All", show=True),
        Binding("X", "collapse_all", "Collapse All", show=True),
        Binding("x", "toggle_expand", "Expand/Collapse", show=True),
        Binding("t", "toggle_log", "Toggle Log", show=True),
        Binding("f", "toggle_follow", "Follow Log", show=True),
        Binding("h", "help", "Help", show=True),
        Binding("slash", "open_search", "Search", show=True),
        Binding("n", "search_next", "Next Match", show=False),
        Binding("N", "search_prev", "Prev Match", show=False),
        Binding("escape", "close_log_search", "Close Search", show=False, priority=True),
        Binding("g", "top", "Top", show=False),
        Binding("G", "bottom", "Bottom", show=False),
    ]

    CSS = f"""
    Screen {{
        background: $surface;
    }}

    #sidebar {{
        width: {SIDEBAR_WIDTH};
        height: 100%;
        border-right: solid $primary;
    }}

    #main_content {{
        width: {MAIN_CONTENT_WIDTH};
        height: 100%;
    }}

    #filter_input {{
        margin: 1;
    }}

    #selected_task_status {{
        height: {STATUS_TABLE_HEIGHT};
        border-bottom: solid $primary;
    }}

    TabbedContent {{
        height: 85%;
    }}

    #details_panel {{
        padding: 1;
        background: $surface;
        overflow-y: scroll;
    }}

    #log_panel {{
        height: 1fr;
    }}

    #log_search_bar {{
        display: none;
        dock: bottom;
        height: auto;
        padding: 0 1;
        background: $boost;
    }}

    #log_search_bar.visible {{
        display: block;
    }}

    #log_search_input {{
        width: 1fr;
    }}

    #search_status {{
        height: 1;
        width: auto;
        min-width: 12;
        padding: 0 1;
        content-align: right middle;
    }}

    #status_bar {{
        height: 1;
        background: $primary;
        color: $text;
        padding-left: 1;
    }}

    HelpScreen {{
        align: center middle;
    }}

    #help_dialog {{
        padding: 1 2;
        background: $surface;
        border: thick $primary;
        width: 60;
        height: auto;
        max-height: 80%;
    }}

    #help_title {{
        content-align: center middle;
        text-style: bold;
        background: $primary;
        color: $text;
        margin-bottom: 1;
    }}

    #help_footer {{
        content-align: center middle;
        text-style: italic;
        margin-top: 1;
    }}

    .bold {{
        text-style: bold;
        color: $accent;
    }}
    """

    all_data: reactive[list[CycleStatus]] = reactive([])
    hide_succeeded: reactive[bool] = reactive(False)
    workflow_summary: reactive[dict[str, int]] = reactive({})
    last_refresh_time: reactive[datetime | None] = reactive(None)
    last_selected_task: reactive[dict[str, Any] | None] = reactive(None)
    last_selected_cycle: reactive[str | None] = reactive(None)

    def __init__(
        self,
        workflow_file: str,
        database_file: str,
        refresh_interval: int = 30,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.parser: RocotoParser = RocotoParser(workflow_file, database_file)
        self.refresh_interval = refresh_interval
        self.log_follow: bool = True
        self.current_log_file: str | None = None
        self._log_lines: list[str] = []
        self._search_query: str = ""
        self._search_matches: list[int] = []
        self._search_index: int = -1
        self._expanded_cycles: set[str] = set()
        self._sort_column: str = "Task"
        self._sort_reverse: bool = False

    def compose(self) -> ComposeResult:
        """
        Compose the UI layout.

        Returns
        -------
        ComposeResult
            The layout components.
        """
        yield Header(show_clock=True)
        with Horizontal():
            with Container(id="sidebar"):
                yield Tree("Cycles", id="cycle_tree")
            with Vertical(id="main_content"):
                yield Input(placeholder="Filter tasks by name...", id="filter_input")
                yield DataTable(id="selected_task_status", cursor_type="row")
                with TabbedContent():
                    with TabPane("Details", id="details_tab"):
                        yield Static("Select a task to see details", id="details_panel")
                    with TabPane("Log", id="log_tab"):
                        yield RichLog(id="log_panel", highlight=True, markup=False)
                        with Horizontal(id="log_search_bar"):
                            yield Input(placeholder="/search...", id="log_search_input")
                            yield Static("", id="search_status")
        yield Static("Path: Workflow", id="status_bar")
        yield Footer()

    def on_mount(self) -> None:
        """
        Handle application mount event.

        Starts the auto-refresh interval and performs an initial refresh.

        Returns
        -------
        None
        """
        self.set_interval(self.refresh_interval, self._auto_refresh)  # Auto-refresh
        self._update_status_bar()
        self.action_reload()

    def _auto_refresh(self) -> None:
        """
        Perform a lightweight background refresh.

        This is used for periodic background updates.
        """
        self._background_refresh(run_pulse=False)

    def action_reload(self) -> None:
        """
        Reload status data without running rocotorun.

        Triggered by the 'l' key. Matches rocoto_viewer's <l> behavior.
        """
        self._background_refresh(run_pulse=False)

    def action_run(self) -> None:
        """
        Run rocotorun and refresh data.

        Triggered by the 'R' key. Matches rocoto_viewer's <R> behavior.
        """
        self._background_refresh(run_pulse=True)

    @work(exclusive=True)
    async def _background_refresh(self, run_pulse: bool = False) -> None:
        """
        Perform background refresh of data.

        This worker parses the workflow XML and queries the database
        asynchronously.

        Parameters
        ----------
        run_pulse : bool, optional
            Whether to run rocotorun before refreshing data (default: False).

        Returns
        -------
        None
        """
        try:
            if run_pulse:
                self.notify("Starting pulse (rocotorun)...")
                await self._run_pulse()
                self.notify("Pulse (rocotorun) completed")

            # Parser methods are now async
            await self.parser.parse_workflow()
            data = await self.parser.get_status()
            summary = self.parser.get_summary(data)

            with self.batch_update():
                self.all_data = list(data)
                self.workflow_summary = summary
                self.last_refresh_time = datetime.now()

            if not run_pulse:
                self.notify("Data refresh completed")
        except Exception as e:
            self.notify(f"Error refreshing data: {e}", severity="error")

    async def _run_pulse(self) -> None:
        """
        Run rocotorun asynchronously.

        Returns
        -------
        None
        """
        cmd = [
            "rocotorun",
            "-w",
            self.parser.workflow_file,
            "-d",
            self.parser.database_file,
        ]
        try:
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            await process.communicate()
        except FileNotFoundError:
            self.notify(
                "rocotorun not found. Is Rocoto installed?",
                severity="warning",
            )
        except Exception as e:
            logger.error("Error running rocotorun: %s", e)

    def on_input_changed(self, event: Input.Changed) -> None:
        """
        Handle filter input changes.

        Parameters
        ----------
        event : Input.Changed
            The input change event.

        Returns
        -------
        None
        """
        self._update_ui()
        self._update_status_bar()

    def watch_hide_succeeded(self, hide: bool) -> None:
        """
        Watch for changes in hide_succeeded and update the UI.

        Parameters
        ----------
        hide : bool
            Whether to hide succeeded tasks.
        """
        self._update_ui()
        self._update_status_bar()

    def watch_workflow_summary(self, summary: dict[str, int]) -> None:
        """
        Watch for changes in workflow_summary and update the status bar.

        Parameters
        ----------
        summary : dict[str, int]
            The new summary.

        Returns
        -------
        None
        """
        self._update_status_bar()

    def watch_all_data(self, data: list[CycleStatus]) -> None:
        """
        Watch for changes in all_data and update the UI.

        Parameters
        ----------
        data : list[CycleStatus]
            The new data.

        Returns
        -------
        None
        """
        self._update_ui()
        self._update_status_bar()

    def watch_last_selected_task(self, task: dict[str, Any] | None) -> None:
        """
        Watch for changes in the last selected task and update details.

        Parameters
        ----------
        task : dict[str, Any] | None
            The newly selected task data.

        Returns
        -------
        None
        """
        if task and self.last_selected_cycle:
            self._display_details(task, self.last_selected_cycle)
        elif self.last_selected_cycle:
            self._display_cycle_details(self.last_selected_cycle)
        self._update_status_bar()

    def _display_cycle_details(self, cycle: str) -> None:
        """
        Display a summary for the selected cycle.

        Parameters
        ----------
        cycle : str
            The cycle string.
        """
        panel = self.query_one("#details_panel", Static)

        # Find cycle info
        tasks = []
        for ci in self.all_data:
            if ci["cycle"] == cycle:
                tasks = ci["tasks"]
                break

        if not tasks:
            panel.update(f"No tasks found for cycle {cycle}")
            return

        counts = defaultdict(int)
        for t in tasks:
            counts[t["state"]] += 1

        table = Table(title=f"Cycle Summary: {cycle}", show_header=True, header_style="bold cyan")
        table.add_column("State")
        table.add_column("Count", justify="right")

        states = ["SUCCEEDED", "RUNNING", "FAILED", "DEAD", "QUEUED", "WAITING"]
        for s in states:
            if counts[s] > 0:
                color = self._get_state_color(s)
                table.add_row(f"[{color}]{s}[/]", str(counts[s]))

        # Add any others
        for s, c in counts.items():
            if s not in states:
                table.add_row(s, str(c))

        panel.update(table)

    def watch_last_selected_cycle(self) -> None:
        """
        Watch for changes in the last selected cycle.

        Returns
        -------
        None
        """
        self._update_status_bar()

    def _update_status_bar(self) -> None:
        """
        Update the status bar with current summary and path.

        Returns
        -------
        None
        """
        try:
            status_bar = self.query_one("#status_bar", Static)
        except Exception:
            return

        summary = self.workflow_summary
        parts = []

        # Define priority states and their short names/colors
        states = [
            ("SUCCEEDED", "S", "green"),
            ("RUNNING", "R", "yellow"),
            ("FAILED", "F", "red"),
            ("DEAD", "D", "red"),
            ("QUEUED", "Q", "blue"),
            ("WAITING", "W", "white"),
        ]

        for state, short, color in states:
            count = summary.get(state, 0)
            if count > 0:
                parts.append(f"[{color}]{short}:{count}[/{color}]")

        summary_str = " | ".join(parts) if parts else "No tasks"

        # Show hide_succeeded status
        if self.hide_succeeded:
            summary_str += " [bold magenta](Hiding Succeeded)[/bold magenta]"

        # Update path
        path = "Path: Workflow"
        if self.last_selected_cycle:
            path += f" > {self.last_selected_cycle}"
            if self.last_selected_task:
                path += f" > {self.last_selected_task['task']}"

        update_time = ""
        if self.last_refresh_time:
            update_time = f" | Updated: {self.last_refresh_time.strftime('%H:%M:%S')}"

        status_bar.update(f"{path} | {summary_str}{update_time}")

    def _update_ui(self) -> None:
        """
        Update UI widgets with new data.

        Refreshes the cycle tree and updates details if a task is selected.

        Returns
        -------
        None
        """
        try:
            tree = self.query_one("#cycle_tree", Tree)
            filter_input = self.query_one("#filter_input", Input)
        except Exception:
            return

        with self.batch_update():
            filter_text = filter_input.value.lower()
            # To preserve expansion state, we'll track existing nodes
            existing_cycles = {str(node.label): node for node in tree.root.children}
            seen_cycles = set()

            for cycle_info in self.all_data:
                cycle_str = cycle_info["cycle"]

                # Pre-filter tasks to see if cycle should be shown
                visible_tasks = []
                for task in cycle_info["tasks"]:
                    if self.hide_succeeded and task["state"] == "SUCCEEDED":
                        continue
                    if not filter_text or filter_text in task["task"].lower():
                        visible_tasks.append(task)

                if not visible_tasks and (filter_text or self.hide_succeeded):
                    # Cycle should be hidden. If it exists, we skip it
                    # so that it gets removed in the cleanup loop.
                    continue

                seen_cycles.add(cycle_str)
                cycle_node = existing_cycles.get(cycle_str)

                # If cycle node doesn't exist, create it.
                if cycle_node is None:
                    is_expanded = cycle_str in self._expanded_cycles
                    cycle_node = tree.root.add(cycle_str, expand=is_expanded)

                # Lazy Loading: Only populate task nodes if the cycle is expanded
                # or if we are filtering (to show matches in collapsed cycles).
                if cycle_node.is_expanded or filter_text:
                    # Track existing task nodes in this cycle
                    existing_tasks = {node.data: node for node in cycle_node.children if node.data}
                    seen_tasks = set()

                    for task in visible_tasks:
                        task_name = task["task"]
                        seen_tasks.add(task_name)
                        state = task["state"]
                        icon = self._get_state_icon(state)
                        state_color = self._get_state_color(state)

                        # Highlight matching part of task name
                        display_name = escape(task_name)
                        if filter_text:
                            # Use regex for case-insensitive replacement to keep original case
                            try:
                                pattern = re.compile(re.escape(filter_text), re.IGNORECASE)
                                display_name = pattern.sub(lambda m: f"[reverse]{escape(m.group(0))}[/reverse]", task_name)
                            except re.error:
                                pass

                        leaf_label = f"{icon} {display_name} [{state_color}]{state}[/{state_color}]"

                        task_node = existing_tasks.get(task_name)
                        if task_node:
                            if str(task_node.label) != leaf_label:
                                task_node.set_label(leaf_label)
                        else:
                            task_node = cycle_node.add_leaf(leaf_label)
                            task_node.data = task_name

                    # Remove tasks that no longer exist or shouldn't be there
                    for tname, tnode in existing_tasks.items():
                        if tname not in seen_tasks:
                            tnode.remove()
                else:
                    # If collapsed and not filtering, remove all child nodes to save memory/DOM
                    if cycle_node.children:
                        cycle_node.remove_children()

            # Remove cycles that no longer exist
            for cstr, cnode in existing_cycles.items():
                if cstr not in seen_cycles:
                    cnode.remove()

            # Refresh cycle data and selected task status
            if self.last_selected_cycle:
                for cycle_info in self.all_data:
                    if cycle_info["cycle"] == self.last_selected_cycle:
                        # Refresh the table for all tasks in this cycle
                        self._update_task_table(cycle_info["tasks"])

                        # Refresh selected task if one exists
                        if self.last_selected_task:
                            for task in cycle_info["tasks"]:
                                if task["task"] == self.last_selected_task["task"]:
                                    resolved_task = task.copy()
                                    if "details" in task:
                                        resolved_task["details"] = self.parser.resolve_task_details(
                                            task["details"], self.last_selected_cycle
                                        )
                                    self.last_selected_task = resolved_task
                                    break
                        break

    def _get_state_icon(self, state: str) -> str:
        """
        Get icon for task state.

        Parameters
        ----------
        state : str
            The task state string.

        Returns
        -------
        str
            The icon emoji.
        """
        if state == "SUCCEEDED":
            return "✅"
        elif state == "RUNNING":
            return "🏃"
        elif state == "FAILED":
            return "❌"
        elif state == "DEAD":
            return "💀"
        elif state == "QUEUED":
            return "🕒"
        elif state in ["WAITING", "PENDING"]:
            return "⌛"
        return "❓"

    def _get_state_color(self, state: str) -> str:
        """
        Get color for task state.

        Parameters
        ----------
        state : str
            The task state string.

        Returns
        -------
        str
            The color name.
        """
        if state == "SUCCEEDED":
            return "green"
        elif state == "RUNNING":
            return "yellow"
        elif state == "FAILED":
            return "red"
        elif state == "DEAD":
            return "red"
        elif state == "QUEUED":
            return "blue"
        elif state in ["WAITING", "PENDING"]:
            return "white"
        return "white"

    def on_tree_node_expanded(self, event: Tree.NodeExpanded) -> None:
        """
        Handle tree node expansion to lazy-load children.

        Parameters
        ----------
        event : Tree.NodeExpanded
            The tree node expansion event.
        """
        node = event.node
        if node.allow_expand:
            self._expanded_cycles.add(str(node.label))
        self._update_ui()

    def on_tree_node_collapsed(self, event: Tree.NodeCollapsed) -> None:
        """
        Handle tree node collapse to free up resources.

        Parameters
        ----------
        event : Tree.NodeCollapsed
            The tree node collapse event.
        """
        node = event.node
        if node.allow_expand:
            self._expanded_cycles.discard(str(node.label))
        self._update_ui()

    def on_tree_node_selected(self, event: Tree.NodeSelected) -> None:
        """
        Handle tree node selection to expand or show details.

        Parameters
        ----------
        event : Tree.NodeSelected
            The tree node selection event.

        Returns
        -------
        None
        """
        node = event.node
        if node.is_root:
            return

        if node.allow_expand:
            node.expand()
            cycle_str = str(node.label)
            self.last_selected_cycle = cycle_str
            self.last_selected_task = None

            # Show all tasks for this cycle in the table
            for cycle_info in self.all_data:
                if cycle_info["cycle"] == cycle_str:
                    self._update_task_table(cycle_info["tasks"])
                    break

            self._update_status_bar()
        else:
            # Task leaf node
            task_name = node.data
            cycle_str = str(node.parent.label)

            # Find the task in all_data
            for cycle_info in self.all_data:
                if cycle_info["cycle"] == cycle_str:
                    for task in cycle_info["tasks"]:
                        if task["task"] == task_name:
                            # On-demand resolution of task details
                            resolved_task = task.copy()
                            if "details" in task:
                                resolved_task["details"] = self.parser.resolve_task_details(task["details"], cycle_str)

                            self.last_selected_task = resolved_task
                            self.last_selected_cycle = cycle_str
                            self._update_status_bar()

                            # Refresh the table and highlight the selected task
                            for ci in self.all_data:
                                if ci["cycle"] == cycle_str:
                                    self._update_task_table(ci["tasks"], highlight_task=task_name)
                                    break

                            self._display_details(resolved_task, cycle_str)
                            self._update_log()
                            break
                    break

    def on_data_table_header_selected(self, event: DataTable.HeaderSelected) -> None:
        """
        Handle header selection in the status table to sort tasks.

        Parameters
        ----------
        event : DataTable.HeaderSelected
            The data table header selection event.
        """
        column_label = str(event.column.label)
        if self._sort_column == column_label:
            self._sort_reverse = not self._sort_reverse
        else:
            self._sort_column = column_label
            self._sort_reverse = False

        # Refresh the table with new sorting
        if self.last_selected_cycle:
            for cycle_info in self.all_data:
                if cycle_info["cycle"] == self.last_selected_cycle:
                    self._update_task_table(
                        cycle_info["tasks"], highlight_task=self.last_selected_task["task"] if self.last_selected_task else None
                    )
                    break

    def on_data_table_row_selected(self, event: DataTable.RowSelected) -> None:
        """
        Handle row selection in the status table to select the task in the tree.

        Parameters
        ----------
        event : DataTable.RowSelected
            The data table row selection event.

        Returns
        -------
        None
        """
        table = self.query_one("#selected_task_status", DataTable)
        row = table.get_row_at(event.cursor_row)
        task_name_with_icon = str(row[1])

        # Find the task name (everything after the icon and space)
        parts = task_name_with_icon.split(" ", 1)
        if len(parts) < 2:
            return
        task_name = parts[1]

        # Find this task in the tree and select it
        tree = self.query_one("#cycle_tree", Tree)
        for cycle_node in tree.root.children:
            if str(cycle_node.label) == self.last_selected_cycle:
                cycle_node.expand()
                for task_node in cycle_node.children:
                    if task_node.data == task_name:
                        tree.select_node(task_node)
                        return

    def _update_task_table(self, tasks: list[TaskStatus], highlight_task: str | None = None) -> None:
        """
        Update the task status table with a list of tasks.

        Parameters
        ----------
        tasks : list[TaskStatus]
            The list of tasks to show.
        highlight_task : str | None
            The task name to highlight/select.
        """
        table = self.query_one("#selected_task_status", DataTable)
        if not table.columns:
            table.add_columns("Cycle", "Task", "Job ID", "State", "Exit", "Tries", "Duration")

        # Apply sorting
        def sort_key(t: TaskStatus) -> Any:
            col = self._sort_column
            if col == "Task":
                return t["task"]
            elif col == "Job ID":
                return t["jobid"] or ""
            elif col == "State":
                return t["state"]
            elif col == "Exit":
                return t["exit"] if t["exit"] is not None else -1
            elif col == "Tries":
                return t["tries"]
            elif col == "Duration":
                return t["duration"] or 0
            return t["task"]

        sorted_tasks = sorted(tasks, key=sort_key, reverse=self._sort_reverse)

        table.clear()
        target_row_idx = -1

        for i, task in enumerate(sorted_tasks):
            state = task["state"]
            icon = self._get_state_icon(state)
            state_color = self._get_state_color(state)

            table.add_row(
                self.last_selected_cycle,
                f"{icon} {task['task']}",
                str(task["jobid"] or "-"),
                f"[{state_color}]{state}[/{state_color}]",
                str(task["exit"] if task["exit"] is not None else "-"),
                str(task["tries"]),
                str(task["duration"] or "-"),
            )

            if highlight_task == task["task"]:
                target_row_idx = i

        if target_row_idx >= 0:
            table.move_cursor(row=target_row_idx)

    def _display_details(self, task: dict[str, Any], cycle: str) -> None:
        """
        Display task details.

        Parameters
        ----------
        task : dict[str, Any]
            The task data (expected to have resolved details).
        cycle : str
            The cycle string.

        Returns
        -------
        None
        """
        # Update details panel
        panel = self.query_one("#details_panel", Static)
        details = task.get("details", {})

        # Details are already resolved by the background parser worker
        command = details.get("command", "")
        stdout = details.get("stdout", "")
        stderr = details.get("stderr", "")
        join = details.get("join", "")

        exit_str = task["exit"] if task["exit"] is not None else "-"

        from rich.console import Group
        from rich.panel import Panel

        # Overview Table
        overview = Table.grid(padding=(0, 2))
        overview.add_column(style="bold cyan")
        overview.add_column()
        overview.add_column(style="bold cyan")
        overview.add_column()

        overview.add_row("Task:", task["task"], "Cycle:", cycle)
        state_color = self._get_state_color(task["state"])
        overview.add_row("State:", f"[{state_color}]{task['state']}[/]", "Job ID:", str(task["jobid"] or "-"))
        overview.add_row("Exit:", str(exit_str), "Tries:", str(task["tries"]))
        overview.add_row("Duration:", str(task["duration"] or "-"), "", "")

        renderables = [Panel(overview, title="Overview", border_style="blue")]

        # Command and Resources
        resources = Table.grid(padding=(0, 2))
        resources.add_column(style="bold cyan")
        resources.add_column()

        resources.add_row("Command:", command)
        if account := details.get("account"):
            resources.add_row("Account:", account)
        if queue := details.get("queue"):
            resources.add_row("Queue:", queue)
        if walltime := details.get("walltime"):
            resources.add_row("Walltime:", walltime)
        if memory := details.get("memory"):
            resources.add_row("Memory:", memory)

        if join:
            resources.add_row("Log:", join)
        else:
            if stdout:
                resources.add_row("Stdout:", stdout)
            if stderr:
                resources.add_row("Stderr:", stderr)

        renderables.append(Panel(resources, title="Execution Details", border_style="green"))

        # Environment Variables
        if envars := details.get("envars"):
            env_table = Table(show_header=True, header_style="bold magenta", expand=True)
            env_table.add_column("Variable")
            env_table.add_column("Value")
            for k, v in sorted(envars.items()):
                env_table.add_row(k, v)
            renderables.append(Panel(env_table, title="Environment Variables", border_style="magenta"))

        # Dependencies
        if deps := details.get("dependencies"):
            renderables.append(Panel(self._format_deps(deps), title="Dependencies", border_style="yellow"))

        panel.update(Group(*renderables))

    def _format_deps(self, deps: list[dict[str, Any]], indent: int = 0) -> str:
        """
        Format dependency list into readable indented text.

        Parameters
        ----------
        deps : list[dict[str, Any]]
            The dependency list from the parser.
        indent : int
            Current indentation level in spaces.

        Returns
        -------
        str
            Formatted dependency string.
        """
        prefix = " " * indent
        lines = ""
        for dep in deps:
            dep_type = dep["type"]
            attrib = dep.get("attrib", {})
            text = dep.get("text", "")

            if dep_type in ["and", "or", "not", "nand", "nor", "xor", "some"]:
                lines += f"{prefix}- [{dep_type.upper()}]\n"
                children = dep.get("children", [])
                lines += self._format_deps(children, indent + 4)
            else:
                # Format attributes as key=value pairs
                attr_parts = [f"{k}={v}" for k, v in attrib.items()]
                attr_str = ", ".join(attr_parts) if attr_parts else ""
                parts = [dep_type]
                if attr_str:
                    parts.append(attr_str)
                if text:
                    parts.append(text)
                lines += f"{prefix}- {' '.join(parts)}\n"
        return lines

    def action_boot(self) -> None:
        """
        Execute rocotoboot for the selected task.

        Triggered by the 'b' key. Matches rocoto_viewer's <b> behavior.

        Returns
        -------
        None
        """
        self._run_rocoto_command("rocotoboot")

    def action_rewind(self) -> None:
        """
        Execute rocotorewind for the selected task.

        Triggered by the 'r' key. Matches rocoto_viewer's <r> behavior.

        Returns
        -------
        None
        """
        self._run_rocoto_command("rocotorewind")

    def action_check(self) -> None:
        """
        Execute rocotocheck for the selected task and display results.

        Triggered by the 'c' key. Matches rocoto_viewer's <c> behavior.

        Returns
        -------
        None
        """
        self._run_rocoto_command("rocotocheck")

    def action_complete(self) -> None:
        """
        Execute rocotocomplete for the selected task.

        Triggered by the 'C' (shift-c) key.

        Returns
        -------
        None
        """
        self._run_rocoto_command("rocotocomplete")

    def action_next_cycle(self) -> None:
        """
        Navigate to the next cycle in the tree.

        Triggered by the right arrow key. Matches rocoto_viewer's (->) behavior.
        """
        tree = self.query_one("#cycle_tree", Tree)
        cycle_nodes = list(tree.root.children)
        if not cycle_nodes:
            return

        current_idx = -1
        if self.last_selected_cycle:
            for i, node in enumerate(cycle_nodes):
                if str(node.label) == self.last_selected_cycle:
                    current_idx = i
                    break

        next_idx = current_idx + 1
        if next_idx < len(cycle_nodes):
            target = cycle_nodes[next_idx]
            target.expand()
            tree.select_node(target)
            self.last_selected_cycle = str(target.label)
            self.last_selected_task = None
            self._update_status_bar()

    def action_prev_cycle(self) -> None:
        """
        Navigate to the previous cycle in the tree.

        Triggered by the left arrow key. Matches rocoto_viewer's (<-) behavior.
        """
        tree = self.query_one("#cycle_tree", Tree)
        cycle_nodes = list(tree.root.children)
        if not cycle_nodes:
            return

        current_idx = len(cycle_nodes)
        if self.last_selected_cycle:
            for i, node in enumerate(cycle_nodes):
                if str(node.label) == self.last_selected_cycle:
                    current_idx = i
                    break

        prev_idx = current_idx - 1
        if prev_idx >= 0:
            target = cycle_nodes[prev_idx]
            target.expand()
            tree.select_node(target)
            self.last_selected_cycle = str(target.label)
            self.last_selected_task = None
            self._update_status_bar()

    def action_find_running(self) -> None:
        """
        Jump to the last cycle that has a RUNNING task.

        Triggered by the 'F' key. Matches rocoto_viewer's <F> behavior.
        """
        tree = self.query_one("#cycle_tree", Tree)
        cycle_nodes = list(tree.root.children)

        target_cycle = None
        for cycle_info in reversed(self.all_data):
            for task in cycle_info["tasks"]:
                if task["state"] == "RUNNING":
                    target_cycle = cycle_info["cycle"]
                    break
            if target_cycle:
                break

        if not target_cycle:
            self.notify("No running tasks found", severity="warning")
            return

        for node in cycle_nodes:
            if str(node.label) == target_cycle:
                node.expand()
                tree.select_node(node)
                self.last_selected_cycle = target_cycle
                self.last_selected_task = None
                self._update_status_bar()
                self.notify(f"Jumped to cycle {target_cycle}")
                return

    def action_toggle_expand(self) -> None:
        """
        Toggle expand/collapse of the currently selected tree node.

        Triggered by the 'x' key. Matches rocoto_viewer's <x> behavior.
        """
        tree = self.query_one("#cycle_tree", Tree)
        node = tree.cursor_node
        if node and node.allow_expand:
            node.toggle()

    def action_expand_all(self) -> None:
        """Expand all cycles in the tree."""
        tree = self.query_one("#cycle_tree", Tree)
        for node in tree.root.children:
            node.expand()
            self._expanded_cycles.add(str(node.label))
        self._update_ui()

    def action_collapse_all(self) -> None:
        """Collapse all cycles in the tree."""
        tree = self.query_one("#cycle_tree", Tree)
        for node in tree.root.children:
            node.collapse()
            self._expanded_cycles.discard(str(node.label))
        self._update_ui()

    def action_toggle_succeeded(self) -> None:
        """Toggle visibility of succeeded tasks."""
        self.hide_succeeded = not self.hide_succeeded
        self.notify(f"Hide succeeded: {'ON' if self.hide_succeeded else 'OFF'}")

    def action_top(self) -> None:
        """Jump to the top of the cycle tree (vi-style g)."""
        tree = self.query_one("#cycle_tree", Tree)
        tree.select_node(tree.root)
        tree.scroll_to_node(tree.root)

    def action_bottom(self) -> None:
        """Jump to the bottom of the cycle tree (vi-style G)."""
        tree = self.query_one("#cycle_tree", Tree)
        last_node = tree.root
        while last_node.children:
            last_node = last_node.children[-1]
            if not last_node.is_expanded:
                break
        tree.select_node(last_node)
        tree.scroll_to_node(last_node)

    def action_help(self) -> None:
        """
        Display a help screen with key bindings.

        Triggered by the 'h' key. Matches rocoto_viewer's <h> behavior.
        """
        self.push_screen(HelpScreen())

    def action_rewind_cycle(self) -> None:
        """
        Execute rocotorewind for every task in the selected cycle.

        Triggered by the 'W' key. Requires a cycle to be selected.
        """
        if not self.last_selected_cycle:
            self.notify("No cycle selected", severity="warning")
            return

        # Find all tasks for the selected cycle
        tasks: list[str] = []
        for cycle_info in self.all_data:
            if cycle_info["cycle"] == self.last_selected_cycle:
                tasks = [t["task"] for t in cycle_info["tasks"]]
                break

        if not tasks:
            self.notify("No tasks found in selected cycle", severity="warning")
            return

        self._rewind_cycle_tasks(self.last_selected_cycle, tasks)

    @work
    async def _rewind_cycle_tasks(self, cycle: str, tasks: list[str]) -> None:
        """
        Run rocotorewind for each task in a cycle asynchronously.

        Parameters
        ----------
        cycle : str
            The cycle string.
        tasks : list[str]
            List of task names to rewind.
        """
        self.notify(f"Rewinding {len(tasks)} tasks in cycle {cycle}...")
        succeeded = 0
        failed = 0

        # We can run these in parallel using asyncio.gather for better performance
        async def run_rewind(task_name: str) -> bool:
            cmd = [
                "rocotorewind",
                "-w",
                self.parser.workflow_file,
                "-d",
                self.parser.database_file,
                "-c",
                cycle,
                "-t",
                task_name,
            ]
            try:
                process = await asyncio.create_subprocess_exec(
                    *cmd,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                )
                _, stderr = await process.communicate()
                if process.returncode == 0:
                    return True
                else:
                    logger.warning("Failed to rewind %s: %s", task_name, stderr.decode().strip())
                    return False
            except FileNotFoundError:
                return False
            except Exception as e:
                logger.error("Error rewinding %s: %s", task_name, e)
                return False

        results = await asyncio.gather(*(run_rewind(t) for t in tasks))
        succeeded = sum(1 for r in results if r)
        failed = len(tasks) - succeeded

        msg = f"Cycle rewind complete: {succeeded} succeeded"
        if failed:
            msg += f", {failed} failed"
            self.notify(msg, severity="warning")
        else:
            self.notify(msg)

    @work
    async def _run_rocoto_command(self, command: str) -> None:
        """
        Run a Rocoto CLI command for the selected task and cycle.

        Parameters
        ----------
        command : str
            The name of the Rocoto command to run.

        Returns
        -------
        None
        """
        if not self.last_selected_task or not self.last_selected_cycle:
            self.notify("No task selected", severity="warning")
            return

        task_name = self.last_selected_task["task"]
        cycle = self.last_selected_cycle

        cmd = [
            command,
            "-w",
            self.parser.workflow_file,
            "-d",
            self.parser.database_file,
            "-c",
            cycle,
            "-t",
            task_name,
        ]

        try:
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            _, stderr = await process.communicate()
            if process.returncode == 0:
                self.notify(f"Successfully executed {command} for {task_name}")
            else:
                error_msg = stderr.decode().strip() or f"Return code {process.returncode}"
                self.notify(f"Failed to execute {command}: {error_msg}", severity="error")
        except FileNotFoundError:
            self.notify(f"Command not found: {command}. Is Rocoto installed?", severity="error")
        except Exception as e:
            self.notify(f"Error executing {command}: {e}", severity="error")

    def action_toggle_log(self) -> None:
        """
        Toggle between Details and Log tabs.

        Returns
        -------
        None
        """
        tabbed_content = self.query_one(TabbedContent)
        if tabbed_content.active == "log_tab":
            tabbed_content.active = "details_tab"
        else:
            tabbed_content.active = "log_tab"

    def action_toggle_follow(self) -> None:
        """
        Toggle log follow mode.

        Returns
        -------
        None
        """
        self.log_follow = not self.log_follow
        self.notify(f"Log follow: {'ON' if self.log_follow else 'OFF'}")

    def action_open_search(self) -> None:
        """
        Open search based on active tab (vi-style /).

        If in Log tab, opens log search.
        Otherwise, focuses the task filter input.
        """
        tabbed_content = self.query_one(TabbedContent)
        if tabbed_content.active == "log_tab":
            bar = self.query_one("#log_search_bar")
            bar.add_class("visible")
            search_input = self.query_one("#log_search_input", Input)
            search_input.value = ""
            search_input.focus()
        else:
            filter_input = self.query_one("#filter_input", Input)
            filter_input.focus()
            filter_input.select_all()

    def action_close_log_search(self) -> None:
        """Close the log search bar and clear highlights."""
        bar = self.query_one("#log_search_bar")
        bar.remove_class("visible")
        self._search_query = ""
        self._search_matches = []
        self._search_index = -1
        self.query_one("#search_status", Static).update("")
        self._redraw_log()

    @on(Input.Submitted, "#log_search_input")
    def _on_search_submitted(self, event: Input.Submitted) -> None:
        """Execute search when Enter is pressed in the search input."""
        query = event.value.strip()
        if not query:
            return
        self._run_log_search(query)

    def _run_log_search(self, query: str) -> None:
        """
        Search log lines for the given query and jump to the first match.

        Parameters
        ----------
        query : str
            The search string (treated as a case-insensitive regex).
        """
        self._search_query = query
        try:
            pattern = re.compile(query, re.IGNORECASE)
        except re.error:
            self.notify(f"Invalid regex: {query}", severity="error")
            return

        self._search_matches = [i for i, line in enumerate(self._log_lines) if pattern.search(line)]

        if not self._search_matches:
            self.query_one("#search_status", Static).update("No matches")
            self._search_index = -1
            self._redraw_log()
            return

        self._search_index = 0
        self._jump_to_match()

    def action_search_next(self) -> None:
        """Jump to the next search match (vi-style n)."""
        if not self._search_matches:
            return
        self._search_index = (self._search_index + 1) % len(self._search_matches)
        self._jump_to_match()

    def action_search_prev(self) -> None:
        """Jump to the previous search match (vi-style N)."""
        if not self._search_matches:
            return
        self._search_index = (self._search_index - 1) % len(self._search_matches)
        self._jump_to_match()

    def _jump_to_match(self) -> None:
        """Scroll the log panel to the current search match and highlight it."""
        if self._search_index < 0 or not self._search_matches:
            return

        match_line = self._search_matches[self._search_index]
        total = len(self._search_matches)
        self.query_one("#search_status", Static).update(f"{self._search_index + 1}/{total}")

        self._redraw_log(highlight_line=match_line)

        log_panel = self.query_one("#log_panel", RichLog)
        log_panel.scroll_to(y=match_line, animate=False)

    def _redraw_log(self, highlight_line: int = -1) -> None:
        """
        Redraw the log panel, optionally highlighting a specific line.

        Parameters
        ----------
        highlight_line : int
            Line index to highlight, or -1 for no highlight.
        """
        log_panel = self.query_one("#log_panel", RichLog)
        log_panel.clear()

        try:
            # Use raw query for regex support, same as _run_log_search
            pattern = re.compile(self._search_query, re.IGNORECASE) if self._search_query else None
        except re.error:
            pattern = None

        for i, line in enumerate(self._log_lines):
            text = Text(line)
            if i == highlight_line:
                text.stylize("black on yellow")
            elif pattern:
                for match in pattern.finditer(line):
                    text.stylize("black on cyan", match.start(), match.end())

            log_panel.write(text)

    def _update_log(self) -> None:
        """
        Initialize log reading for the selected task.

        Returns
        -------
        None
        """
        if not self.last_selected_task or not self.last_selected_cycle:
            return

        log_panel = self.query_one("#log_panel", RichLog)
        log_panel.clear()
        self._log_lines = []
        self._search_query = ""
        self._search_matches = []
        self._search_index = -1
        self.query_one("#search_status", Static).update("")

        details = self.last_selected_task.get("details", {})
        # Log file path is already resolved in details
        log_file = details.get("join") or details.get("stdout") or ""

        if not log_file:
            log_panel.write("No log file defined for this task.")
            self.current_log_file = None
            return

        self.current_log_file = log_file
        self.tail_log(log_file)

    @work(exclusive=True)
    async def tail_log(self, log_file: str) -> None:
        """
        Tail the log file asynchronously.

        Parameters
        ----------
        log_file : str
            The path to the log file.

        Returns
        -------
        None
        """
        log_panel = self.query_one("#log_panel", RichLog)
        try:
            if not await asyncio.to_thread(os.path.exists, log_file):
                log_panel.write(f"Log file not found: {log_file}")
                return

            size = await asyncio.to_thread(os.path.getsize, log_file)
            async with aiofiles.open(log_file, encoding="utf-8", errors="replace") as f:
                if size > self.MAX_LOG_READ_SIZE:
                    await f.seek(size - self.MAX_LOG_READ_SIZE)
                    # Skip the first partial line if we seeked
                    await f.readline()
                    truncation_msg = f"--- Log truncated. Showing last {self.MAX_LOG_READ_SIZE // 1024}KB ---"
                    log_panel.write(truncation_msg)
                    self._log_lines.append(truncation_msg)

                content = await f.read()
                log_panel.write(content)
                self._log_lines.extend(content.splitlines())

                while self.current_log_file == log_file and self.is_running:
                    line = await f.readline()
                    if line:
                        stripped = line.rstrip()
                        log_panel.write(stripped)
                        self._log_lines.append(stripped)
                        if self.log_follow:
                            log_panel.scroll_end()
                    else:
                        await asyncio.sleep(0.1)
        except Exception as e:
            if self.is_running:
                self.notify(f"Error reading log: {e}", severity="error")


if __name__ == "__main__":
    from rocototop.cli import main

    main()
