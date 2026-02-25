"""
.. note:: warning: "If you modify features, API, or usage, you MUST update the documentation immediately."
"""

from __future__ import annotations

import os
import subprocess
import time
from typing import Any

from textual import work
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Container, Horizontal, Vertical
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

from rocotoviewer.parser import RocotoParser


class RocotoApp(App[None]):
    """
    The main Textual application for RocotoViewer.

    Parameters
    ----------
    workflow_file : str
        Path to the Rocoto workflow XML file.
    database_file : str
        Path to the Rocoto SQLite database file.
    **kwargs : Any
        Additional keyword arguments passed to the Textual App constructor.
    """

    REFRESH_INTERVAL = 30
    SIDEBAR_WIDTH = "25%"
    MAIN_CONTENT_WIDTH = "75%"
    STATUS_TABLE_HEIGHT = "15%"
    MAX_LOG_READ_SIZE = 100_000  # 100KB

    BINDINGS = [
        Binding("q", "quit", "Quit"),
        Binding("r", "refresh", "Refresh"),
        Binding("b", "boot", "Boot Task", show=True),
        Binding("w", "rewind", "Rewind Task", show=True),
        Binding("c", "complete", "Mark Complete", show=True),
        Binding("l", "toggle_log", "Toggle Log", show=True),
        Binding("f", "toggle_follow", "Follow Log", show=True),
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

    #status_bar {{
        height: 1;
        background: $primary;
        color: $text;
        padding-left: 1;
    }}

    .bold {{
        text-style: bold;
        color: $accent;
    }}
    """

    def __init__(self, workflow_file: str, database_file: str, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.parser: RocotoParser = RocotoParser(workflow_file, database_file)
        self.all_data: list[dict[str, Any]] = []
        self.last_selected_task: dict[str, Any] | None = None
        self.last_selected_cycle: str | None = None
        self.log_follow: bool = True
        self.current_log_file: str | None = None

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
        self.set_interval(self.REFRESH_INTERVAL, self.action_refresh)  # Auto-refresh
        self.action_refresh()

    @work(thread=True)
    def action_refresh(self) -> None:
        """
        Perform background refresh of data.

        This worker parses the workflow XML and queries the database
        in a separate thread to avoid blocking the UI.

        Returns
        -------
        None
        """
        try:
            # We don't notify on auto-refresh to avoid being annoying
            # self.call_from_thread(self.notify, "Refreshing data...")
            self.parser.parse_workflow()
            self.all_data = self.parser.get_status()
            self.call_from_thread(self._update_ui)
        except Exception as e:
            self.call_from_thread(self.notify, f"Error refreshing data: {e}", severity="error")

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

    def _update_ui(self) -> None:
        """
        Update UI widgets with new data.

        Refreshes the cycle tree and updates details if a task is selected.

        Returns
        -------
        None
        """
        with self.batch_update():
            filter_text = self.query_one("#filter_input", Input).value.lower()

            tree = self.query_one("#cycle_tree", Tree)
            # To preserve expansion state, we'll track existing nodes
            existing_cycles = {str(node.label): node for node in tree.root.children}

            for cycle_info in self.all_data:
                cycle_str = cycle_info["cycle"]
                cycle_node = existing_cycles.get(cycle_str)

                # If cycle node doesn't exist, create it.
                if cycle_node is None:
                    cycle_node = tree.root.add(cycle_str, expand=False)

                # Track existing task nodes in this cycle
                existing_tasks = {node.data: node for node in cycle_node.children if node.data}
                seen_tasks = set()

                for task in cycle_info["tasks"]:
                    task_name = task["task"]
                    if filter_text and filter_text not in task_name.lower():
                        continue

                    seen_tasks.add(task_name)
                    state = task["state"]
                    icon = self._get_state_icon(state)
                    state_color = self._get_state_color(state)

                    leaf_label = f"{icon} {task_name} [{state_color}]{state}[/{state_color}]"

                    task_node = existing_tasks.get(task_name)
                    if task_node:
                        if str(task_node.label) != leaf_label:
                            task_node.set_label(leaf_label)
                    else:
                        task_node = cycle_node.add_leaf(leaf_label)
                        task_node.data = task_name

                # Remove tasks that no longer exist or are filtered out
                for tname, tnode in existing_tasks.items():
                    if tname not in seen_tasks:
                        tnode.remove()

            # Refresh selected task status if one is selected
            if self.last_selected_task and self.last_selected_cycle:
                # Find the updated task data
                for cycle_info in self.all_data:
                    if cycle_info["cycle"] == self.last_selected_cycle:
                        for task in cycle_info["tasks"]:
                            if task["task"] == self.last_selected_task["task"]:
                                self.last_selected_task = task
                                self._display_details(task, self.last_selected_cycle)
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
            self.query_one("#status_bar", Static).update(f"Path: Workflow > {node.label}")
        else:
            # Task leaf node
            task_name = node.data
            cycle_str = str(node.parent.label)

            # Find the task in all_data
            for cycle_info in self.all_data:
                if cycle_info["cycle"] == cycle_str:
                    for task in cycle_info["tasks"]:
                        if task["task"] == task_name:
                            self.last_selected_task = task
                            self.last_selected_cycle = cycle_str
                            self.query_one("#status_bar", Static).update(f"Path: Workflow > {cycle_str} > {task_name}")
                            self._display_details(task, cycle_str)
                            self._update_log()
                            break
                    break

    def on_data_table_row_selected(self, event: DataTable.RowSelected) -> None:
        """
        Handle row selection in the status table.

        Parameters
        ----------
        event : DataTable.RowSelected
            The data table row selection event.

        Returns
        -------
        None
        """
        # In this version, the table only has one row for the selected task,
        # so selecting it doesn't change much, but we'll keep the handler.
        pass

    def _display_details(self, task: dict[str, Any], cycle: str) -> None:
        """
        Display task details and update simplified status.

        Parameters
        ----------
        task : dict[str, Any]
            The task data.
        cycle : str
            The cycle string.

        Returns
        -------
        None
        """
        # Update simplified status table
        table = self.query_one("#selected_task_status", DataTable)
        if not table.columns:
            table.add_columns("Cycle", "Task", "Job ID", "State", "Exit", "Tries", "Duration")
        table.clear()

        state = task["state"]
        icon = self._get_state_icon(state)
        state_color = self._get_state_color(state)

        table.add_row(
            cycle,
            f"{icon} {task['task']}",
            str(task["jobid"] or "-"),
            f"[{state_color}]{state}[/{state_color}]",
            str(task["exit"] if task["exit"] is not None else "-"),
            str(task["tries"]),
            str(task["duration"] or "-"),
        )

        # Update details panel
        panel = self.query_one("#details_panel", Static)
        details = task.get("details", {})

        command = self.parser.resolve_cyclestr(details.get("command", ""), cycle)
        stdout = self.parser.resolve_cyclestr(details.get("stdout", ""), cycle)
        stderr = self.parser.resolve_cyclestr(details.get("stderr", ""), cycle)
        join = self.parser.resolve_cyclestr(details.get("join", ""), cycle)

        exit_str = task["exit"] if task["exit"] is not None else "-"
        content = f"[bold]Task:[/bold] {task['task']}  [bold]Cycle:[/bold] {cycle}\n"
        content += f"[bold]State:[/bold] {task['state']}  [bold]Job ID:[/bold] {task['jobid'] or '-'}\n"
        content += f"[bold]Exit Status:[/bold] {exit_str}  [bold]Tries:[/bold] {task['tries']}\n"
        content += f"[bold]Duration:[/bold] {task['duration'] or '-'}\n"
        content += "-" * 40 + "\n"
        content += f"[bold]Command:[/bold] {command}\n"
        if account := details.get("account"):
            content += f"[bold]Account:[/bold] {account}\n"
        if queue := details.get("queue"):
            content += f"[bold]Queue:[/bold] {queue}\n"
        if walltime := details.get("walltime"):
            content += f"[bold]Walltime:[/bold] {walltime}\n"
        if memory := details.get("memory"):
            content += f"[bold]Memory:[/bold] {memory}\n"

        if join:
            content += f"[bold]Log (Joined):[/bold] {join}\n"
        else:
            if stdout:
                content += f"[bold]Stdout:[/bold] {stdout}\n"
            if stderr:
                content += f"[bold]Stderr:[/bold] {stderr}\n"

        if deps := details.get("dependencies"):
            content += "[bold]Dependencies:[/bold]\n"
            for dep in deps:
                content += f"  - {dep['type']}: {dep.get('attrib', {})} {dep.get('text', '')}\n"

        panel.update(content)

    def action_boot(self) -> None:
        """
        Execute rocotoboot for the selected task.

        Returns
        -------
        None
        """
        self._run_rocoto_command("rocotoboot")

    def action_rewind(self) -> None:
        """
        Execute rocotorewind for the selected task.

        Returns
        -------
        None
        """
        self._run_rocoto_command("rocotorewind")

    def action_complete(self) -> None:
        """
        Execute rocotocomplete for the selected task.

        Returns
        -------
        None
        """
        self._run_rocoto_command("rocotocomplete")

    @work(thread=True)
    def _run_rocoto_command(self, command: str) -> None:
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
            self.call_from_thread(self.notify, "No task selected", severity="warning")
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
            result = subprocess.run(cmd, capture_output=True, text=True, check=False)
            if result.returncode == 0:
                self.call_from_thread(self.notify, f"Successfully executed {command} for {task_name}")
            else:
                error_msg = result.stderr.strip() or f"Return code {result.returncode}"
                self.call_from_thread(self.notify, f"Failed to execute {command}: {error_msg}", severity="error")
        except FileNotFoundError:
            self.call_from_thread(self.notify, f"Command not found: {command}. Is Rocoto installed?", severity="error")
        except Exception as e:
            self.call_from_thread(self.notify, f"Error executing {command}: {e}", severity="error")

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

        details = self.last_selected_task.get("details", {})
        log_file = self.parser.resolve_cyclestr(details.get("join") or details.get("stdout") or "", self.last_selected_cycle)

        if not log_file:
            log_panel.write("No log file defined for this task.")
            self.current_log_file = None
            return

        if not os.path.exists(log_file):
            log_panel.write(f"Log file not found: {log_file}")
            self.current_log_file = None
            return

        self.current_log_file = log_file
        self.tail_log(log_file)

    @work(thread=True, exclusive=True)
    def tail_log(self, log_file: str) -> None:
        """
        Tail the log file in a background thread.

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
            size = os.path.getsize(log_file)
            with open(log_file, encoding="utf-8", errors="replace") as f:
                if size > self.MAX_LOG_READ_SIZE:
                    f.seek(size - self.MAX_LOG_READ_SIZE)
                    # Skip the first partial line if we seeked
                    f.readline()
                    self.call_from_thread(
                        log_panel.write,
                        f"--- Log truncated. Showing last {self.MAX_LOG_READ_SIZE // 1024}KB ---",
                    )

                content = f.read()
                self.call_from_thread(log_panel.write, content)

                while self.current_log_file == log_file and self.is_running:
                    line = f.readline()
                    if line:
                        self.call_from_thread(log_panel.write, line.rstrip())
                        if self.log_follow:
                            self.call_from_thread(log_panel.scroll_end)
                    else:
                        time.sleep(0.1)
        except Exception as e:
            if self.is_running:
                self.call_from_thread(self.notify, f"Error reading log: {e}", severity="error")


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 3:
        print("Usage: rocotoviewer -w <workflow.xml> -d <database.db>")
        sys.exit(1)
    app = RocotoApp(sys.argv[1], sys.argv[2])
    app.run()
