"""
.. note:: warning: "If you modify features, API, or usage, you MUST update the documentation immediately."
"""

from __future__ import annotations

from typing import Any

from textual import work
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Container, Horizontal, Vertical
from textual.widgets import DataTable, Footer, Header, Input, Static, Tree

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

    CSS = """
    Screen {
        background: $surface;
    }

    #sidebar {
        width: 25%;
        height: 100%;
        border-right: solid $primary;
    }

    #main_content {
        width: 75%;
        height: 100%;
    }

    #filter_input {
        margin: 1;
    }

    DataTable {
        height: 60%;
    }

    #details_panel {
        height: 40%;
        border-top: double $primary;
        padding: 1;
        background: $surface;
        overflow-y: scroll;
    }

    .bold {
        text-style: bold;
        color: $accent;
    }
    """

    BINDINGS = [
        Binding("q", "quit", "Quit"),
        Binding("r", "refresh", "Refresh"),
        Binding("b", "boot", "Boot Task", show=True),
        Binding("w", "rewind", "Rewind Task", show=True),
        Binding("c", "complete", "Mark Complete", show=True),
    ]

    def __init__(self, workflow_file: str, database_file: str, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.parser: RocotoParser = RocotoParser(workflow_file, database_file)
        self.all_data: list[dict[str, Any]] = []
        self.last_selected_task: dict[str, Any] | None = None

    def compose(self) -> ComposeResult:
        """Compose the UI layout."""
        yield Header(show_clock=True)
        yield Horizontal(
            Container(Tree("Cycles", id="cycle_tree"), id="sidebar"),
            Vertical(
                Input(placeholder="Filter tasks by name...", id="filter_input"),
                DataTable(id="status_table", cursor_type="row"),
                Static("Select a task to see details", id="details_panel"),
                id="main_content",
            ),
        )
        yield Footer()

    def on_mount(self) -> None:
        """Handle application mount event."""
        self.set_interval(30, self.action_refresh)  # Auto-refresh every 30 seconds
        self.action_refresh()

    @work(thread=True)
    def action_refresh(self) -> None:
        """Perform background refresh of data."""
        try:
            # We don't notify on auto-refresh to avoid being annoying
            # self.call_from_thread(self.notify, "Refreshing data...")
            self.parser.parse_workflow()
            self.all_data = self.parser.get_status()
            self.call_from_thread(self._update_ui)
        except Exception as e:
            self.call_from_thread(self.notify, f"Error refreshing data: {e}", severity="error")

    def on_input_changed(self, event: Input.Changed) -> None:
        """Handle filter input changes."""
        self._update_ui()

    def _update_ui(self) -> None:
        """Update UI widgets with new data."""
        filter_text = self.query_one("#filter_input", Input).value.lower()

        tree = self.query_one("#cycle_tree", Tree)
        tree.clear()
        tree.root.expand()

        table = self.query_one("#status_table", DataTable)
        # Store current scroll position if possible?
        # Textual DataTable clear might lose it.
        table.clear(columns=True)
        table.add_columns("Cycle", "Task", "Job ID", "State", "Exit", "Tries", "Duration")

        for cycle_info in self.all_data:
            cycle_str = cycle_info["cycle"]
            cycle_node = None

            for task in cycle_info["tasks"]:
                task_name = task["task"]
                if filter_text and filter_text not in task_name.lower():
                    continue

                if cycle_node is None:
                    cycle_node = tree.root.add(cycle_str, expand=True)

                cycle_node.add_leaf(task_name)

                row_key = f"{cycle_str}:{task_name}"
                table.add_row(
                    cycle_str,
                    task_name,
                    str(task["jobid"] or "-"),
                    task["state"],
                    str(task["exit"] if task["exit"] is not None else "-"),
                    str(task["tries"]),
                    str(task["duration"] or "-"),
                    key=row_key,
                )

    def on_data_table_row_selected(self, event: DataTable.RowSelected) -> None:
        """Handle row selection to show details."""
        row_key_obj = event.row_key
        if not row_key_obj or row_key_obj.value is None:
            return

        row_key = str(row_key_obj.value)
        if ":" not in row_key:
            return

        cycle_str, task_name = row_key.split(":", 1)

        selected_task = None
        for cycle_info in self.all_data:
            if cycle_info["cycle"] == cycle_str:
                for task in cycle_info["tasks"]:
                    if task["task"] == task_name:
                        selected_task = task
                        break
                if selected_task:
                    break

        if selected_task:
            self.last_selected_task = selected_task
            self._display_details(selected_task, cycle_str)

    def _display_details(self, task: dict[str, Any], cycle: str) -> None:
        """Display task details in the panel."""
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
        """Placeholder for rocotoboot."""
        if self.last_selected_task:
            task_name = self.last_selected_task["task"]
            self.notify(f"Booting task: {task_name} (Mock Action)")
        else:
            self.notify("No task selected", severity="warning")

    def action_rewind(self) -> None:
        """Placeholder for rocotorewind."""
        if self.last_selected_task:
            task_name = self.last_selected_task["task"]
            self.notify(f"Rewinding task: {task_name} (Mock Action)")
        else:
            self.notify("No task selected", severity="warning")

    def action_complete(self) -> None:
        """Placeholder for rocotocomplete."""
        if self.last_selected_task:
            task_name = self.last_selected_task["task"]
            self.notify(f"Marking task complete: {task_name} (Mock Action)")
        else:
            self.notify("No task selected", severity="warning")


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 3:
        print("Usage: rocotoviewer -w <workflow.xml> -d <database.db>")
        sys.exit(1)
    app = RocotoApp(sys.argv[1], sys.argv[2])
    app.run()
