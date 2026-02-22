from __future__ import annotations

from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Container, Horizontal
from textual.widgets import Footer, Header, Tree, DataTable
from textual import work

from rocotoviewer.parser import RocotoParser

class RocotoApp(App):
    CSS = """
    Screen {
        background: $surface;
    }

    #sidebar {
        width: 30%;
        height: 100%;
        border-right: solid $primary;
    }

    #main_content {
        width: 70%;
        height: 100%;
    }

    DataTable {
        height: 100%;
    }
    """

    BINDINGS = [
        Binding("q", "quit", "Quit"),
        Binding("r", "refresh", "Refresh"),
    ]

    def __init__(self, workflow_file: str, database_file: str, **kwargs):
        super().__init__(**kwargs)
        self.parser = RocotoParser(workflow_file, database_file)

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        yield Horizontal(
            Container(Tree("Cycles", id="cycle_tree"), id="sidebar"),
            Container(DataTable(id="status_table"), id="main_content"),
        )
        yield Footer()

    def on_mount(self) -> None:
        self.action_refresh()

    @work(thread=True)
    def action_refresh(self) -> None:
        """Perform background refresh of data."""
        self.call_from_thread(self.notify, "Refreshing data...")

        # Parse workflow if not already done (or to update)
        self.parser.parse_workflow()

        # Get status from DB
        status_data = self.parser.get_status()

        self.call_from_thread(self._update_ui, status_data)

    def _update_ui(self, status_data):
        """Update UI widgets with new data. Must be called from main thread."""
        tree = self.query_one("#cycle_tree", Tree)
        tree.clear()

        table = self.query_one("#status_table", DataTable)
        table.clear(columns=True)
        table.add_columns("Cycle", "Task", "Job ID", "State", "Exit", "Tries", "Duration")

        for cycle_info in status_data:
            cycle_node = tree.root.add(cycle_info['cycle'], expand=True)
            for task in cycle_info['tasks']:
                cycle_node.add_leaf(task['task'])
                table.add_row(
                    cycle_info['cycle'],
                    task['task'],
                    str(task['jobid']),
                    task['state'],
                    str(task['exit']),
                    str(task['tries']),
                    str(task['duration'])
                )

        self.notify("Status Refreshed")

if __name__ == "__main__":
    import sys
    app = RocotoApp(sys.argv[1], sys.argv[2])
    app.run()
