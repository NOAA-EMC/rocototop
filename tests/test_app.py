import sqlite3

import pytest
from textual.widgets import DataTable, Tree

from rocototop.app import RocotoApp


@pytest.fixture
def mock_rocoto_files(tmp_path):
    workflow_file = tmp_path / "workflow.xml"
    db_file = tmp_path / "rocoto.db"

    workflow_content = """<?xml version="1.0"?>
<workflow name="test">
  <cycledef group="default">202301010000 202301011200 06:00:00</cycledef>
  <task name="task1" cycledefs="default"></task>
</workflow>"""
    workflow_file.write_text(workflow_content)

    conn = sqlite3.connect(db_file)
    c = conn.cursor()
    c.execute("CREATE TABLE cycles (cycle INTEGER)")
    c.execute("INSERT INTO cycles VALUES (1672531200)")  # 2023-01-01 00:00:00
    c.execute("""
        CREATE TABLE jobs (
            taskname TEXT, cycle INTEGER, state TEXT,
            exit_status INTEGER, duration INTEGER, tries INTEGER, jobid TEXT
        )
    """)
    c.execute("INSERT INTO jobs VALUES ('task1', 1672531200, 'SUCCEEDED', 0, 100, 1, '12345')")
    conn.commit()
    conn.close()

    return str(workflow_file), str(db_file)


@pytest.mark.asyncio
async def test_app_ui_loading(mock_rocoto_files):
    wf, db = mock_rocoto_files
    app = RocotoApp(workflow_file=wf, database_file=db)
    async with app.run_test() as pilot:
        # Wait for refresh to complete (it's a background worker)
        await pilot.pause(0.5)

        tree = app.query_one("#cycle_tree", Tree)
        assert tree.root.children
        assert "202301010000" in str(tree.root.children[0].label)

        # Initially no task selected, so table should be empty or have no rows
        table = app.query_one("#selected_task_status", DataTable)
        assert table.row_count == 0

        # Select a task in the tree
        tree = app.query_one("#cycle_tree", Tree)
        cycle_node = tree.root.children[0]
        cycle_node.expand()
        await pilot.pause(0.1)
        task_node = cycle_node.children[0]
        tree.select_node(task_node)
        await pilot.pause(0.1)

        assert table.row_count == 1
        # Check if task1 is in the table (it should have an icon now)
        row = [str(cell) for cell in table.get_row_at(0)]
        assert any("task1" in cell for cell in row)


def test_app_refresh_interval_assignment(mock_rocoto_files):
    wf, db = mock_rocoto_files
    app = RocotoApp(workflow_file=wf, database_file=db, refresh_interval=42)
    assert app.refresh_interval == 42


@pytest.mark.asyncio
async def test_app_status_bar_and_filtering(mock_rocoto_files):
    wf, db = mock_rocoto_files
    app = RocotoApp(workflow_file=wf, database_file=db)
    async with app.run_test() as pilot:
        await pilot.pause(0.5)

        status_bar = app.query_one("#status_bar")
        # Check summary in status bar
        assert "S:1" in str(status_bar.render())

        # Test filtering
        tree = app.query_one("#cycle_tree", Tree)

        # Initially 1 cycle
        assert len(tree.root.children) == 1

        # Filter for something that doesn't exist
        await pilot.click("#filter_input")
        for char in "nonexistent":
            await pilot.press(char)
        await pilot.pause(0.1)

        # Cycle should be hidden
        assert len(tree.root.children) == 0

        # Clear filter
        for _ in range(11):
            await pilot.press("backspace")
        await pilot.pause(0.1)

        # Cycle should be back
        assert len(tree.root.children) == 1

        # Select task and check status bar path
        cycle_node = tree.root.children[0]
        cycle_node.expand()
        await pilot.pause(0.1)
        task_node = cycle_node.children[0]
        tree.select_node(task_node)
        await pilot.pause(0.1)

        assert "Path: Workflow > 202301010000 > task1" in str(status_bar.render())
