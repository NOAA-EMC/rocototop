import sqlite3

import pytest
from textual.widgets import DataTable, Input, Static

from rocotoviewer.app import RocotoApp


@pytest.fixture
def mock_advanced_files(tmp_path):
    wf = tmp_path / "wf.xml"
    db = tmp_path / "db.db"
    wf.write_text("""<workflow><task name="t1"><command>cmd</command></task></workflow>""")
    conn = sqlite3.connect(db)
    conn.execute("CREATE TABLE cycles (cycle INTEGER)")
    conn.execute("INSERT INTO cycles VALUES (202301010000)")
    conn.execute(
        "CREATE TABLE jobs (taskname TEXT, cycle INTEGER, state TEXT, "
        "exit_status INTEGER, duration INTEGER, tries INTEGER, jobid TEXT)"
    )
    conn.execute("INSERT INTO jobs VALUES ('t1', 202301010000, 'SUCCEEDED', 0, 10, 1, '123')")
    conn.commit()
    conn.close()
    return str(wf), str(db)


@pytest.mark.asyncio
async def test_app_details_display(mock_advanced_files):
    wf, db = mock_advanced_files
    app = RocotoApp(wf, db)
    async with app.run_test() as pilot:
        await pilot.pause(0.5)
        tree = app.query_one("#cycle_tree")
        cycle_node = tree.root.children[0]
        cycle_node.expand()
        await pilot.pause(0.1)
        task_node = cycle_node.children[0]
        tree.select_node(task_node)
        await pilot.pause(0.2)

        table = app.query_one(DataTable)
        assert table.row_count == 1

        details = app.query_one("#details_panel", Static)
        content = str(details.render())
        assert "t1" in content
        assert "cmd" in content


@pytest.mark.asyncio
async def test_app_filtering(mock_advanced_files):
    wf, db = mock_advanced_files
    app = RocotoApp(wf, db)
    async with app.run_test() as pilot:
        await pilot.pause(0.5)
        tree = app.query_one("#cycle_tree")
        cycle_node = tree.root.children[0]
        cycle_node.expand()
        await pilot.pause(0.1)
        assert len(cycle_node.children) == 1

        # Filter out
        input_widget = app.query_one(Input)
        input_widget.focus()
        for char in "nonexistent":
            await pilot.press(char)
        await pilot.pause(0.1)
        # Tree should be empty (cycle node still there but no children)
        assert len(cycle_node.children) == 0

        # Backspace
        for _ in range(11):
            await pilot.press("backspace")
        await pilot.pause(0.1)
        assert len(cycle_node.children) == 1
