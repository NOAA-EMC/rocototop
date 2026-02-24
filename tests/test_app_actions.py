import sqlite3

import pytest

from rocotoviewer.app import RocotoApp


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
    c.execute("INSERT INTO cycles VALUES (1672531200)")
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
async def test_app_actions(mock_rocoto_files):
    wf, db = mock_rocoto_files
    app = RocotoApp(workflow_file=wf, database_file=db)
    async with app.run_test() as pilot:
        await pilot.pause(0.5)

        # Select a task
        from textual.widgets import Tree

        tree = app.query_one("#cycle_tree", Tree)
        cycle_node = tree.root.children[0]
        cycle_node.expand()
        await pilot.pause(0.1)
        task_node = cycle_node.children[0]
        tree.select_node(task_node)
        await pilot.pause(0.1)

        # Test boot action
        await pilot.press("b")
        # Test rewind action
        await pilot.press("w")
        # Test complete action
        await pilot.press("c")

        # Also test with no selection
        app.last_selected_task = None
        await pilot.press("b")
        await pilot.press("w")
        await pilot.press("c")


@pytest.mark.asyncio
async def test_app_toggle_log_follow(mock_rocoto_files):
    wf, db = mock_rocoto_files
    app = RocotoApp(workflow_file=wf, database_file=db)
    async with app.run_test() as pilot:
        await pilot.pause(0.1)

        # Toggle log follow
        assert app.log_follow is True
        await pilot.press("f")
        assert app.log_follow is False
        await pilot.press("f")
        assert app.log_follow is True
