import sqlite3

import pytest
from textual.widgets import RichLog, Tree

from rocototop.app import RocotoApp


@pytest.fixture
def mock_rocoto_large_log(tmp_path):
    workflow_file = tmp_path / "workflow.xml"
    db_file = tmp_path / "rocoto.db"
    log_file = tmp_path / "large.log"

    # Create a log file larger than 100KB
    with open(log_file, "w") as f:
        for i in range(10000):
            f.write(f"Line {i:05} - some extra text to make it bigger\n")

    workflow_content = f"""<?xml version="1.0"?>
<workflow name="test">
  <cycledef group="default">202301010000 202301010000 06:00:00</cycledef>
  <task name="task1" cycledefs="default">
    <stdout>{log_file}</stdout>
  </task>
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

    return str(workflow_file), str(db_file), str(log_file)


@pytest.mark.asyncio
async def test_large_log_truncation(mock_rocoto_large_log):
    wf, db, log = mock_rocoto_large_log
    app = RocotoApp(workflow_file=wf, database_file=db)
    # Set a smaller limit for testing
    app.MAX_LOG_READ_SIZE = 1000

    async with app.run_test() as pilot:
        await pilot.pause(0.5)

        # Select task from tree to load log
        tree = app.query_one("#cycle_tree", Tree)
        cycle_node = tree.root.children[0]
        cycle_node.expand()
        await pilot.pause(0.1)
        task_node = cycle_node.children[0]
        tree.select_node(task_node)

        await pilot.pause(0.5)

        log_panel = app.query_one("#log_panel", RichLog)

        # Check if truncation message is present
        # If the file was 10,000 lines, but we only read 1000 bytes,
        # the number of lines should be small.
        assert log_panel.virtual_size.height < 100


@pytest.mark.asyncio
async def test_tree_node_reuse(mock_rocoto_large_log):
    wf, db, log = mock_rocoto_large_log
    app = RocotoApp(workflow_file=wf, database_file=db)

    async with app.run_test() as pilot:
        await pilot.pause(0.5)

        tree = app.query_one("#cycle_tree", Tree)
        cycle_node = tree.root.children[0]
        cycle_node.expand()
        await pilot.pause(0.1)
        task_node = cycle_node.children[0]

        initial_node_id = id(task_node)

        # Trigger an update
        app.action_refresh()
        await pilot.pause(0.5)

        # Get the task node again
        cycle_node = tree.root.children[0]
        task_node = cycle_node.children[0]

        assert id(task_node) == initial_node_id
