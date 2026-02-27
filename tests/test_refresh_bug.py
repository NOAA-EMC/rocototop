import sqlite3

import pytest
from textual.widgets import Tree

from rocotoviewer.app import RocotoApp


@pytest.mark.asyncio
async def test_refresh_updates_status(tmp_path):
    wf = tmp_path / "workflow.xml"
    db = tmp_path / "rocoto.db"

    wf.write_text("""<?xml version="1.0"?>
<workflow name="test">
  <cycledef group="default">202301010000 202301010000 01:00:00</cycledef>
  <task name="task1" cycledefs="default"></task>
</workflow>""")

    conn = sqlite3.connect(db)
    c = conn.cursor()
    c.execute("CREATE TABLE cycles (cycle INTEGER)")
    c.execute("INSERT INTO cycles VALUES (202301010000)")
    c.execute(
        "CREATE TABLE jobs (taskname TEXT, cycle INTEGER, state TEXT, "
        "exit_status INTEGER, duration INTEGER, tries INTEGER, jobid TEXT)"
    )
    c.execute("INSERT INTO jobs VALUES ('task1', 202301010000, 'QUEUED', NULL, NULL, 0, '1')")
    conn.commit()
    conn.close()

    app = RocotoApp(workflow_file=str(wf), database_file=str(db), refresh_interval=1)
    async with app.run_test() as pilot:
        await pilot.pause(0.5)

        # Check initial state
        tree = app.query_one("#cycle_tree", Tree)
        assert len(tree.root.children) > 0
        cycle_node = tree.root.children[0]
        cycle_node.expand()
        await pilot.pause(0.2)
        assert len(cycle_node.children) > 0
        task_node = cycle_node.children[0]
        assert "QUEUED" in str(task_node.label)

        # Update database
        conn = sqlite3.connect(db)
        c = conn.cursor()
        c.execute("UPDATE jobs SET state='RUNNING' WHERE taskname='task1'")
        conn.commit()
        conn.close()

        # Trigger refresh
        await pilot.press("r")
        await pilot.pause(0.5)

        # Check if updated
        assert "RUNNING" in str(task_node.label)
