import sqlite3

import pytest
from textual.widgets import DataTable, RichLog

from rocotoviewer.app import RocotoApp


@pytest.fixture
def mock_rocoto_with_logs(tmp_path):
    workflow_file = tmp_path / "workflow.xml"
    db_file = tmp_path / "rocoto.db"
    log_file = tmp_path / "task1.log"

    log_file.write_text("Line 1\nLine 2\n")

    workflow_content = f"""<?xml version="1.0"?>
<workflow name="test">
  <cycledef group="default">202301010000 202301011200 06:00:00</cycledef>
  <task name="task1" cycledefs="default">
    <stdout>{log_file}</stdout>
  </task>
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

    return str(workflow_file), str(db_file), str(log_file)


@pytest.mark.asyncio
async def test_log_viewer_toggle_and_content(mock_rocoto_with_logs):
    wf, db, log = mock_rocoto_with_logs
    app = RocotoApp(workflow_file=wf, database_file=db)
    async with app.run_test() as pilot:
        await pilot.pause(0.5)

        # Toggle log viewer
        await pilot.press("l")
        log_panel = app.query_one("#log_panel", RichLog)
        assert log_panel.display is True

        # Select task to load log
        table = app.query_one(DataTable)
        table.focus()
        await pilot.press("enter")  # Select first row

        await pilot.pause(0.5)

        # Check content
        assert log_panel.virtual_size.height > 0

        initial_height = log_panel.virtual_size.height

        # Append to log file and check if it updates
        with open(log, "a") as f:
            f.write("Line 3\n")
            f.flush()

        await pilot.pause(0.5)  # Wait for tailer to pick it up

        # The virtual height should have increased
        assert log_panel.virtual_size.height > initial_height
