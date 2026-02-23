import sqlite3

import pytest
from textual.widgets import DataTable, Static

from rocotoviewer.app import RocotoApp


@pytest.fixture
def mock_rocoto_data(tmp_path):
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
async def test_status_bar_path(mock_rocoto_data):
    wf, db = mock_rocoto_data
    app = RocotoApp(workflow_file=wf, database_file=db)
    async with app.run_test() as pilot:
        await pilot.pause(0.5)

        status_bar = app.query_one("#status_bar", Static)
        assert "Path: Workflow" in str(status_bar.content)

        # Select task in table
        table = app.query_one(DataTable)
        table.focus()
        await pilot.press("enter")

        await pilot.pause(0.1)

        assert "202301010000" in str(status_bar.content)
        assert "task1" in str(status_bar.content)
