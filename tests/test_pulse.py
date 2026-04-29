import sqlite3
from unittest.mock import AsyncMock, patch

import pytest

from rocototop.app import RocotoApp


@pytest.fixture
def mock_rocoto_files(tmp_path):
    workflow_file = tmp_path / "workflow.xml"
    db_file = tmp_path / "rocoto.db"

    workflow_content = """<?xml version="1.0"?>
<workflow name="test">
  <cycledef group="default">202301010000 202301010000 06:00:00</cycledef>
  <task name="task1" cycledefs="default"></task>
</workflow>"""
    workflow_file.write_text(workflow_content)

    conn = sqlite3.connect(db_file)
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

    return str(workflow_file), str(db_file)


@pytest.mark.asyncio
async def test_pulse_runs_rocotorun(mock_rocoto_files):
    wf, db = mock_rocoto_files
    app = RocotoApp(workflow_file=wf, database_file=db)

    with patch("asyncio.create_subprocess_exec") as mock_exec:
        mock_process = AsyncMock()
        mock_process.communicate.return_value = (b"", b"")
        mock_process.returncode = 0
        mock_exec.return_value = mock_process

        async with app.run_test() as pilot:
            for _ in range(50):
                if not app.workers:
                    break
                await pilot.pause(0.1)

            # Reset mock after initial refresh on mount
            mock_exec.reset_mock()

            # Press 'R' for run (rocotorun)
            await pilot.press("R")
            for _ in range(50):
                if not app.workers:
                    break
                await pilot.pause(0.1)

            # Verify rocotorun was called
            mock_exec.assert_called_once()
            args, kwargs = mock_exec.call_args
            assert args[0] == "rocotorun"
            assert "-w" in args
            assert wf in args
            assert "-d" in args
            assert db in args


@pytest.mark.asyncio
async def test_refresh_runs_rocotorun(mock_rocoto_files):
    wf, db = mock_rocoto_files
    app = RocotoApp(workflow_file=wf, database_file=db)

    with patch("asyncio.create_subprocess_exec") as mock_exec:
        mock_process = AsyncMock()
        mock_process.communicate.return_value = (b"", b"")
        mock_process.returncode = 0
        mock_exec.return_value = mock_process

        async with app.run_test() as pilot:
            for _ in range(50):
                if not app.workers:
                    break
                await pilot.pause(0.1)

            # Reset mock
            mock_exec.reset_mock()

            # Press 'R' for run (rocotorun) — matches rocoto_viewer's <R> key
            await pilot.press("R")
            for _ in range(50):
                if not app.workers:
                    break
                await pilot.pause(0.1)

            # Verify rocotorun was called
            mock_exec.assert_called_once()
            assert mock_exec.call_args[0][0] == "rocotorun"
