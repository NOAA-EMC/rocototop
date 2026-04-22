import sqlite3
import time

import pytest

from rocototop.parser import RocotoParser


@pytest.mark.asyncio
async def test_parser_performance_large_workflow(tmp_path):
    """
    Test RocotoParser performance with a large number of cycles and tasks.
    This ensures that the O(1) lookup optimization is effective.
    """
    workflow_file = tmp_path / "large_workflow.xml"
    db_file = tmp_path / "large_rocoto.db"

    # Create a workflow with 1000 cycles and 50 tasks
    num_cycles = 1000
    num_tasks = 50

    tasks_xml = "\n".join([f'  <task name="task_{i}" cycledefs="default"></task>' for i in range(num_tasks)])
    workflow_content = f"""<?xml version="1.0"?>
<workflow name="large_test">
  <cycledef group="default">202301010000 202302111200 01:00:00</cycledef>
{tasks_xml}
</workflow>"""
    workflow_file.write_text(workflow_content)

    # Create a database and populate it
    conn = sqlite3.connect(db_file)
    c = conn.cursor()
    c.execute("CREATE TABLE cycles (cycle INTEGER)")
    c.execute("""
        CREATE TABLE jobs (
            taskname TEXT, cycle INTEGER, state TEXT,
            exit_status INTEGER, duration INTEGER, tries INTEGER, jobid TEXT
        )
    """)

    start_ts = 1672531200  # 2023-01-01 00:00:00
    for i in range(num_cycles):
        ts = start_ts + i * 3600
        c.execute("INSERT INTO cycles VALUES (?)", (ts,))
        # Populate some jobs to make it more realistic
        if i % 10 == 0:
            for j in range(num_tasks):
                c.execute("INSERT INTO jobs VALUES (?, ?, 'SUCCEEDED', 0, 100, 1, ?)", (f"task_{j}", ts, f"job_{i}_{j}"))

    conn.commit()
    conn.close()

    parser = RocotoParser(str(workflow_file), str(db_file))

    # Measure parse time
    start_time = time.time()
    await parser.parse_workflow()
    parse_duration = time.time() - start_time

    # Measure status fetching time
    start_time = time.time()
    status = await parser.get_status()
    fetch_duration = time.time() - start_time

    assert len(status) == num_cycles
    assert len(status[0]["tasks"]) == num_tasks

    # With 1000 cycles and 50 tasks, this should be very fast (< 1s)
    # Even on slow environments, O(1) lookups should keep this well under a few seconds.
    print(f"Parse duration: {parse_duration:.4f}s")
    print(f"Fetch duration: {fetch_duration:.4f}s")
    assert fetch_duration < 2.0
