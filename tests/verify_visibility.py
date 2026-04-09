import sqlite3

from rocotoviewer.parser import RocotoParser


def test_all_cycles_show_all_tasks(tmp_path):
    workflow_file = tmp_path / "workflow_all.xml"
    db_file = tmp_path / "rocoto_all.db"

    # Define a workflow where tasks have disjoint cycledefs
    workflow_content = """<?xml version="1.0"?>
<workflow name="test">
  <cycledef group="cycle1">202301010000 202301010000 01:00:00</cycledef>
  <cycledef group="cycle2">202301010100 202301010100 01:00:00</cycledef>
  <task name="task1" cycledefs="cycle1"></task>
  <task name="task2" cycledefs="cycle2"></task>
</workflow>"""
    workflow_file.write_text(workflow_content)

    # Cycles in DB
    conn = sqlite3.connect(db_file)
    c = conn.cursor()
    c.execute("CREATE TABLE cycles (cycle INTEGER)")
    c.execute("INSERT INTO cycles VALUES (1672531200)")  # 00:00
    c.execute("INSERT INTO cycles VALUES (1672534800)")  # 01:00

    c.execute("""
        CREATE TABLE jobs (
            taskname TEXT, cycle INTEGER, state TEXT,
            exit_status INTEGER, duration INTEGER, tries INTEGER, jobid TEXT
        )
    """)
    # Only cycle 1 has task1, only cycle 2 has task2 (normally)
    c.execute("INSERT INTO jobs VALUES ('task1', 1672531200, 'SUCCEEDED', 0, 100, 1, '1')")
    c.execute("INSERT INTO jobs VALUES ('task2', 1672534800, 'SUCCEEDED', 0, 100, 1, '2')")
    conn.commit()
    conn.close()

    parser = RocotoParser(str(workflow_file), str(db_file))
    parser.parse_workflow()
    status = parser.get_status()

    # Both cycles should show BOTH tasks
    for i, cycle_data in enumerate(status):
        tasks = [t["task"] for t in cycle_data["tasks"]]
        print(f"Cycle {i} tasks: {tasks}")
        assert "task1" in tasks, f"task1 missing from cycle {i}"
        assert "task2" in tasks, f"task2 missing from cycle {i}"
