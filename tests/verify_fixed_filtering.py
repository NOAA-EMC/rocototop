import os
import sqlite3
import pytest
from rocotoviewer.parser import RocotoParser

def test_filtering_logic_preserved_except_first_cycle(tmp_path):
    workflow_file = tmp_path / "workflow_filt.xml"
    db_file = tmp_path / "rocoto_filt.db"

    workflow_content = """<?xml version="1.0"?>
<workflow name="test">
  <cycledef group="gcdas">202304011800 202304011800 06:00:00</cycledef>
  <cycledef group="gcafs">202304020000 202304020000 06:00:00</cycledef>
  <task name="gcdas_job" cycledefs="gcdas"></task>
  <task name="gcafs_job" cycledefs="gcafs"></task>
</workflow>"""
    workflow_file.write_text(workflow_content)

    conn = sqlite3.connect(db_file)
    c = conn.cursor()
    c.execute("CREATE TABLE cycles (cycle INTEGER)")
    c.execute("INSERT INTO cycles VALUES (1680372000)") # 202304011800 (First)
    c.execute("INSERT INTO cycles VALUES (1680393600)") # 202304020000 (Second)
    
    c.execute("""
        CREATE TABLE jobs (
            taskname TEXT, cycle INTEGER, state TEXT,
            exit_status INTEGER, duration INTEGER, tries INTEGER, jobid TEXT
        )
    """)
    conn.commit()
    conn.close()

    parser = RocotoParser(str(workflow_file), str(db_file))
    parser.parse_workflow()
    status = parser.get_status()

    # Cycle 0 (18z) - SHOULD show ALL because it's first
    c0_tasks = [t["task"] for t in status[0]["tasks"]]
    print(f"Cycle 0 (18z) tasks: {c0_tasks}")
    assert "gcdas_job" in c0_tasks
    assert "gcafs_job" in c0_tasks

    # Cycle 1 (00z) - SHOULD ONLY show gcafs_job (per cycledefs)
    c1_tasks = [t["task"] for t in status[1]["tasks"]]
    print(f"Cycle 1 (00z) tasks: {c1_tasks}")
    assert "gcafs_job" in c1_tasks
    assert "gcdas_job" not in c1_tasks
