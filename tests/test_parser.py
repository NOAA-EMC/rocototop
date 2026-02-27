import os
import sqlite3

import pytest

from rocotoviewer.parser import RocotoParser


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

    return str(workflow_file), str(db_file)


def test_parser_init(mock_rocoto_files):
    wf, db = mock_rocoto_files
    parser = RocotoParser(wf, db)
    assert parser.workflow_file == wf
    assert parser.database_file == db
    parser.parse_workflow()
    assert len(parser.tasks_ordered) > 0
    assert "task1" in parser.tasks_ordered


def test_parser_get_status(mock_rocoto_files):
    wf, db = mock_rocoto_files
    parser = RocotoParser(wf, db)
    # We should call parse_workflow to have task definitions
    parser.parse_workflow()
    status = parser.get_status()
    assert len(status) == 1
    assert status[0]["cycle"] == "202301010000"
    assert len(status[0]["tasks"]) == 1
    assert status[0]["tasks"][0]["task"] == "task1"
    assert status[0]["tasks"][0]["state"] == "SUCCEEDED"


def test_parser_cycle_parsing():
    parser = RocotoParser("wf", "db")
    # Test YYYYMMDDHHMM
    assert parser._parse_cycle(202301010000) == "202301010000"
    # Test timestamp
    assert parser._parse_cycle(1672531200) == "202301010000"


def test_parser_xml_caching(tmp_path):
    workflow_file = tmp_path / "workflow.xml"
    workflow_file.write_text("<workflow><task name='t1'></task></workflow>")

    parser = RocotoParser(str(workflow_file), "db")
    parser.parse_workflow()
    assert parser._last_parsed_mtime is not None
    mtime1 = parser._last_parsed_mtime

    # Second call should skip parsing
    parser.parse_workflow()
    assert parser._last_parsed_mtime == mtime1

    # Modify file
    # Ensure mtime changes by setting it explicitly to avoid waiting
    new_mtime = mtime1 + 100
    workflow_file.write_text("<workflow><task name='t1'></task><task name='t2'></task></workflow>")
    os.utime(workflow_file, (new_mtime, new_mtime))

    parser.parse_workflow()
    assert parser._last_parsed_mtime == new_mtime
    assert "t2" in parser.tasks_dict


def test_get_status_structure(mock_rocoto_files):
    wf, db = mock_rocoto_files
    parser = RocotoParser(wf, db)
    parser.parse_workflow()
    status = parser.get_status()
    assert isinstance(status, list)
    assert len(status) > 0
    cycle = status[0]
    assert "cycle" in cycle
    assert "tasks" in cycle
    assert len(cycle["tasks"]) > 0
    task = cycle["tasks"][0]
    expected_keys = {"task", "state", "exit", "duration", "tries", "jobid", "details"}
    assert expected_keys.issubset(task.keys())


def test_parser_get_summary(mock_rocoto_files):
    wf, db = mock_rocoto_files
    parser = RocotoParser(wf, db)
    parser.parse_workflow()
    status = parser.get_status()

    summary = parser.get_summary(status)
    assert summary == {"SUCCEEDED": 1}

    # Add a dummy task with a different state
    status.append(
        {
            "cycle": "202301010600",
            "tasks": [
                {
                    "task": "task2",
                    "state": "RUNNING",
                    "exit": None,
                    "duration": None,
                    "tries": 1,
                    "jobid": "123",
                    "details": {},
                }
            ],
        }
    )
    summary = parser.get_summary(status)
    assert summary == {"SUCCEEDED": 1, "RUNNING": 1}
