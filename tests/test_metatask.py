import os
import xml.etree.ElementTree as ET
from rocotoviewer.parser import RocotoParser

def test_metatask_expansion(tmp_path):
    workflow_file = tmp_path / "workflow.xml"
    db_file = tmp_path / "rocoto.db"

    workflow_content = """<?xml version="1.0"?>
<workflow name="test">
  <metatask name="ensemble">
    <var name="member">01 02</var>
    <metatask name="member_#member#_tasks">
        <var name="forecast">00 06</var>
        <task name="post_#member#_#forecast#"></task>
    </metatask>
  </metatask>
</workflow>"""
    workflow_file.write_text(workflow_content)

    parser = RocotoParser(str(workflow_file), str(db_file))
    parser.parse_workflow()

    # Expected tasks:
    # post_01_00, post_01_06, post_02_00, post_02_06
    task_names = parser.tasks_ordered
    assert len(task_names) == 4
    assert "post_01_00" in task_names
    assert "post_01_06" in task_names
    assert "post_02_00" in task_names
    assert "post_02_06" in task_names

    # Check metatask list expansion
    assert "member_01_tasks" in parser.metatask_list
    assert "post_01_00" in parser.metatask_list["member_01_tasks"]
    assert "post_01_06" in parser.metatask_list["member_01_tasks"]

    assert "ensemble" in parser.metatask_list
    assert len(parser.metatask_list["ensemble"]) == 4

def test_parallel_vars_expansion(tmp_path):
    workflow_file = tmp_path / "workflow.xml"
    db_file = tmp_path / "rocoto.db"

    workflow_content = """<?xml version="1.0"?>
<workflow name="test">
  <metatask name="parallel">
    <var name="v1">a b</var>
    <var name="v2">1 2</var>
    <task name="task_#v1#_#v2#"></task>
  </metatask>
</workflow>"""
    workflow_file.write_text(workflow_content)

    parser = RocotoParser(str(workflow_file), str(db_file))
    parser.parse_workflow()

    task_names = parser.tasks_ordered
    assert len(task_names) == 2
    assert "task_a_1" in task_names
    assert "task_b_2" in task_names
    assert "task_a_2" not in task_names
