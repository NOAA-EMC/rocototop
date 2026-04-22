import pytest
from rocototop.parser import RocotoParser


@pytest.mark.asyncio
async def test_system_entity_resolution(tmp_path):
    # Create an external file for the entity
    ext_file = tmp_path / "tasks.xml"
    ext_file.write_text('<task name="ext_task"></task>')

    workflow_file = tmp_path / "workflow.xml"
    workflow_content = f"""<?xml version="1.0"?>
<!DOCTYPE workflow [
  <!ENTITY ext_tasks SYSTEM "{ext_file.name}">
]>
<workflow name="test">
  <cycledef group="default">202301010000 202301010000 01:00:00</cycledef>
  &ext_tasks;
</workflow>"""
    workflow_file.write_text(workflow_content)

    parser = RocotoParser(str(workflow_file), "dummy.db")
    await parser.parse_workflow()

    assert "ext_task" in parser.tasks_dict


@pytest.mark.asyncio
async def test_tasks_tag_parsing(tmp_path):
    workflow_file = tmp_path / "workflow.xml"
    workflow_content = """<?xml version="1.0"?>
<workflow name="test">
  <cycledef group="default">202301010000 202301010000 01:00:00</cycledef>
  <tasks>
    <task name="task_in_group"></task>
    <metatask name="meta">
      <var name="V">1 2</var>
      <tasks>
        <task name="task_#V#"></task>
      </tasks>
    </metatask>
  </tasks>
</workflow>"""
    workflow_file.write_text(workflow_content)

    parser = RocotoParser(str(workflow_file), "dummy.db")
    await parser.parse_workflow()

    assert "task_in_group" in parser.tasks_dict
    assert "task_1" in parser.tasks_dict
    assert "task_2" in parser.tasks_dict
