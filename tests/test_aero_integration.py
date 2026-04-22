import pytest

from rocototop.parser import RocotoParser


@pytest.mark.asyncio
async def test_complex_cyclestr_resolution():
    """Test resolution of complex <cyclestr> tags with offsets and various flags."""
    parser = RocotoParser("wf", "db")
    cycle = "202301011200"

    # Test multiple tags and various flags
    text = "Log at <cyclestr>@Y/@m/@d @H:@M</cyclestr> for cycle <cyclestr>@Y@m@d@H@M</cyclestr>"
    expected = "Log at 2023/01/01 12:00 for cycle 202301011200"
    assert parser.resolve_cyclestr(text, cycle) == expected

    # Test offset (positive)
    text = "Next cycle: <cyclestr offset='06:00:00'>@Y@m@d@H@M</cyclestr>"
    expected = "Next cycle: 202301011800"
    assert parser.resolve_cyclestr(text, cycle) == expected

    # Test offset (negative)
    text = "Prev cycle: <cyclestr offset='-06:00:00'>@Y@m@d@H@M</cyclestr>"
    expected = "Prev cycle: 202301010600"
    assert parser.resolve_cyclestr(text, cycle) == expected

    # Test nested-like but flat multiple tags with different offsets
    text = "<cyclestr offset='-1:00:00'>@H</cyclestr> to <cyclestr>@H</cyclestr>"
    expected = "11 to 12"
    assert parser.resolve_cyclestr(text, cycle) == expected


@pytest.mark.asyncio
async def test_xml_system_entity_resolution(tmp_path):
    """Test resolution of XML SYSTEM entities, including nested entities."""
    base_dir = tmp_path / "workflow"
    base_dir.mkdir()

    inc_file = base_dir / "include.xml"
    inc_file.write_text("<!ENTITY nested 'nested_value'>")

    # Rocoto often uses SYSTEM entities to include task definitions
    tasks_file = base_dir / "tasks.inc"
    tasks_file.write_text("<task name='&task_name;'>&task_content;</task>")

    workflow_file = base_dir / "workflow.xml"
    workflow_content = """<?xml version="1.0"?>
<!DOCTYPE workflow [
  <!ENTITY % secret SYSTEM "include.xml">
  %secret;
  <!ENTITY task_name "my_task">
  <!ENTITY task_content "content_with_&nested;">
  <!ENTITY my_tasks SYSTEM "tasks.inc">
]>
<workflow>
  &my_tasks;
</workflow>"""
    workflow_file.write_text(workflow_content)

    parser = RocotoParser(str(workflow_file), "db")
    await parser.parse_workflow()

    assert "my_task" in parser.tasks_dict
    task = parser.tasks_dict["my_task"]
    assert task.name == "my_task"

    # Check if nested entity was resolved in the entity value itself
    assert parser.entity_values["task_content"] == "content_with_nested_value"


@pytest.mark.asyncio
async def test_xml_system_entity_missing_file(tmp_path):
    """Test robustness when a SYSTEM entity file is missing."""
    workflow_file = tmp_path / "workflow.xml"
    workflow_content = """<?xml version="1.0"?>
<!DOCTYPE workflow [
  <!ENTITY missing SYSTEM "nonexistent.inc">
]>
<workflow>
  <task name="t1">&missing;</task>
</workflow>"""
    workflow_file.write_text(workflow_content)

    parser = RocotoParser(str(workflow_file), "db")
    # Should not raise exception
    await parser.parse_workflow()
    assert "t1" in parser.tasks_dict
