import pytest
from rocototop.parser import RocotoParser


def test_resolve_cyclestr():
    parser = RocotoParser("wf", "db")
    cycle = "202301010000"

    # Test basic
    assert parser.resolve_cyclestr("<cyclestr>@Y@m@d@H</cyclestr>", cycle) == "2023010100"

    # Test offset
    assert parser.resolve_cyclestr("<cyclestr offset='01:00:00'>@Y@m@d@H</cyclestr>", cycle) == "2023010101"
    assert parser.resolve_cyclestr("<cyclestr offset='-01:00:00'>@Y@m@d@H</cyclestr>", cycle) == "2022123123"

    # Test multiple
    text = "Log: <cyclestr>@Y@m@d</cyclestr> Cmd: <cyclestr offset='1:00'>@H</cyclestr>"
    assert parser.resolve_cyclestr(text, cycle) == "Log: 20230101 Cmd: 00"  # 1:00 is 1 min, so Hour remains 00
    assert parser.resolve_cyclestr("Hour: <cyclestr offset='01:00:00'>@H</cyclestr>", cycle) == "Hour: 01"


@pytest.mark.asyncio
async def test_entity_substitution_in_xml(tmp_path):
    wf = tmp_path / "wf.xml"
    wf.write_text("""<?xml version="1.0"?>
<!DOCTYPE workflow [
  <!ENTITY HOME "/path/to/home">
  <!ENTITY CMD "&HOME;/bin/run">
]>
<workflow>
  <task name="test">
    <command>&CMD;</command>
  </task>
</workflow>""")
    parser = RocotoParser(str(wf), "db")
    await parser.parse_workflow()
    assert parser.tasks_dict["test"].command == "/path/to/home/bin/run"
