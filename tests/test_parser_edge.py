from rocotoviewer.parser import RocotoParser


def test_parser_missing_files():
    parser = RocotoParser("nonexistent.xml", "nonexistent.db")
    parser.parse_workflow()
    assert parser.tasks_ordered == []
    assert parser.get_status() == []

def test_parser_invalid_xml(tmp_path):
    wf = tmp_path / "invalid.xml"
    wf.write_text("invalid xml")
    parser = RocotoParser(str(wf), "db")
    parser.parse_workflow()
    assert parser.tasks_ordered == []

def test_parser_invalid_db(tmp_path):
    db = tmp_path / "invalid.db"
    db.write_text("not a sqlite db")
    parser = RocotoParser("wf", str(db))
    assert parser.get_status() == []

def test_parser_parse_cycle_edge_cases():
    parser = RocotoParser("wf", "db")
    # Invalid timestamp
    assert parser._parse_cycle("not an int") == "not an int"
    # Large int that is not a YYYYMMDDHHMM
    assert parser._parse_cycle(100) == "197001010001"
    # Negative int (invalid timestamp)
    assert parser._parse_cycle(-1) == "-1"

def test_entity_parsing(tmp_path):
    wf = tmp_path / "entities.xml"
    wf.write_text("""<?xml version="1.0"?>
<!DOCTYPE workflow [
  <!ENTITY TEST "value">
  <!ENTITY SYSTEM_TEST SYSTEM "some_file">
]>
<workflow>
</workflow>""")
    parser = RocotoParser(str(wf), "db")
    entities = parser._get_entity_values()
    assert entities["TEST"] == "value"
    assert entities["SYSTEM_TEST"] == "some_file"
