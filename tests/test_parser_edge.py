"""
.. note:: warning: "If you modify features, API, or usage, you MUST update the documentation immediately."
"""

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
    entities = parser._get_entity_values(wf.read_text())
    assert entities["TEST"] == "value"
    assert entities["SYSTEM_TEST"] == "some_file"


def test_parser_sqlite_error(caplog):
    import sqlite3
    from unittest.mock import patch

    parser = RocotoParser("wf", "db")
    # Make exists return True so it tries to connect
    with patch("os.path.exists", return_value=True):
        with patch("sqlite3.connect", side_effect=sqlite3.Error("Mocked database error")):
            status = parser.get_status()
            assert status == []
            assert "Database error while fetching status: Mocked database error" in caplog.text


def test_parser_xml_parse_error(tmp_path, caplog):
    wf = tmp_path / "malformed.xml"
    wf.write_text("<workflow><task name='foo'>")  # Missing closing tags
    parser = RocotoParser(str(wf), "db")
    parser.parse_workflow()
    assert parser.tasks_ordered == []
    assert "Failed to parse workflow XML" in caplog.text
