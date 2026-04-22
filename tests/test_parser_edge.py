"""
.. note:: warning: "If you modify features, API, or usage, you MUST update the documentation immediately."
"""

import pytest

from rocototop.parser import RocotoParser


@pytest.mark.asyncio
async def test_parser_missing_files():
    parser = RocotoParser("nonexistent.xml", "nonexistent.db")
    await parser.parse_workflow()
    assert parser.tasks_ordered == []
    assert await parser.get_status() == []


@pytest.mark.asyncio
async def test_parser_invalid_xml(tmp_path):
    wf = tmp_path / "invalid.xml"
    wf.write_text("invalid xml")
    parser = RocotoParser(str(wf), "db")
    await parser.parse_workflow()
    assert parser.tasks_ordered == []


@pytest.mark.asyncio
async def test_parser_invalid_db(tmp_path):
    db = tmp_path / "invalid.db"
    db.write_text("not a sqlite db")
    parser = RocotoParser("wf", str(db))
    assert await parser.get_status() == []


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
    # Create the some_file so SYSTEM entity doesn't return empty string
    (tmp_path / "some_file").write_text("some_file_content")
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
    assert entities["SYSTEM_TEST"] == "some_file_content"


@pytest.mark.asyncio
async def test_parser_sqlite_error(caplog):
    import sqlite3
    from unittest.mock import patch

    parser = RocotoParser("wf", "db")
    # Make exists return True so it tries to connect
    with patch("asyncio.to_thread", return_value=True):
        with patch("aiosqlite.connect", side_effect=sqlite3.Error("Mocked database error")):
            status = await parser.get_status()
            assert status == []
            assert "Database error while fetching status: Mocked database error" in caplog.text


@pytest.mark.asyncio
async def test_parser_xml_parse_error(tmp_path, caplog):
    wf = tmp_path / "malformed.xml"
    wf.write_text("<workflow><task name='foo'>")  # Missing closing tags
    parser = RocotoParser(str(wf), "db")
    await parser.parse_workflow()
    assert parser.tasks_ordered == []
    assert "Failed to parse workflow XML" in caplog.text
