import sys
from unittest.mock import patch

import pytest

from rocotoviewer.cli import main


def test_cli_help():
    with patch.object(sys, "argv", ["rocotoviewer", "--help"]):
        with pytest.raises(SystemExit) as e:
            main()
        assert e.value.code == 0


def test_cli_missing_args():
    with patch.object(sys, "argv", ["rocotoviewer"]):
        with pytest.raises(SystemExit) as e:
            main()
        assert e.value.code != 0


def test_cli_nonexistent_files():
    with patch.object(sys, "argv", ["rocotoviewer", "-w", "no.xml", "-d", "no.db"]):
        with pytest.raises(SystemExit) as e:
            main()
        assert e.value.code == 1


def test_cli_interval_parsing(tmp_path):
    wf = tmp_path / "wf.xml"
    db = tmp_path / "db.db"
    wf.write_text("<workflow></workflow>")
    db.write_text("")

    with patch("rocotoviewer.cli.RocotoApp") as mock_app:
        # Mock app.run() to avoid starting the TUI
        mock_app.return_value.run.return_value = None
        with patch.object(sys, "argv", ["rocotoviewer", "-w", str(wf), "-d", str(db), "-i", "45"]):
            main()
            mock_app.assert_called_once()
            _, kwargs = mock_app.call_args
            assert kwargs["refresh_interval"] == 45
