import sys
from unittest.mock import patch
import pytest
from rocotoviewer.cli import main

def test_cli_help():
    with patch.object(sys, 'argv', ['rocotoviewer', '--help']):
        with pytest.raises(SystemExit) as e:
            main()
        assert e.value.code == 0

def test_cli_missing_args():
    with patch.object(sys, 'argv', ['rocotoviewer']):
        with pytest.raises(SystemExit) as e:
            main()
        assert e.value.code != 0

def test_cli_nonexistent_files():
    with patch.object(sys, 'argv', ['rocotoviewer', '-w', 'no.xml', '-d', 'no.db']):
        with pytest.raises(SystemExit) as e:
            main()
        assert e.value.code == 1
