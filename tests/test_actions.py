from unittest.mock import AsyncMock, patch

import pytest

from rocototop.app import RocotoApp


@pytest.fixture
def mock_app(tmp_path):
    wf = tmp_path / "wf.xml"
    db = tmp_path / "db.db"
    wf.write_text("<workflow></workflow>")
    db.write_text("")
    app = RocotoApp(str(wf), str(db))
    app.last_selected_task = {"task": "task1"}
    app.last_selected_cycle = "202301010000"
    return app


@pytest.mark.asyncio
async def test_action_boot_calls_subprocess(mock_app):
    async with mock_app.run_test() as pilot:
        # Clear any calls from initial refresh
        with patch("asyncio.create_subprocess_exec") as mock_exec:
            mock_process = AsyncMock()
            mock_process.communicate.return_value = (b"", b"")
            mock_process.returncode = 0
            mock_exec.return_value = mock_process

            mock_app.action_boot()
            await pilot.pause(0.2)

            assert mock_exec.call_count >= 1
            args = mock_exec.call_args[0]
            assert args[0] == "rocotoboot"
            assert "-w" in args
            assert "-d" in args
            assert "-c" in args
            assert "202301010000" in args
            assert "-t" in args
            assert "task1" in args


@pytest.mark.asyncio
async def test_action_rewind_calls_subprocess(mock_app):
    async with mock_app.run_test() as pilot:
        with patch("asyncio.create_subprocess_exec") as mock_exec:
            mock_process = AsyncMock()
            mock_process.communicate.return_value = (b"", b"")
            mock_process.returncode = 0
            mock_exec.return_value = mock_process

            mock_app.action_rewind()
            await pilot.pause(0.2)

            assert mock_exec.call_count >= 1
            # Check if ANY of the calls were 'rocotorewind'
            assert any(call[0][0] == "rocotorewind" for call in mock_exec.call_args_list)


@pytest.mark.asyncio
async def test_action_complete_calls_subprocess(mock_app):
    async with mock_app.run_test() as pilot:
        with patch("asyncio.create_subprocess_exec") as mock_exec:
            mock_process = AsyncMock()
            mock_process.communicate.return_value = (b"", b"")
            mock_process.returncode = 0
            mock_exec.return_value = mock_process

            mock_app.action_complete()
            await pilot.pause(0.2)

            assert mock_exec.call_count >= 1
            # Check if ANY of the calls were 'rocotocomplete'
            assert any(call[0][0] == "rocotocomplete" for call in mock_exec.call_args_list)


@pytest.mark.asyncio
async def test_action_command_not_found(mock_app):
    async with mock_app.run_test() as pilot:
        with patch("asyncio.create_subprocess_exec", side_effect=FileNotFoundError):
            with patch.object(mock_app, "notify") as mock_notify:
                mock_app.action_boot()
                await pilot.pause(0.2)
                assert mock_notify.called
                # Look for failure notification
                assert any("Command not found" in str(call) for call in mock_notify.call_args_list)
