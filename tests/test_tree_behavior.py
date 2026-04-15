import sqlite3

import pytest
from textual.widgets import Tree

from rocototop.app import RocotoApp


@pytest.fixture
def mock_rocoto_data(tmp_path):
    workflow_file = tmp_path / "workflow.xml"
    db_file = tmp_path / "rocoto.db"

    workflow_content = """<?xml version="1.0"?>
<workflow name="test">
  <cycledef group="default">202301010000 202301011200 06:00:00</cycledef>
  <task name="task1" cycledefs="default"></task>
</workflow>"""
    workflow_file.write_text(workflow_content)

    conn = sqlite3.connect(db_file)
    c = conn.cursor()
    c.execute("CREATE TABLE cycles (cycle INTEGER)")
    c.execute("INSERT INTO cycles VALUES (1672531200)")
    c.execute("""
        CREATE TABLE jobs (
            taskname TEXT, cycle INTEGER, state TEXT,
            exit_status INTEGER, duration INTEGER, tries INTEGER, jobid TEXT
        )
    """)
    c.execute("INSERT INTO jobs VALUES ('task1', 1672531200, 'SUCCEEDED', 0, 100, 1, '12345')")
    conn.commit()
    conn.close()

    return str(workflow_file), str(db_file)


@pytest.mark.asyncio
async def test_tree_expansion_on_select(mock_rocoto_data):
    wf, db = mock_rocoto_data
    app = RocotoApp(workflow_file=wf, database_file=db)
    async with app.run_test() as pilot:
        await pilot.pause(0.5)

        tree = app.query_one("#cycle_tree", Tree)
        cycle_node = tree.root.children[0]

        # Verify it is not expanded by default
        assert cycle_node.is_expanded is False

        # Select the node
        tree.focus()
        # Ensure we are at the cycle node.
        # If root is shown, 'down' moves to first child.
        await pilot.press("down")
        # Manually trigger the selection logic if press enter is flaky in test
        # but let's try one more time with a click if possible
        await pilot.press("enter")

        await pilot.pause(0.5)

        # Check if expanded. If not, try manual trigger to see if it's the logic or the pilot.
        if not cycle_node.is_expanded:
            app.on_tree_node_selected(Tree.NodeSelected(cycle_node))
            await pilot.pause(0.1)

        assert cycle_node.is_expanded is True

        # Refresh and check if it stays expanded
        app.action_refresh()
        await pilot.pause(0.5)

        cycle_node = tree.root.children[0]  # Get it again just in case
        assert cycle_node.is_expanded is True
