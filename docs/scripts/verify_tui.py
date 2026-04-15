import asyncio
import sqlite3

from rocototop.app import RocotoApp


async def take_screenshot():
    # Setup mock data
    wf = "tests/mock_workflow.xml"
    db = "tests/mock_rocoto.db"

    with open(wf, "w") as f:
        f.write("""<?xml version="1.0"?>
<workflow name="test">
  <cycledef group="default">202301010000 202301011200 06:00:00</cycledef>
  <metatask name="ensemble">
    <var name="member">01 02</var>
    <task name="post_#member#" cycledefs="default"></task>
  </metatask>
</workflow>""")

    conn = sqlite3.connect(db)
    c = conn.cursor()
    c.execute("CREATE TABLE IF NOT EXISTS cycles (cycle INTEGER)")
    c.execute("DELETE FROM cycles")
    c.execute("INSERT INTO cycles VALUES (1672531200)")
    c.execute(
        "CREATE TABLE IF NOT EXISTS jobs (taskname TEXT, cycle INTEGER, "
        "state TEXT, exit_status INTEGER, duration INTEGER, tries INTEGER, "
        "jobid TEXT)"
    )
    c.execute("DELETE FROM jobs")
    c.execute("INSERT INTO jobs VALUES ('post_01', 1672531200, 'SUCCEEDED', 0, 100, 1, '12345')")
    c.execute("INSERT INTO jobs VALUES ('post_02', 1672531200, 'RUNNING', NULL, 50, 1, '12346')")
    conn.commit()
    conn.close()

    app = RocotoApp(workflow_file=wf, database_file=db)
    async with app.run_test() as pilot:
        await pilot.pause(1.0)  # Wait for refresh
        app.save_screenshot("tui_screenshot.svg")
        # Textual save_screenshot saves as SVG by default, but can do PNG if configured,
        # but here we just want to see if it runs and we can maybe convert it or just
        # trust the tests.
        # Actually, let's try to get a PNG if possible.
        # But Textual's screenshot is usually SVG.
        print("Screenshot saved to tui_screenshot.svg")


if __name__ == "__main__":
    asyncio.run(take_screenshot())
