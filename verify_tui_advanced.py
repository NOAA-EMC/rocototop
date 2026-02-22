import asyncio
import os
import sqlite3

from rocotoviewer.app import RocotoApp


async def take_screenshot():
    # Setup mock data
    wf = "tests/mock_workflow_advanced.xml"
    db = "tests/mock_rocoto_advanced.db"

    with open(wf, "w") as f:
        f.write("""<?xml version="1.0"?>
<workflow name="test">
  <cycledef group="default">202301010000 202301011200 06:00:00</cycledef>
  <task name="post_proc" cycledefs="default">
    <command>echo hello</command>
    <account>research</account>
    <dependency>
        <taskdep task="prev_task"/>
    </dependency>
  </task>
</workflow>""")

    if os.path.exists(db):
        os.remove(db)
    conn = sqlite3.connect(db)
    c = conn.cursor()
    c.execute("CREATE TABLE cycles (cycle INTEGER)")
    c.execute("INSERT INTO cycles VALUES (1672531200)")
    c.execute(
        "CREATE TABLE jobs (taskname TEXT, cycle INTEGER, state TEXT, "
        "exit_status INTEGER, duration INTEGER, tries INTEGER, jobid TEXT)"
    )
    c.execute(
        "INSERT INTO jobs VALUES ('post_proc', 1672531200, 'RUNNING', NULL, 3600, 1, "
        "'88888')"
    )
    conn.commit()
    conn.close()

    app = RocotoApp(workflow_file=wf, database_file=db)
    async with app.run_test() as pilot:
        await pilot.pause(1.0)  # Wait for refresh
        # Select row
        await pilot.press("down")
        await pilot.press("enter")
        await pilot.pause(0.5)

        # Take screenshot
        # Textual screenshots are SVG.
        app.save_screenshot("rocoto_advanced.svg")
        print("Screenshot saved to rocoto_advanced.svg")


if __name__ == "__main__":
    asyncio.run(take_screenshot())
