import asyncio
import os

# Add src to path if needed, but assuming installed in editable mode
from rocotoviewer.app import RocotoApp


async def generate_assets():
    # Change directory to docs so relative paths in XML resolve correctly
    if os.path.basename(os.getcwd()) != "docs":
        os.chdir("docs")

    wf = "example_workflow.xml"
    db = "example_workflow.db"

    if not os.path.exists(wf) or not os.path.exists(db):
        print("Error: example_workflow.xml or .db not found in current directory.")
        return

    app = RocotoApp(workflow_file=wf, database_file=db)
    async with app.run_test() as pilot:
        await pilot.pause(1.0)

        # 1. Collapsed View
        os.makedirs("screenshots", exist_ok=True)
        app.save_screenshot("screenshots/collapsed.svg")
        print("Saved screenshots/collapsed.svg")

        # 2. Expanded View (12Z cycle expanded)
        await pilot.press("down")
        await pilot.press("down")
        await pilot.press("enter")
        await pilot.pause(0.5)
        app.save_screenshot("screenshots/overview.svg")
        print("Saved screenshots/overview.svg")

        # 3. Filtering Screenshot
        await pilot.click("#filter_input")
        await pilot.press(*"model")
        await pilot.pause(0.5)
        app.save_screenshot("screenshots/filtering.svg")
        print("Saved screenshots/filtering.svg")

        # Clear filter
        await pilot.click("#filter_input")
        for _ in range(5):
            await pilot.press("backspace")
        await pilot.pause(0.2)

        # 4. Details and Log Screenshot
        await pilot.click("#status_table")
        await pilot.press("home")
        # Cycle 00Z has 6 tasks.
        # Cycle 12Z ingest is 7th row.
        # Cycle 12Z run_model_A is 8th row (index 7).
        for _ in range(7):
            await pilot.press("down")
        await pilot.press("enter")

        await pilot.press("l")
        await pilot.pause(0.5)
        app.save_screenshot("screenshots/details_log.svg")
        print("Saved screenshots/details_log.svg")


if __name__ == "__main__":
    asyncio.run(generate_assets())
