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
        # Wait for data to load and UI to update
        await pilot.pause(2.0)

        # 1. Collapsed View
        os.makedirs("screenshots", exist_ok=True)
        app.save_screenshot("screenshots/collapsed.svg")
        print("Saved screenshots/collapsed.svg")

        # 2. Expanded View (All cycles expanded)
        tree = app.query_one("#cycle_tree")
        for node in tree.root.children:
            node.expand()
        tree.root.expand()

        await pilot.pause(1.0)
        app.save_screenshot("screenshots/overview.svg")
        print("Saved screenshots/overview.svg")

        # 3. Filtering Screenshot
        await pilot.click("#filter_input")
        await pilot.press(*"model")
        await pilot.pause(1.0)
        app.save_screenshot("screenshots/filtering.svg")
        print("Saved screenshots/filtering.svg")

        # Clear filter
        await pilot.click("#filter_input")
        app.query_one("#filter_input").value = ""
        await pilot.pause(0.5)

        # 4. Details and Log Screenshot
        await pilot.click("#status_table")
        await pilot.press("home")
        # Row 7 is run_model_A 12Z
        for _ in range(7):
            await pilot.press("down")
        await pilot.press("enter")

        await pilot.pause(0.5)
        details = app.last_selected_task
        print(f"Selected task: {details['task']}")
        print(f"Resolved stdout: {app.parser.resolve_cyclestr(details['details']['stdout'], app.last_selected_cycle)}")

        await pilot.press("l")
        await pilot.pause(2.0)  # Wait for log to be read

        # In Textual, RichLog might not have .lines but we can check its content via private or other means
        # but let's just save the screenshot and hope the pause worked.

        app.save_screenshot("screenshots/details_log.svg")
        print("Saved screenshots/details_log.svg")


if __name__ == "__main__":
    asyncio.run(generate_assets())
