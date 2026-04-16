# Tutorial: Monitoring Your First Workflow

This tutorial will walk you through a typical session using RocotoTop to monitor a running workflow.

## Scenario

Suppose you have a workflow named `TutorialWorkflow` that has been running for several hours. You want to check the status of the `12Z` cycle and see why a specific task failed.

## Step 1: Launch RocotoTop

Open your terminal and run RocotoTop pointing to your workflow files:

```bash
rocototop -w example_workflow.xml -d example_workflow.db
```

When the application starts, you will see the **Cycle Tree** on the left with all cycles initially collapsed.

![Initial Collapsed View](screenshots/collapsed.svg)

## Step 2: Navigate and Expand Cycles

The **Cycle Tree** allows you to navigate the hierarchy of your workflow.

*   **Keyboard**: Use the `Up` and `Down` arrow keys to highlight a cycle. Press `Enter` to **expand** or **collapse** the selected cycle.
*   **Mouse**: Click on a cycle name or the expansion icon next to it to toggle its state.

Expand the `202310271200` (the 12Z cycle). You will see the tasks associated with that cycle appear underneath it.

RocotoTop uses **icons** and **colors** to help you quickly identify task states:
*   ✅ **SUCCEEDED**: Task finished successfully.
*   🏃 **RUNNING**: Task is currently executing.
*   💀 **DEAD**: Task failed and will not be retried automatically.
*   🕒 **QUEUED**: Task is waiting for resources in the batch system.
*   ⌛ **WAITING / PENDING**: Task is waiting for dependencies to be met.

![Expanded 12Z Cycle](screenshots/overview.svg)

## Step 3: Filter for the Failed Task

In the main view, you see a list of tasks. You are looking for a task named `run_model_A`.

1.  Press `Tab` to move focus to the **Filter Input** box at the top (or click it with your mouse).
2.  Type `model`.

![Filtering Tasks](screenshots/filtering.svg)

The **Cycle Tree** will now only show tasks containing "model" in their name, making it easier to find what you need across all cycles.

## Step 4: Inspect Task Details

Find the `run_model_A` task for the `202310271200` cycle in the tree. You notice its state is `DEAD`.

*   **Select**: Click on the task in the tree or use arrow keys to highlight it.
*   **Observe**: The **Selected Task Status** table and the **Details** tab will populate with information specific to this task instance.

The Details Panel shows the **Resolved Command** and **Log Paths** (Stdout/Stderr). RocotoTop automatically resolves `<cyclestr>` tags based on the selected cycle.

## Step 5: View Live Logs

If a task is running or has failed, you often want to see the output.

1.  With the task selected, press `t` to switch to the **Log** tab.
2.  The tab will show a live `tail` of the log file.

![Log Viewer in Action](screenshots/details_log.svg)

You can see the "Segmentation fault" error right in the TUI!
*   Press `f` to toggle **Log Follow** mode (autoscroll to the bottom).

## Step 6: Explore Complex Dependencies

RocotoTop helps you visualize complex dependency logic. In our example, the `verify` task has a multi-part dependency:

```xml
<task name="verify" cycledefs="standard">
  <dependency>
    <and>
      <taskdep task="archive"/>
      <!-- Depends on archive from the PREVIOUS cycle -->
      <taskdep task="archive">
        <cycleoffset>-12:00:00</cycleoffset>
      </taskdep>
    </and>
  </dependency>
</task>
```

When you select the `verify` task, the **Details Panel** lists these dependencies, helping you understand why a task might be stuck in a `PENDING` state.

## Step 7: TUI Navigation Cheat Sheet

| Key | Action | Context |
| :--- | :--- | :--- |
| `Up/Down` | Move Selection | Cycle Tree |
| `Enter` | Expand/Collapse or Select | Cycle Tree |
| `Tab` | Switch Focus | Between Tree, Filter, and Tabs |
| `→`/`←` | Next/Previous Cycle | Cycle Tree |
| `x` | Expand/Collapse Metatask | Cycle Tree |
| `c` | Check Task (rocotocheck) | Global |
| `b` | Boot Task (rocotoboot) | Global |
| `r` | Rewind Task (rocotorewind) | Global |
| `R` | Run Workflow (rocotorun) | Global |
| `C` | Mark Complete | Global |
| `W` | Rewind Entire Cycle | Global |
| `l` | Reload Data | Global |
| `F` | Find Last Running Cycle | Global |
| `t` | Toggle between Details/Log | Global |
| `f` | Toggle Log Follow | While Log tab is active |
| `/` | Search Log | While Log tab is active |
| `n` / `N` | Next / Prev Match | While searching |
| `Escape` | Close Search | While searching |
| `h` | Help | Global |
| `q` / `Q` | Quit | Global |

## Step 8: Rewind and Monitor

After fixing the underlying issue (e.g., fixing a data issue that caused the Segfault), you can signal Rocoto to retry.

With `run_model_A` selected, press `r` to "Rewind" the task. RocotoTop will send the command to Rocoto. Wait for the auto-refresh (every 30s) or press `l` to reload data and see the state change to `QUEUED` or `RUNNING`.

To rewind an entire cycle at once, select the cycle node in the tree and press `W` (uppercase). RocotoTop will run `rocotorewind` for every task in that cycle and report the results. You can also press `R` to trigger `rocotorun` manually.

*(Note: In this demo version, actions show a notification in the UI).*
