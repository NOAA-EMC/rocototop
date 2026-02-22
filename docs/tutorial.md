# Tutorial: Monitoring Your First Workflow

This tutorial will walk you through a typical session using RocotoViewer to monitor a running workflow.

## Scenario

Suppose you have a workflow named `forecast_system` that has been running for several hours. You want to check the status of the `00Z` cycle and see why a specific task failed.

## Step 1: Launch RocotoViewer

Open your terminal and run RocotoViewer pointing to your workflow files:

```bash
rocotoviewer -w forecast.xml -d forecast.db
```

## Step 2: Navigate to the Cycle

In the sidebar on the left, you will see a list of cycles. Use your arrow keys or mouse to find the `202310270000` (the 00Z cycle) and select it.

## Step 3: Filter for the Failed Task

In the main view, you see a long list of tasks. You are looking for a task named `post_process`.
Click on the "Filter tasks by name..." input box at the top and type `post`.

The table will now only show tasks containing "post".

## Step 4: Inspect the Failure

Find the `post_process` task in the table. You notice its state is `DEAD` and the exit status is `1`.
Click on that row to select it.

## Step 5: Check the Logs

Look at the Details Panel at the bottom. It shows the resolved path for `Stdout` and `Stderr`.
You can now copy this path and use another terminal to `tail` or `cat` the log file to debug the issue.

## Step 6: Rewind and Retry

After fixing the underlying issue, you want to retry the task.
With the `post_process` task still selected, press `w` to "Rewind" the task.

*(Note: Currently, Rocoto actions are placeholders and will show a notification in the UI).*

## Step 7: Refresh and Monitor

Press `r` to manually refresh and verify that the task state has changed (e.g., to `QUEUED` or `RUNNING`).
Alternatively, just wait for the auto-refresh to kick in!
