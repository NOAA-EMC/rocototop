import os
import sqlite3

# Run from docs directory
db = "example_workflow.db"
if os.path.exists(db):
    os.remove(db)

conn = sqlite3.connect(db)
c = conn.cursor()
c.execute("CREATE TABLE cycles (cycle INTEGER)")
# 202310270000 -> 1698364800
# 202310271200 -> 1698408000
c.execute("INSERT INTO cycles VALUES (1698364800)")
c.execute("INSERT INTO cycles VALUES (1698408000)")

c.execute(
    "CREATE TABLE jobs (taskname TEXT, cycle INTEGER, state TEXT, exit_status INTEGER, duration INTEGER, tries INTEGER, jobid TEXT)"
)

# Cycle 202310270000: All Succeeded
tasks_00 = [
    ("ingest", "SUCCEEDED", 0, 120, 1, "1001"),
    ("run_model_A", "SUCCEEDED", 0, 3600, 1, "1002"),
    ("run_model_B", "SUCCEEDED", 0, 3500, 1, "1003"),
    ("post_process", "SUCCEEDED", 0, 600, 1, "1004"),
    ("archive", "SUCCEEDED", 0, 300, 1, "1005"),
    ("verify", "SUCCEEDED", 0, 100, 1, "1006"),
]
for t in tasks_00:
    c.execute("INSERT INTO jobs VALUES (?, 1698364800, ?, ?, ?, ?, ?)", t)

# Cycle 202310271200: Mixed
tasks_12 = [
    ("ingest", "SUCCEEDED", 0, 130, 1, "2001"),
    ("run_model_A", "DEAD", 1, 1500, 1, "2002"),
    ("run_model_B", "RUNNING", None, 1200, 1, "2003"),
    ("verify", "PENDING", None, 0, 0, None),
]
for t in tasks_12:
    c.execute("INSERT INTO jobs VALUES (?, 1698408000, ?, ?, ?, ?, ?)", t)

conn.commit()
conn.close()

# Create dummy log files
os.makedirs("logs", exist_ok=True)
with open("logs/model_A_2023102712.log", "w") as f:
    f.write("Starting model A...\n")
    f.write("Loading data...\n")
    f.write("ERROR: Segmentation fault in core solver.\n")
    f.write("Aborting.\n")
