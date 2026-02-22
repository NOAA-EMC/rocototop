"""
.. note:: warning: "If you modify features, API, or usage, you MUST update the documentation immediately."
"""
from __future__ import annotations

import collections
import os
import re
import sqlite3
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from typing import Any, Dict, List, Tuple, Optional

# Constants
DEFAULT_CYCLE = "default_cycle"
NO_NAME = "NO_NAME"
CYCLE_FORMAT = "%Y%m%d%H%M"


class RocotoTask:
    """
    Represents a task definition from the Rocoto XML.
    """
    def __init__(self, name: str, cycledefs: str):
        self.name = name
        self.cycledefs = cycledefs
        self.command: str = ""
        self.account: str = ""
        self.queue: str = ""
        self.walltime: str = ""
        self.memory: str = ""
        self.join: str = ""
        self.stdout: str = ""
        self.stderr: str = ""
        self.dependencies: List[Dict[str, Any]] = []

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "cycledefs": self.cycledefs,
            "command": self.command,
            "account": self.account,
            "queue": self.queue,
            "walltime": self.walltime,
            "memory": self.memory,
            "join": self.join,
            "stdout": self.stdout,
            "stderr": self.stderr,
            "dependencies": self.dependencies,
        }


class RocotoParser:
    """
    A parser for Rocoto workflow XML files and associated SQLite databases.
    """

    def __init__(self, workflow_file: str, database_file: str) -> None:
        self.workflow_file: str = workflow_file
        self.database_file: str = database_file
        self.entity_values: dict[str, str] = {}
        self.tasks_dict: dict[str, RocotoTask] = {}
        self.tasks_ordered: list[str] = []
        self.metatask_list: dict[str, list[str]] = collections.defaultdict(list)
        self.cycledef_group_cycles: dict[str, list[str]] = collections.defaultdict(list)

    def parse_workflow(self) -> None:
        """Parse the XML workflow file."""
        self.entity_values = self._get_entity_values()
        self._load_workflow_xml()

    def _get_entity_values(self) -> dict[str, str]:
        entity_values: dict[str, str] = collections.defaultdict(str)
        if not os.path.exists(self.workflow_file):
            return entity_values

        try:
            with open(self.workflow_file) as f:
                content = f.read()
                dtd_match = re.search(r"<!DOCTYPE workflow\s+\[(.*?)\]>", content, re.DOTALL)
                if dtd_match:
                    dtd_content = dtd_match.group(1)
                    entity_matches = re.finditer(r'<!ENTITY\s+(\w+)\s+(?:SYSTEM\s+)?["\']([^"\']*)["\']\s*>', dtd_content)
                    for match in entity_matches:
                        name, value = match.groups()
                        for k, v in entity_values.items():
                            value = value.replace(f"&{k};", v)
                        entity_values[name] = value
        except Exception:
            pass
        return entity_values

    def _load_workflow_xml(self) -> None:
        if not os.path.exists(self.workflow_file):
            return

        try:
            with open(self.workflow_file, 'r') as f:
                content = f.read()

            # Substitute entities
            for _ in range(3):
                changed = False
                for k, v in self.entity_values.items():
                    if f"&{k};" in content:
                        content = content.replace(f"&{k};", v)
                        changed = True
                if not changed:
                    break

            # Remove DOCTYPE properly
            content = re.sub(r'<!DOCTYPE workflow\s+\[.*?\]>', '', content, flags=re.DOTALL)
            content = re.sub(r'<!DOCTYPE.*?>', '', content, flags=re.DOTALL)

            root = ET.fromstring(content.strip())
        except Exception:
            # Re-initialize to clear any partial data if desired, but here we just return
            return

        self.tasks_dict = {}
        self.tasks_ordered = []
        self.metatask_list = collections.defaultdict(list)
        self.cycledef_group_cycles = collections.defaultdict(list)

        for child in root:
            if child.tag == "cycledef":
                self._parse_cycledef(child)
            elif child.tag == "task":
                self._add_task(child, {}, [])
            elif child.tag == "metatask":
                self._expand_metatask(child, {}, [])

    def _parse_cycledef(self, element: ET.Element) -> None:
        group = element.attrib.get("group", DEFAULT_CYCLE)
        if not element.text:
            return

        text = element.text.strip()
        parts = text.split()

        if len(parts) >= 3:
            try:
                start = datetime.strptime(parts[0], CYCLE_FORMAT)
                end = datetime.strptime(parts[1], CYCLE_FORMAT)
                inc_match = re.match(r"(\d+):(\d+)(?::(\d+))?", parts[2])
                if inc_match:
                    h, m, s = inc_match.groups()
                    inc = timedelta(hours=int(h), minutes=int(m), seconds=int(s or 0))
                    curr = start
                    while curr <= end:
                        self.cycledef_group_cycles[group].append(curr.strftime(CYCLE_FORMAT))
                        curr += inc
            except Exception:
                pass

    def _expand_metatask(self, element: ET.Element, current_vars: dict[str, str], parent_metatasks: list[str]) -> None:
        m_name = element.attrib.get("name", NO_NAME)

        vars_dict: dict[str, list[str]] = {}
        for var_elem in element.findall("var"):
            v_name = var_elem.attrib.get("name")
            if v_name and var_elem.text:
                vars_dict[v_name] = var_elem.text.split()

        if not vars_dict:
            new_parents = parent_metatasks + [m_name]
            for child in element:
                if child.tag == "task":
                    self._add_task(child, current_vars, new_parents)
                elif child.tag == "metatask":
                    self._expand_metatask(child, current_vars, new_parents)
            return

        num_values = len(next(iter(vars_dict.values())))
        for i in range(num_values):
            new_vars = current_vars.copy()
            for v_name, v_values in vars_dict.items():
                if i < len(v_values):
                    new_vars[v_name] = v_values[i]

            expanded_m_name = m_name
            for v_name, v_val in new_vars.items():
                expanded_m_name = expanded_m_name.replace(f"#{v_name}#", v_val)

            new_parents = parent_metatasks + [expanded_m_name]
            for child in element:
                if child.tag == "task":
                    self._add_task(child, new_vars, new_parents)
                elif child.tag == "metatask":
                    self._expand_metatask(child, new_vars, new_parents)

    def _add_task(self, element: ET.Element, vars_dict: dict[str, str], parent_metatasks: list[str]) -> None:
        name = element.attrib.get("name", NO_NAME)
        cycledefs = element.attrib.get("cycledefs", DEFAULT_CYCLE)

        for v_name, v_val in vars_dict.items():
            name = name.replace(f"#{v_name}#", v_val)
            cycledefs = cycledefs.replace(f"#{v_name}#", v_val)

        task = RocotoTask(name, cycledefs)

        for sub in element:
            if sub.tag == "command": task.command = (sub.text or "").strip()
            elif sub.tag == "account": task.account = (sub.text or "").strip()
            elif sub.tag == "queue": task.queue = (sub.text or "").strip()
            elif sub.tag == "walltime": task.walltime = (sub.text or "").strip()
            elif sub.tag == "memory": task.memory = (sub.text or "").strip()
            elif sub.tag == "join": task.join = (sub.text or "").strip()
            elif sub.tag == "stdout": task.stdout = (sub.text or "").strip()
            elif sub.tag == "stderr": task.stderr = (sub.text or "").strip()
            elif sub.tag == "dependency":
                task.dependencies = self._parse_deps(sub)

        self.tasks_dict[name] = task
        self.tasks_ordered.append(name)
        for p_name in parent_metatasks:
            self.metatask_list[p_name].append(name)

    def _parse_deps(self, element: ET.Element) -> List[Dict[str, Any]]:
        deps = []
        for child in element:
            dep = {"type": child.tag, "attrib": dict(child.attrib)}
            if child.tag in ["and", "or", "not", "nand", "nor", "xor", "some"]:
                dep["children"] = self._parse_deps(child)
            else:
                dep["text"] = (child.text or "").strip()
            deps.append(dep)
        return deps

    def resolve_cyclestr(self, text: str, cycle: str) -> str:
        try:
            dt = datetime.strptime(cycle, CYCLE_FORMAT)
        except Exception:
            return text

        def replace_cyclestr(match):
            full_tag = match.group(0)
            content = match.group(1)

            offset_attr = re.search(r'offset=["\'](.*?)["\']', full_tag)
            current_dt = dt
            if offset_attr:
                offset_str = offset_attr.group(1)
                negative = False
                if offset_str.startswith("-"):
                    negative = True
                    offset_str = offset_str[1:]

                parts = offset_str.split(":")
                delta = timedelta()
                try:
                    if len(parts) == 4:
                        delta = timedelta(days=int(parts[0]), hours=int(parts[1]), minutes=int(parts[2]), seconds=int(parts[3]))
                    elif len(parts) == 3:
                        delta = timedelta(hours=int(parts[0]), minutes=int(parts[1]), seconds=int(parts[2]))
                    elif len(parts) == 2:
                        delta = timedelta(minutes=int(parts[0]), seconds=int(parts[1]))
                    elif len(parts) == 1:
                        delta = timedelta(seconds=int(parts[0]))
                except ValueError:
                    pass

                if negative:
                    current_dt -= delta
                else:
                    current_dt += delta

            res = content
            flags = {
                "@Y": "%Y", "@y": "%y", "@m": "%m", "@d": "%d",
                "@H": "%H", "@I": "%I", "@M": "%M", "@S": "%S",
                "@p": "%p", "@j": "%j", "@A": "%A", "@a": "%a",
                "@B": "%B", "@b": "%b"
            }
            for flag, fmt in flags.items():
                res = res.replace(flag, current_dt.strftime(fmt))
            res = res.replace("@s", str(int(current_dt.timestamp())))
            return res

        return re.sub(r'<cyclestr(?:\s+[^>]*?)?>(.*?)</cyclestr>', replace_cyclestr, text, flags=re.DOTALL)

    def get_status(self) -> list[dict[str, Any]]:
        if not os.path.exists(self.database_file):
            return []

        try:
            connection = sqlite3.connect(self.database_file)
            connection.row_factory = sqlite3.Row
            c = connection.cursor()
            cycles_raw = [row["cycle"] for row in c.execute("SELECT cycle FROM cycles ORDER BY cycle ASC")]
            jobs_data = collections.defaultdict(dict)
            q = c.execute("SELECT taskname, cycle, state, exit_status, duration, tries, jobid FROM jobs")
            for row in q:
                jobs_data[row["cycle"]][row["taskname"]] = dict(row)
            connection.close()
        except sqlite3.Error:
            return []

        result: list[dict[str, Any]] = []
        for cycle_raw in cycles_raw:
            cycle_str = self._parse_cycle(cycle_raw)
            tasks_status = []

            # If tasks_ordered is empty, it means parse_workflow wasn't called or failed
            # For backward compatibility with tests, we can fall back to showing just DB tasks
            if not self.tasks_ordered:
                for tname, job in jobs_data.get(cycle_raw, {}).items():
                    tasks_status.append({
                        "task": tname,
                        "state": job["state"],
                        "exit": job["exit_status"],
                        "duration": job["duration"],
                        "tries": job["tries"],
                        "jobid": job["jobid"],
                        "details": {}
                    })
            else:
                for tname in self.tasks_ordered:
                    task_def = self.tasks_dict[tname]
                    if task_def.cycledefs != DEFAULT_CYCLE:
                        if cycle_str not in self.cycledef_group_cycles.get(task_def.cycledefs, []):
                            continue

                    job = jobs_data.get(cycle_raw, {}).get(tname)
                    task_info = {
                        "task": tname,
                        "state": job["state"] if job else "PENDING",
                        "exit": job["exit_status"] if job else None,
                        "duration": job["duration"] if job else None,
                        "tries": job["tries"] if job else 0,
                        "jobid": job["jobid"] if job else None,
                        "details": task_def.to_dict()
                    }
                    tasks_status.append(task_info)
            result.append({"cycle": cycle_str, "tasks": tasks_status})
        return result

    def _parse_cycle(self, cycle_val: Any) -> str:
        if isinstance(cycle_val, int):
            if cycle_val > 200000000000:
                return str(cycle_val)
            else:
                try:
                    if cycle_val >= 0:
                        return datetime.fromtimestamp(cycle_val).strftime(CYCLE_FORMAT)
                except Exception:
                    pass
        return str(cycle_val)
