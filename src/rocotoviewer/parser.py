# .. note:: warning: "If you modify features, API, or usage, you MUST update the documentation immediately."

"""
Parser for Rocoto workflow XML files and SQLite databases.
"""

from __future__ import annotations

import logging
import os
import re
import sqlite3
import xml.etree.ElementTree as ET
from collections import defaultdict
from collections.abc import Callable
from datetime import datetime, timedelta
from typing import Any, TypedDict


class TaskDetails(TypedDict):
    """
    TypedDict representing the static definition of a task.

    Attributes
    ----------
    name : str
        The name of the task.
    cycledefs : str
        The cycle definitions associated with the task.
    command : str
        The command to execute.
    account : str
        The account to use for the job.
    queue : str
        The queue to submit the job to.
    walltime : str
        The walltime limit for the job.
    memory : str
        The memory limit for the job.
    join : str
        The path to the joined stdout/stderr log.
    stdout : str
        The path to the stdout log.
    stderr : str
        The path to the stderr log.
    dependencies : list[dict[str, Any]]
        The list of task dependencies.
    """

    name: str
    cycledefs: str
    command: str
    account: str
    queue: str
    walltime: str
    memory: str
    join: str
    stdout: str
    stderr: str
    envars: dict[str, str]
    dependencies: list[dict[str, Any]]


class TaskStatus(TypedDict):
    """
    TypedDict representing the status of a single task.

    Attributes
    ----------
    task : str
        The name of the task.
    state : str
        The current state of the task (e.g., SUCCEEDED, RUNNING).
    exit : int | None
        The exit status of the task.
    duration : int | None
        The duration of the task in seconds.
    tries : int
        The number of times the task has been tried.
    jobid : str | None
        The job ID assigned by the scheduler.
    details : TaskDetails | dict[str, Any]
        Additional task definitions from the XML.
    """

    task: str
    state: str
    exit: int | None
    duration: int | None
    tries: int
    jobid: str | None
    details: TaskDetails | dict[str, Any]


class CycleStatus(TypedDict):
    """
    TypedDict representing the status of a cycle and its tasks.

    Attributes
    ----------
    cycle : str
        The formatted cycle string (YYYYMMDDHHMM).
    tasks : list[TaskStatus]
        The list of task statuses for this cycle.
    """

    cycle: str
    tasks: list[TaskStatus]


# Set up logging
logger = logging.getLogger(__name__)

# Constants
DEFAULT_CYCLE = "default_cycle"
NO_NAME = "NO_NAME"
CYCLE_FORMAT = "%Y%m%d%H%M"
ENTITY_RECURSION_LIMIT = 3
CYCLE_TIMESTAMP_THRESHOLD = 200000000000


class RocotoTask:
    """
    Represents a task definition from the Rocoto XML.

    Attributes
    ----------
    name : str
        The name of the task.
    cycledefs : str
        The cycle definitions associated with the task.
    command : str
        The command to execute.
    account : str
        The account to use for the job.
    queue : str
        The queue to submit the job to.
    walltime : str
        The walltime limit for the job.
    memory : str
        The memory limit for the job.
    join : str
        The path to the joined stdout/stderr log.
    stdout : str
        The path to the stdout log.
    stderr : str
        The path to the stderr log.
    dependencies : list[dict[str, Any]]
        The list of task dependencies.
    """

    def __init__(self, name: str, cycledefs: str) -> None:
        """
        Initialize a RocotoTask.

        Parameters
        ----------
        name : str
            The name of the task.
        cycledefs : str
            The cycle definitions associated with the task.
        """
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
        self.envars: dict[str, str] = {}
        self.dependencies: list[dict[str, Any]] = []

    def to_dict(self) -> TaskDetails:
        """
        Convert the RocotoTask to a dictionary.

        Returns
        -------
        TaskDetails
            A dictionary representation of the task.
        """
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
            "envars": self.envars,
            "dependencies": self.dependencies,
        }


class RocotoParser:
    """
    A parser for Rocoto workflow XML files and associated SQLite databases.

    Attributes
    ----------
    workflow_file : str
        Path to the Rocoto workflow XML file.
    database_file : str
        Path to the Rocoto SQLite database file.
    entity_values : dict[str, str]
        Dictionary of XML entity values.
    tasks_dict : dict[str, RocotoTask]
        Dictionary mapping task names to RocotoTask objects.
    tasks_ordered : list[str]
        List of task names in the order they appear in the XML.
    metatask_list : dict[str, list[str]]
        Dictionary mapping metatask names to their child task names.
    cycledef_group_cycles : dict[str, list[str]]
        Dictionary mapping cycledef groups to their lists of cycles.
    _last_parsed_mtime : float | None
        The modification time of the XML file when it was last parsed.
    """

    def __init__(self, workflow_file: str, database_file: str) -> None:
        """
        Initialize the RocotoParser.

        Parameters
        ----------
        workflow_file : str
            Path to the Rocoto workflow XML file.
        database_file : str
            Path to the Rocoto SQLite database file.
        """
        self.workflow_file: str = workflow_file
        self.database_file: str = database_file
        self.entity_values: dict[str, str] = {}
        self.tasks_dict: dict[str, RocotoTask] = {}
        self.tasks_ordered: list[str] = []
        self.metatask_list: dict[str, list[str]] = defaultdict(list)
        self.cycledef_group_cycles: dict[str, list[str]] = defaultdict(list)
        self._last_parsed_mtime: float | None = None

    def parse_workflow(self) -> None:
        """
        Parse the XML workflow file.

        This method reads the workflow file, extracts entity values,
        and then loads the XML structure while performing entity
        and variable substitution.

        Returns
        -------
        None
        """
        if not os.path.exists(self.workflow_file):
            return

        try:
            mtime = os.path.getmtime(self.workflow_file)
            if self._last_parsed_mtime is not None and mtime <= self._last_parsed_mtime:
                return

            with open(self.workflow_file, encoding="utf-8") as f:
                content = f.read()
            self._last_parsed_mtime = mtime
        except OSError as e:
            logger.error("Failed to read workflow XML file: %s", e)
            return

        self.entity_values = self._get_entity_values(content)
        self._load_workflow_xml(content)

    def _get_entity_values(self, content: str) -> dict[str, str]:
        """
        Extract XML entity values from the workflow file's DTD.

        Parameters
        ----------
        content : str
            The content of the workflow file.

        Returns
        -------
        dict[str, str]
            A dictionary mapping entity names to their values.
        """
        entity_values: dict[str, str] = defaultdict(str)

        dtd_match = re.search(r"<!DOCTYPE workflow\s+\[(.*?)\]>", content, re.DOTALL)
        if dtd_match:
            dtd_content = dtd_match.group(1)
            entity_matches = re.finditer(
                r'<!ENTITY\s+(\w+)\s+(SYSTEM\s+)?["\']([^"\']*)["\']\s*>',
                dtd_content,
            )
            for match in entity_matches:
                name, system, value = match.groups()
                if system:
                    # Resolve path relative to workflow file
                    base_dir = os.path.dirname(os.path.abspath(self.workflow_file))
                    abs_path = os.path.join(base_dir, value)
                    if os.path.exists(abs_path):
                        try:
                            with open(abs_path, encoding="utf-8") as f:
                                value = f.read()
                        except OSError as e:
                            logger.error("Failed to read SYSTEM entity file %s: %s", abs_path, e)
                            value = ""
                    else:
                        logger.warning("SYSTEM entity file not found: %s", abs_path)
                        value = ""

                for k, v in entity_values.items():
                    value = value.replace(f"&{k};", v)
                entity_values[name] = value
        return entity_values

    def _load_workflow_xml(self, content: str) -> None:
        """
        Load and parse the workflow XML after entity substitution.

        Parameters
        ----------
        content : str
            The content of the workflow file.

        Returns
        -------
        None
        """
        try:
            # Substitute entities
            for _ in range(ENTITY_RECURSION_LIMIT):
                changed = False
                for k, v in self.entity_values.items():
                    if f"&{k};" in content:
                        content = content.replace(f"&{k};", v)
                        changed = True
                if not changed:
                    break

            # Remove DOCTYPE properly
            content = re.sub(r"<!DOCTYPE workflow\s+\[.*?\]>", "", content, flags=re.DOTALL)
            content = re.sub(r"<!DOCTYPE.*?>", "", content, flags=re.DOTALL)

            root = ET.fromstring(content.strip())
        except ET.ParseError as e:
            logger.error("Failed to parse workflow XML: %s", e)
            return

        self.tasks_dict = {}
        self.tasks_ordered = []
        self.metatask_list = defaultdict(list)
        self.cycledef_group_cycles = defaultdict(list)

        for child in root:
            if child.tag == "cycledef":
                self._parse_cycledef(child)
            elif child.tag == "task":
                self._add_task(child, {}, [])
            elif child.tag == "metatask":
                self._expand_metatask(child, {}, [])
            elif child.tag == "tasks":
                self._process_tasks_tag(child, {}, [])

    def _parse_cycledef(self, element: ET.Element) -> None:
        """
        Parse a <cycledef> element and populate cycledef_group_cycles.

        Parameters
        ----------
        element : ET.Element
            The cycledef XML element.

        Returns
        -------
        None
        """
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
            except ValueError as e:
                logger.warning("Failed to parse cycledef text '%s': %s", text, e)

    def _process_tasks_tag(
        self,
        element: ET.Element,
        current_vars: dict[str, str],
        parent_metatasks: list[str],
    ) -> None:
        """
        Process a <tasks> grouping element.

        Parameters
        ----------
        element : ET.Element
            The tasks XML element.
        current_vars : dict[str, str]
            Current variable substitutions.
        parent_metatasks : list[str]
            List of parent metatask names.

        Returns
        -------
        None
        """
        for child in element:
            if child.tag == "task":
                self._add_task(child, current_vars, parent_metatasks)
            elif child.tag == "metatask":
                self._expand_metatask(child, current_vars, parent_metatasks)
            elif child.tag == "tasks":
                self._process_tasks_tag(child, current_vars, parent_metatasks)

    def _expand_metatask(
        self,
        element: ET.Element,
        current_vars: dict[str, str],
        parent_metatasks: list[str],
    ) -> None:
        """
        Recursively expand <metatask> elements.

        Parameters
        ----------
        element : ET.Element
            The metatask XML element.
        current_vars : dict[str, str]
            Current variable substitutions.
        parent_metatasks : list[str]
            List of parent metatask names for hierarchical tracking.

        Returns
        -------
        None
        """
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
                elif child.tag == "tasks":
                    self._process_tasks_tag(child, current_vars, new_parents)
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
                elif child.tag == "tasks":
                    self._process_tasks_tag(child, new_vars, new_parents)

    def _add_task(
        self,
        element: ET.Element,
        vars_dict: dict[str, str],
        parent_metatasks: list[str],
    ) -> None:
        """
        Parse and add a <task> definition.

        Parameters
        ----------
        element : ET.Element
            The task XML element.
        vars_dict : dict[str, str]
            Current variable substitutions.
        parent_metatasks : list[str]
            List of parent metatask names.

        Returns
        -------
        None
        """
        name = element.attrib.get("name", NO_NAME)
        cycledefs = element.attrib.get("cycledefs", DEFAULT_CYCLE)

        def resolve_vars(text: str) -> str:
            if not text:
                return text
            for v_name, v_val in vars_dict.items():
                text = text.replace(f"#{v_name}#", v_val)
            return text

        name = resolve_vars(name)
        cycledefs = resolve_vars(cycledefs)

        task = RocotoTask(name, cycledefs)

        def get_content(elem: ET.Element) -> str:
            # Reconstruct the inner XML content including tags like <cyclestr>
            content = elem.text or ""
            for child in elem:
                content += ET.tostring(child, encoding="unicode")
            return content.strip()

        for sub in element:
            if sub.tag == "command":
                task.command = resolve_vars(get_content(sub))
            elif sub.tag == "account":
                task.account = resolve_vars(get_content(sub))
            elif sub.tag == "queue":
                task.queue = resolve_vars(get_content(sub))
            elif sub.tag == "walltime":
                task.walltime = resolve_vars(get_content(sub))
            elif sub.tag == "memory":
                task.memory = resolve_vars(get_content(sub))
            elif sub.tag == "join":
                task.join = resolve_vars(get_content(sub))
            elif sub.tag == "stdout":
                task.stdout = resolve_vars(get_content(sub))
            elif sub.tag == "stderr":
                task.stderr = resolve_vars(get_content(sub))
            elif sub.tag == "envar":
                name_elem = sub.find("name")
                val_elem = sub.find("value")
                if name_elem is not None and val_elem is not None:
                    # Envars can also contain <cyclestr> tags, but get_content handles them
                    v_name = resolve_vars(get_content(name_elem))
                    v_val = resolve_vars(get_content(val_elem))
                    task.envars[v_name] = v_val
            elif sub.tag == "dependency":
                task.dependencies = self._parse_deps_with_vars(sub, resolve_vars)

        self.tasks_dict[name] = task
        self.tasks_ordered.append(name)
        for p_name in parent_metatasks:
            self.metatask_list[p_name].append(name)

    def _parse_deps_with_vars(self, element: ET.Element, resolve_vars: Callable[[str], str]) -> list[dict[str, Any]]:
        """
        Parse task dependencies recursively, resolving variables.

        Parameters
        ----------
        element : ET.Element
            The dependency XML element.
        resolve_vars : Callable[[str], str]
            Function to resolve variables in strings.

        Returns
        -------
        list[dict[str, Any]]
            A list of dependency dictionaries.
        """
        deps = []
        for child in element:
            attrib = {k: resolve_vars(v) for k, v in child.attrib.items()}
            dep = {"type": child.tag, "attrib": attrib}
            if child.tag in ["and", "or", "not", "nand", "nor", "xor", "some"]:
                dep["children"] = self._parse_deps_with_vars(child, resolve_vars)
            else:
                dep["text"] = resolve_vars((child.text or "").strip())
            deps.append(dep)
        return deps

    def resolve_cyclestr(self, text: str, cycle: str) -> str:
        """
        Resolve Rocoto <cyclestr> tags in a string.

        Parameters
        ----------
        text : str
            The string containing <cyclestr> tags.
        cycle : str
            The cycle string to use for resolution.

        Returns
        -------
        str
            The resolved string.
        """
        try:
            dt = datetime.strptime(cycle, CYCLE_FORMAT)
        except ValueError:
            return text

        def replace_cyclestr(match: re.Match) -> str:
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
                        delta = timedelta(
                            days=int(parts[0]),
                            hours=int(parts[1]),
                            minutes=int(parts[2]),
                            seconds=int(parts[3]),
                        )
                    elif len(parts) == 3:
                        delta = timedelta(
                            hours=int(parts[0]),
                            minutes=int(parts[1]),
                            seconds=int(parts[2]),
                        )
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
                "@Y": "%Y",
                "@y": "%y",
                "@m": "%m",
                "@d": "%d",
                "@H": "%H",
                "@I": "%I",
                "@M": "%M",
                "@S": "%S",
                "@p": "%p",
                "@j": "%j",
                "@A": "%A",
                "@a": "%a",
                "@B": "%B",
                "@b": "%b",
            }
            for flag, fmt in flags.items():
                res = res.replace(flag, current_dt.strftime(fmt))
            res = res.replace("@s", str(int(current_dt.timestamp())))
            return res

        return re.sub(
            r"<cyclestr(?:\s+[^>]*?)?>(.*?)</cyclestr>",
            replace_cyclestr,
            text,
            flags=re.DOTALL,
        )

    def get_summary(self, status_data: list[CycleStatus]) -> dict[str, int]:
        """
        Get a summary of task states across all cycles.

        Parameters
        ----------
        status_data : list[CycleStatus]
            The list of cycle status information.

        Returns
        -------
        dict[str, int]
            A dictionary mapping task states to their counts.
        """
        summary: dict[str, int] = defaultdict(int)
        for cycle in status_data:
            for task in cycle["tasks"]:
                summary[task["state"]] += 1
        return dict(summary)

    def get_status(self) -> list[CycleStatus]:
        """
        Query the SQLite database for the status of tasks and cycles.

        Returns
        -------
        list[CycleStatus]
            A list of cycle-task status information.
        """
        if not os.path.exists(self.database_file):
            return []

        try:
            connection = sqlite3.connect(self.database_file)
            connection.row_factory = sqlite3.Row
            c = connection.cursor()
            cycles_raw = [row["cycle"] for row in c.execute("SELECT cycle FROM cycles ORDER BY cycle ASC")]
            jobs_data = defaultdict(dict)
            q = c.execute(
                "SELECT taskname, cycle, state, exit_status, duration, tries, jobid FROM jobs",
            )
            for row in q:
                jobs_data[row["cycle"]][row["taskname"]] = dict(row)
            connection.close()
        except sqlite3.Error as e:
            logger.error("Database error while fetching status: %s", e)
            return []

        result: list[CycleStatus] = []
        for cycle_raw in cycles_raw:
            cycle_str = self._parse_cycle(cycle_raw)
            tasks_status = []

            # Determine tasks defined for this cycle in the XML
            xml_tasks_for_cycle = set()
            if self.tasks_ordered:
                for tname in self.tasks_ordered:
                    task_def = self.tasks_dict[tname]
                    if task_def.cycledefs == DEFAULT_CYCLE:
                        xml_tasks_for_cycle.add(tname)
                    else:
                        groups = [g.strip() for g in task_def.cycledefs.split(",")]
                        for group in groups:
                            if cycle_str in self.cycledef_group_cycles.get(group, []):
                                xml_tasks_for_cycle.add(tname)
                                break

            # Get names of all tasks that have job records in the DB for this cycle
            db_tasks_for_cycle = set(jobs_data.get(cycle_raw, {}).keys())

            # The set of tasks to show is the union of what's in the XML for this cycle
            # AND anything that actually has a record in the database for this cycle.
            all_task_names = xml_tasks_for_cycle | db_tasks_for_cycle

            if not self.tasks_ordered:
                # Fallback if XML hasn't been parsed: just show what's in the DB
                ordered_names = sorted(list(db_tasks_for_cycle))
            else:
                # Preserve XML order for tasks that exist in XML, 
                # then append any DB-only tasks at the end.
                ordered_names = [t for t in self.tasks_ordered if t in all_task_names]
                db_only = sorted(list(db_tasks_for_cycle - set(self.tasks_ordered)))
                ordered_names.extend(db_only)

            for tname in ordered_names:
                task_def = self.tasks_dict.get(tname)
                job = jobs_data.get(cycle_raw, {}).get(tname)
                
                task_info = {
                    "task": tname,
                    "state": job["state"] if job else "WAITING",
                    "exit": job["exit_status"] if job else None,
                    "duration": job["duration"] if job else None,
                    "tries": job["tries"] if job else 0,
                    "jobid": job["jobid"] if job else None,
                    "details": task_def.to_dict() if task_def else {},
                }
                tasks_status.append(task_info)

            result.append({"cycle": cycle_str, "tasks": tasks_status})
        return result

    def _parse_cycle(self, cycle_val: int | str | None) -> str:
        """
        Parse a cycle value (timestamp or string) into YYYYMMDDHHMM format.

        Parameters
        ----------
        cycle_val : int | str | None
            The cycle value to parse.

        Returns
        -------
        str
            The formatted cycle string.
        """
        if isinstance(cycle_val, int):
            if cycle_val > CYCLE_TIMESTAMP_THRESHOLD:
                return str(cycle_val)
            else:
                try:
                    if cycle_val >= 0:
                        return datetime.fromtimestamp(cycle_val).strftime(CYCLE_FORMAT)
                except (ValueError, OSError) as e:
                    logger.warning("Failed to parse cycle timestamp %d: %s", cycle_val, e)
        return str(cycle_val) if cycle_val is not None else ""
