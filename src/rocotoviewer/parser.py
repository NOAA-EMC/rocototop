from __future__ import annotations
import os
import sqlite3
import collections
from datetime import datetime, timedelta
import re
import xml.etree.ElementTree as ET

class RocotoParser:
    def __init__(self, workflow_file: str, database_file: str):
        self.workflow_file = workflow_file
        self.database_file = database_file
        self.entity_values = {}
        self.tasks_ordered = []
        self.metatask_list = collections.defaultdict(list)
        self.cycledef_group_cycles = collections.defaultdict(list)

    def parse_workflow(self):
        """Parse the XML workflow file. Heavy operation."""
        self.entity_values = self._get_entity_values()
        self.tasks_ordered, self.metatask_list, self.cycledef_group_cycles = self._get_tasklist()

    def _get_entity_values(self) -> dict[str, str]:
        entity_values = collections.defaultdict(str)
        if not os.path.exists(self.workflow_file):
            return entity_values
        with open(self.workflow_file, 'r') as f:
            for line in f:
                if ']>' in line:
                    break
                if 'ENTITY' in line:
                    split_line = line.split()
                    if 'SYSTEM' in line:
                        if len(split_line) >= 4:
                            value = split_line[3]
                            entity_values[split_line[1]] = value.strip('"').strip("'")
                    else:
                        if len(split_line) >= 3:
                            value = split_line[2]
                            entity_values[split_line[1]] = value.strip('"').strip("'")
        return entity_values

    def _get_tasklist(self):
        tasks_ordered = []
        metatask_list = collections.defaultdict(list)
        cycledef_group_cycles = collections.defaultdict(list)

        if not os.path.exists(self.workflow_file):
            return tasks_ordered, metatask_list, cycledef_group_cycles

        try:
            tree = ET.parse(self.workflow_file)
            root = tree.getroot()
        except Exception:
            return tasks_ordered, metatask_list, cycledef_group_cycles

        cycle_noname = 'default_cycle'

        for child in root:
            if child.tag == 'cycledef':
                group = child.attrib.get('group', cycle_noname)
                cycle_string = child.text.split()
                if len(cycle_string) >= 3:
                    try:
                        start = datetime.strptime(cycle_string[0], '%Y%m%d%H%M')
                        end = datetime.strptime(cycle_string[1], '%Y%m%d%H%M')
                        # Very basic interval parsing
                        inc_match = re.match(r'(\d+):(\d+)(?::(\d+))?', cycle_string[2])
                        if inc_match:
                            h, m, s = inc_match.groups()
                            inc = timedelta(hours=int(h), minutes=int(m), seconds=int(s or 0))
                            curr = start
                            while curr <= end:
                                cycledef_group_cycles[group].append(curr.strftime('%Y%m%d%H%M'))
                                curr += inc
                    except Exception:
                        pass

            elif child.tag == 'task':
                name = child.attrib.get('name')
                cycledefs = child.attrib.get('cycledefs', cycle_noname)
                tasks_ordered.append((name, cycledefs))

            elif child.tag == 'metatask':
                m_name = child.attrib.get('name', 'NO_NAME')
                for task in child.findall('task'):
                    t_name = task.attrib.get('name')
                    t_cycledefs = task.attrib.get('cycledefs', cycle_noname)
                    tasks_ordered.append((t_name, t_cycledefs))
                    metatask_list[m_name].append(t_name)

        return tasks_ordered, metatask_list, cycledef_group_cycles

    def _parse_cycle(self, cycle_val) -> str:
        """Robustly parse cycle value from SQLite."""
        if isinstance(cycle_val, int):
            # Try to see if it's a timestamp (e.g. seconds since 1970)
            # or YYYYMMDDHHMM
            if cycle_val > 200000000000: # Likely YYYYMMDDHHMM
                return str(cycle_val)
            else: # Likely timestamp
                try:
                    return datetime.fromtimestamp(cycle_val).strftime('%Y%m%d%H%M')
                except Exception:
                    return str(cycle_val)
        return str(cycle_val)

    def get_status(self):
        """Query the SQLite database. Heavy operation."""
        if not os.path.exists(self.database_file):
            return []

        connection = sqlite3.connect(self.database_file)
        c = connection.cursor()

        cycles_raw = []
        try:
            q = c.execute('SELECT cycle FROM cycles ORDER BY cycle ASC')
            cycles_raw = [row[0] for row in q]
        except Exception:
            pass

        data = collections.defaultdict(list)
        try:
            q = c.execute('SELECT taskname, cycle, state, exit_status, duration, tries, jobid FROM jobs')
            for row in q:
                taskname, cycle_raw, state, exit_status, duration, tries, jobid = row
                data[cycle_raw].append({
                    'task': taskname,
                    'state': state,
                    'exit': exit_status,
                    'duration': duration,
                    'tries': tries,
                    'jobid': jobid
                })
        except Exception:
            pass
        finally:
            connection.close()

        result = []
        for cycle_raw in cycles_raw:
            cycle_str = self._parse_cycle(cycle_raw)
            cycle_data = {
                'cycle': cycle_str,
                'tasks': data.get(cycle_raw, [])
            }
            result.append(cycle_data)

        return result
