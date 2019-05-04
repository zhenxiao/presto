from functools import total_ordering
import json
from tabulate import tabulate
import argparse
import sys

parser = argparse.ArgumentParser(description='Parsing presto logs')
parser.add_argument('--raw', default=False, help='Time logs are raw sessionLogEntries json list')
parser.add_argument('--file', type=str, help='Input file')
args = parser.parse_args()

def load_json_file(args):
    if args.file is None:
        return json.load(sys.stdin)
    else:
        with open(args.file) as f:
            return json.load(f)


def get_session_log_entries(obj, taskid):
    def with_taskid(o):
        o["task"] = taskid
        return o

    return [with_taskid(o) for o in obj.get("sessionLogEntries", [])]


def populate_session_log_entries(sessionLogEntries, stages):
    for stage in stages:
        for task in stage.get("tasks", []):
            taskid = task["taskStatus"]["taskId"].split(".",2)[-1]
            sessionLogEntries.extend(get_session_log_entries(task, taskid))
        populate_session_log_entries(sessionLogEntries, stage.get("subStages", []))

def get_time_logs(args):
    presto = load_json_file(args)
    if args.raw:
        return presto
    sessionLogEntries = get_session_log_entries(presto, "coordinator")
    stage = presto.get("outputStage", None)
    if stage is not None:
        populate_session_log_entries(sessionLogEntries, [stage])
    return sessionLogEntries

@total_ordering
class Measure(object):
    def __init__(self, entry):
        self.message = entry["message"]
        self.nanos = entry["nanos"]
        self.task = entry["task"]
        self.thread_name = entry["threadName"]

    def _internal(self):
        return (self.nanos, self.message, self.thread_name) # no task, since there can be double logging

    def __eq__(self, other):
        return (self._internal() == other._internal())

    def __ne__(self, other):
        return not (self == other)

    def __lt__(self, other):
        return self._internal() < other._internal()

    def __repr__(self):
        return str(self._internal())

    def __hash__(self):
        return hash(self._internal())

proscribed = []
prev_time = None
deltas = []
prev_message = None
entries = []
headers = ["delta_ms", "absolute_ms", "activity", "thread", "task"]
first_time = None
time_logs = sorted(set([Measure(x) for x in get_time_logs(args)]))
for time_log in time_logs:
    print(json.dumps(time_log.__dict__))
    message = time_log.message
    nanos = time_log.nanos
    if first_time is None:
        first_time = nanos
    if prev_time is not None:
        delta_time = round((nanos - prev_time)/1e6, 2)
        first_time_delta = round((nanos - first_time)/1e6, 2)
        delta_name = ' --BW-- '.join([prev_message, message])
        deltas.append((delta_time, delta_name, first_time_delta))
        entries.append([delta_time, first_time_delta, message[0:75], time_log.thread_name[0:40], time_log.task])

    prev_message = message
    prev_time = nanos
for delta in sorted(deltas, key=lambda x: x[0], reverse=True):
    print(delta)
print(tabulate(entries, headers=headers, tablefmt="pipe"))

