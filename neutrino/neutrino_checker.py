#!/usr/bin/env python

# Reason for not using ubermon directly:
# - requires a rollout/deployment of the ubermon package, and sometimes the docker hosts have an older version
# - internal helper files like presto_jmx_metrics weren't exposed
# - ubermon presto package is very tied to bare metal
# - refactoring can fix both of these

# Basic differences with regular presto: 
# - We have a multi-coordinator set up, so each node.environment and node.id is actually unique. We disregard that

# TODO: The metric definitions here are the real secret sauce and they should not be duplicated here.

# This file is an amalgamation of uber-metrics binary, presto_jvm.py, presto_metrics.py

# TODO: DRY this with ubermon

import argparse
import collections
import logging
from logging.handlers import RotatingFileHandler
import re
import requests
from requests.adapters import HTTPAdapter
import socket
import sched
import sys
import time
import urllib3
from six import string_types

import ubermon

METRICS = [
{
    "prefix": "jvm.Memory",
    "mbean": "java.lang:type=Memory",
    "metrics": [('HeapMemoryUsage', ['committed', 'init', 'max', 'used'], 'GAUGE'), ('NonHeapMemoryUsage', ['committed', 'init', 'max', 'used'], 'GAUGE')]
},
{
    "prefix": "jvm.Memory.G1YoungGen",
    "mbean": "java.lang:name=G1 Young Generation,type=GarbageCollector",
    "metrics": [('CollectionCount', None, 'COUNTER'), ('CollectionTime', None, 'COUNTER')]
},
{
    "prefix": "jvm.Memory.G1OldGen",
    "mbean": "java.lang:name=G1 Old Generation,type=GarbageCollector",
    "metrics": [('CollectionCount', None, 'COUNTER'), ('CollectionTime', None, 'COUNTER'), ('LastGcInfo', ['duration'], 'GAUGE')]
},
{
    "prefix": "jvm.Threading",
    "mbean": "java.lang:type=Threading",
    "metrics": ['DaemonThreadCount', 'PeakThreadCount', 'ThreadCount', ('TotalStartedThreadCount', None, 'COUNTER')]
},
{
    "prefix": "presto.Execution",
    "mbean": "com.facebook.presto.execution:name=QueryManager",
},
{
    "prefix": "presto.scheduler",
    "mbean": "com.facebook.presto.execution.scheduler:name=SplitSchedulerStats"
},
{
    "prefix": "presto.httpclient.scheduler",
    "mbean": "io.airlift.http.client:name=ForScheduler,type=HttpClient"
},
{
    "prefix": "presto.metadata",
    "mbean": "com.facebook.presto.metadata:name=DiscoveryNodeManager",
    "metrics": ['activenodecount', 'inactivenodecount', 'shuttingdownnodecount']
},
{
    "prefix": "presto.schedulerclient",
    "mbean": "io.airlift.http.client:type=HttpClient,name=ForScheduler",
    "metrics": ["activeconnectionsperdestination", "connectionstats", "threadpool"]
},
{
    "prefix": "presto.Execution",
    "mbean": "com.facebook.presto.execution:name=TaskManager"
},
{
    "prefix": "presto.Execution",
    "mbean": "com.facebook.presto.execution.executor:name=TaskExecutor",
    "metrics": ["BlockedSplits", "WaitingSplits", "RunnerThreads", "RunningSplits",
                "Tasks", "TotalSplits", "RunAwaySplitCount"]
},
{
    "prefix": "presto.taskresource",
    "mbean": "com.facebook.presto.server:name=TaskResource"
},
{
    "prefix": "presto.httpserver",
    "mbean": "io.airlift.http.server:name=HttpServer",
    "metrics": ["httpconnectionstats"]
},
{
    "prefix": "presto.requeststats",
    "mbean": "io.airlift.http.server:name=RequestStats"
},
{
    "prefix": "presto.statementresource",
    "mbean": "com.facebook.presto.server.protocol:name=StatementResource"
},
{
    "prefix": "presto.exchangeclient",
    "mbean": "io.airlift.http.client:type=HttpClient,name=ForExchange",
    "metrics": ["activeconnectionsperdestination", "connectionstats", "threadpool"]
},
{
    "prefix": "presto.coordinator.system",
    "mbean": "java.lang:type=OperatingSystem",
    "metrics": ["SystemLoadAverage", "", "ProcessCpuLoad", "TotalPhysicalMemorySize",
                "FreePhysicalMemorySize", "TotalSwapSpaceSize", "FreeSwapSpaceSize",
                "OpenFileDescriptorCount"]
},
{
    "prefix": "presto.worker.system",
    "mbean": "java.lang:type=OperatingSystem",
    "metrics": ["SystemLoadAverage", "", "ProcessCpuLoad", "TotalPhysicalMemorySize",
                "FreePhysicalMemorySize", "TotalSwapSpaceSize", "FreeSwapSpaceSize",
                "OpenFileDescriptorCount"]
},
{
    "prefix": "presto.worker.runtime",
    "mbean": "java.lang:type=Runtime",
    "metrics": ["Uptime"]
}
]

# TODO: Keep this list obviously up to date with the catalog
for pinot_cluster in ["pinotstg", "pinotsandbox", "pinotprod", "pinotadhoc", "pinotphxstg"]:
    METRICS.append({
        "prefix": "presto.pinot." + pinot_cluster,
        "mbean": "com.facebook.presto.pinot:type=PinotMetrics,name=" + pinot_cluster
        })

app_log = None

def setup_logging(args):
    global app_log
    fmt = '%(asctime)s %(levelname)s %(message)s'

    if args.stderr_logging:
        logging.basicConfig(
                stream=sys.stderr,
                level=logging.DEBUG,
                format=fmt)
        app_log = logging.getLogger('root')
        app_log.setLevel(logging.DEBUG)
    else:
        app_log = logging.getLogger('root')
        my_handler = RotatingFileHandler(args.log_file, mode='a', maxBytes=1024*1024,
                backupCount=2, encoding=None, delay=0)
        my_handler.setFormatter(logging.Formatter(fmt))
        app_log.setLevel(logging.DEBUG if args.debug else logging.WARNING)
        app_log.addHandler(my_handler)

def filter_none(result):
    if not result:
        return

    for metric_name in result.keys():
        if not result[metric_name]['value'] or result[metric_name]['value'] == 'NaN':
            result.pop(metric_name)

def construct_regex(ll):
    return re.compile('|'.join(str(l).lower() for l in ll))

def jmx_http_url(port):
    return 'http://localhost:' + str(port) + '/v1/jmx/mbean/'

DEFAULT_TYPE_MATCHER = construct_regex(['double', 'long', 'int'])

def requests_retry_session(
        retries=3,
        backoff_factor=0.3,
        status_forcelist=(500, 502, 504),
        session=None,
):
    session = session or requests.Session()
    retry = urllib3.util.retry.Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session

ALL_TIME_SERIES = construct_regex(['OneMinute', 'FiveMinute', 'FifteenMinute', 'AllTime'])

def get_metrics(port, metrics):
    ret = get_metrics_helper(port, metrics)
    app_log.debug("For the metrics spec {}, returning {}".format(metrics, ret))
    return ret
    
def get_metrics_helper(port, metrics):
    prefix = metrics['prefix']
    mbean_object = metrics['mbean']
   
    desired_attributes = {}
    for metric in metrics.get('metrics', []):
        if isinstance(metric, tuple):
            attr, sub_metrics, m3type = metric
            desired_attributes[attr.lower()] = (attr, sub_metrics, m3type)
        elif isinstance(metric, string_types):
            desired_attributes[metric.lower()] = (metric, None, 'GAUGE')

    url = jmx_http_url(port) + mbean_object
    try:
        response = requests_retry_session().get(url, timeout=5)
    except Exception as x:
        app_log.error("Error fetching %s. %s", mbean_object, x)
        return {}

    if not response.ok:
        app_log.error("Error fetching %s: return code is %s", mbean_object, response.status_code)
        return {}

    if not response.text:
        app_log.warn("Response fetching %s is empty", mbean_object)
        return {}

    results = {}
    ts = time.time()
    for attr in response.json().get('attributes', []):
        if 'name' not in attr or 'value' not in attr or 'type' not in attr:
            continue

        name = str(attr['name'])
        value = attr['value']
        t = attr['type']

        nl = name.lower()

        attr_details = desired_attributes.get(nl, None)

        if attr_details is None:
            # No metrics were given, get everything for primitives
            if DEFAULT_TYPE_MATCHER.search(t):
                key = ".".join([prefix, name])
                results[key] = {'ts': ts, 'type': 'GAUGE', 'value': value}
        else:
            publish_name = attr_details[0]
            sub_metrics = attr_details[1]
            if sub_metrics is None and DEFAULT_TYPE_MATCHER.search(t):
                # valid primitive type
                key = ".".join([prefix, publish_name])
                results[key] = {'ts': ts, 'type': attr_details[2], 'value': value}
            elif sub_metrics and isinstance(value, collections.Mapping):
                for sub_metric in sub_metrics:
                    value_sub_metric = value.get(sub_metric, None)
                    if value_sub_metric:
                        key = ".".join([prefix, publish_name, sub_metric])
                        results[key] = {'ts': ts, 'type': attr_details[2], 'value': value_sub_metric}
    return results

def updateResult(port, result, metrics):
    start = time.time()
    result.update(get_metrics(port, metrics))
    app_log.info("updateResult for %s finished in %f seconds", metrics['mbean'], time.time() - start)


def run_check(port, service_name, m3obj, pretend, debug):
    result = {}
    for metric in METRICS:
        updateResult(port, result, metric)
    filter_none(result)
    if debug:
        app_log.info('Data for graphite:')
        app_log.info(m3obj.format_for_graphite(service_name, result))
    if not pretend:
        m3obj.update(service_name, result)

def create_m3_obj():
    m3obj = ubermon.Metrics()
    m3obj.current_check_group = 'neutrino'
    return m3obj

def run_checks(port, service_name, pretend=False, debug=False):
    m3obj = create_m3_obj()
    check_timeout = ubermon.conf('check_timeout', default=20)

    t0 = time.time()
    try:
        run_check(port, service_name, m3obj, pretend, debug)
    except Exception, e:
        app_log.exception(e)
    app_log.debug('completed in %0.2f seconds\n' % (time.time() - t0))

def daemon_checks(right_away, port, service_name, pretend=False, interval=60, debug=False):
    timer = sched.scheduler(time.time, time.sleep)

    def add(wait):
        timer.enter(wait, 1, run_checks, [port, service_name, pretend, debug])

    add(0 if right_away else interval)
    while not timer.empty():
        timer.run()
        add(interval)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-p',
        '--pretend',
        action='store_true',
        help='Print check output instead of sending it to graphite'
    )
    parser.add_argument(
        '-c',
        '--conf-path',
        default='/etc/uber/metrics.conf',
        help='Path to config file (default %(default)s)'
    )
    parser.add_argument(
        '-l',
        '--log_file',
        default='metrics.log',
        help='Path to metrics file'
    )
    parser.add_argument(
        '--interval',
        default=60,
        type=float,
        help='interval for daemon checks'
    )
    parser.add_argument(
        '--port',
        default=8080,
        type=int,
        help='Port'
    )
    parser.add_argument(
        '-s',
        '--service-name',
        required=True,
        help='Service name'
    )
    parser.add_argument(
        '-d',
        '--debug',
        action='store_true',
        help='Debug'
    )
    parser.add_argument(
        '--stderr_logging',
        action='store_true',
        help='Debug'
    )
    parser.add_argument(
        '--right-away',
        action='store_true',
        help='Do the first round right away'
    )
    args = parser.parse_args()
    setup_logging(args)
    ubermon.init(args.conf_path)
    daemon_checks(
        right_away=args.right_away,
        port=args.port,
        service_name=args.service_name,
        pretend=args.pretend,
        interval=args.interval,
        debug=args.debug
    )

if __name__ == '__main__':
    main()
