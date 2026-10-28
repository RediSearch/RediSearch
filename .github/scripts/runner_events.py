# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

"""Extract fixed event categories; never publish raw runner diagnostic text."""

from collections import Counter
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import stat
import sys


DIRECTORY = Path('/runner-diag')
MAX_BYTES = 4 * 1024 * 1024
HEADER = re.compile(
    r'^\[(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}Z) '
    r'(INFO|WARN|ERR|VERB) +'
    r'(JobDispatcher|BrokerMessageListener|BrokerServer|RunServer|ResultServer|'
    r'JobServerQueue|JobRunner|Worker|Runner|ProcessChannel)\] (.*)$'
)
EVENTS = (
    ('lease_renewed', r'Successfully renew job(?: request)? '),
    ('lease_retry', r'Retrying lock renewal '),
    ('lease_retry_exhausted', r'Lock renewal has run out of retry'),
    ('lease_error', r'Catch exception during renew runner '),
    ('lease_job_missing', r'TaskAgentJobNotFoundException received'),
    ('lease_token_expired', r'TaskAgentJobTokenExpiredException received'),
    ('lease_cancelled', r'job renew has been cancelled'),
    ('lease_stopped', r'Stop renew job request '),
    ('worker_finished', r'Worker finished for job '),
    ('worker_dispatch_error', r'Worker Dispatch failed with an exception'),
    ('worker_shutdown', r'Cancellation/Shutdown message received\.'),
    ('queue_shutdown_started', r'Fire signal to shutdown all queues\.'),
    ('queue_shutdown_finished', r'All queue process task stopped\.'),
    ('websocket_connected', r'Successfully started (?:runner )?websocket client\.'),
    ('websocket_connect_error', r'Exception caught during (?:runner )?websocket connect'),
    ('websocket_receive_error', r'Exception caught while holding runner websocket'),
    ('websocket_reconnect', r'Websocket is not open, let.s attempt to connect'),
    ('broker_request_acknowledged', r'Acknowledging runner request '),
)
EXCEPTIONS = (
    'TaskCanceledException', 'OperationCanceledException', 'HttpRequestException',
    'WebSocketException', 'SocketException', 'IOException',
    'ObjectDisposedException', 'NullReferenceException', 'TimeoutException',
)


def snapshot():
    result = {'observed_utc': datetime.now(timezone.utc).isoformat(), 'logs': {}}
    for kind in ('Runner', 'Worker'):
        entry = {'readable': False, 'bytes_read': 0, 'counts': {}, 'events': []}
        result['logs'][kind] = entry
        try:
            names = sorted(
                p for p in DIRECTORY.iterdir()
                if re.fullmatch(kind + r'_\d{8}-\d{6}-utc\.log', p.name)
            )
            if not names:
                entry['status'] = 'missing'
                continue
            # Do not follow a diagnostic-file symlink outside the read-only mount.
            descriptor = os.open(names[-1], os.O_RDONLY | os.O_NOFOLLOW)
            with os.fdopen(descriptor, 'rb') as source:
                metadata = os.fstat(source.fileno())
                if not stat.S_ISREG(metadata.st_mode):
                    entry['status'] = 'not_regular'
                    continue
                offset = max(0, metadata.st_size - MAX_BYTES)
                source.seek(offset)
                data = source.read(MAX_BYTES)
            entry.update(readable=True, status='ok', bytes_read=len(data))
        except OSError:
            entry['status'] = 'unavailable'
            continue

        lines = data.decode('utf-8', errors='replace').splitlines()
        if offset and lines:
            lines = lines[1:]
        if data and not data.endswith(b'\n'):
            lines = lines[:-1]
        counts = Counter()
        for line in lines:
            match = HEADER.fullmatch(line)
            if not match:
                continue
            timestamp, level, component, message = match.groups()
            event = next((name for name, pattern in EVENTS if re.match(pattern, message)), None)
            if event is None and level == 'ERR':
                event = 'component_error'
            if event is None:
                continue
            # Output values come from fixed enums or numeric timestamp captures,
            # never from the free-form message, URLs, or exception detail.
            record = {'utc': timestamp, 'component': component, 'event': event}
            exception = next((name for name in EXCEPTIONS if re.search(r'\b' + name + r'\b', message)), None)
            if exception:
                record['exception'] = exception
            counts[event] += 1
            entry['events'].append(record)
        entry['counts'] = dict(counts)
        entry['events'] = entry['events'][-40:]
    return result


if __name__ == '__main__':
    result = snapshot()
    print(json.dumps(result, separators=(',', ':')))
    if '--require-readable' in sys.argv and not all(log['readable'] for log in result['logs'].values()):
        sys.exit(1)
