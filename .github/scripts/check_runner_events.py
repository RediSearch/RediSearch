# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

"""CI-only check of padded error headers and diagnostic output boundaries."""

import json
from collections import Counter
from pathlib import Path
import tempfile

import runner_events


# Literal message formats from actions/runner v2.338.0. Keep these independent
# of the parser's patterns so an upstream-format mismatch fails in CI.
cases = [
    ('JobDispatcher', 'INFO', 'Successfully renew job dummy-job, job is valid till 10/28/2026 12:10:02 PM', 'lease_renewed'),
    ('JobDispatcher', 'INFO', 'Successfully renew job request 42, job is valid till 10/28/2026 12:10:02 PM', 'lease_renewed'),
    ('JobDispatcher', 'INFO', 'Retrying lock renewal for job dummy-job. The first job renew request has failed.', 'lease_retry'),
    ('JobDispatcher', 'INFO', 'Lock renewal has run out of retry, stop renew lock for job dummy-job.', 'lease_retry_exhausted'),
    ('JobDispatcher', 'ERR ', 'Catch exception during renew runner job dummy-job.', 'lease_error'),
    ('JobDispatcher', 'INFO', 'TaskAgentJobNotFoundException received when renew job dummy-job, job is no longer valid, stop renew job request.', 'lease_job_missing'),
    ('JobDispatcher', 'INFO', 'TaskAgentJobTokenExpiredException received renew job request 42, job is no longer valid, stop renew job request.', 'lease_token_expired'),
    ('JobDispatcher', 'INFO', 'job renew has been cancelled, stop renew job dummy-job.', 'lease_cancelled'),
    ('JobDispatcher', 'INFO', 'Stop renew job request for job dummy-job.', 'lease_stopped'),
    ('JobDispatcher', 'INFO', 'Worker finished for job dummy-job. Code: 0', 'worker_finished'),
    ('JobDispatcher', 'ERR ', 'Worker Dispatch failed with an exception for job request dummy-job.', 'worker_dispatch_error'),
    ('Worker', 'INFO', 'Cancellation/Shutdown message received.', 'worker_shutdown'),
    ('JobServerQueue', 'INFO', 'Fire signal to shutdown all queues.', 'queue_shutdown_started'),
    ('JobServerQueue', 'INFO', 'All queue process task stopped.', 'queue_shutdown_finished'),
    ('ResultServer', 'INFO', 'Successfully started websocket client.', 'websocket_connected'),
    ('BrokerServer', 'INFO', 'Successfully started runner websocket client.', 'websocket_connected'),
    ('ResultServer', 'INFO', 'Exception caught during websocket client connect, retry connection.', 'websocket_connect_error'),
    ('BrokerServer', 'INFO', 'Exception caught during runner websocket connect, will retry.', 'websocket_connect_error'),
    ('BrokerServer', 'INFO', 'Exception caught while holding runner websocket, will reconnect.', 'websocket_receive_error'),
    ('ResultServer', 'INFO', "Websocket is not open, let's attempt to connect back again with random backoff 00:00:00.3080000 ms.", 'websocket_reconnect'),
    ('BrokerMessageListener', 'INFO', "Acknowledging runner request 'dummy-job'.", 'broker_request_acknowledged'),
]
exceptions = [
    'TaskCanceledException', 'OperationCanceledException', 'HttpRequestException',
    'WebSocketException', 'SocketException', 'IOException',
    'ObjectDisposedException', 'NullReferenceException', 'TimeoutException',
]
expected = [
    {'utc': '2026-10-28 12:00:00Z', 'component': component, 'event': event}
    for component, level, message, event in cases
]
expected.extend(
    {'utc': '2026-10-28 12:00:00Z', 'component': 'RunServer',
     'event': 'component_error', 'exception': name}
    for name in exceptions
)

with tempfile.TemporaryDirectory() as directory:
    runner_events.DIRECTORY = Path(directory)
    dummy_url = 'https://diagnostic.invalid/path?token=DUMMY_SECRET_CANARY'
    fixture = ''.join(
        f'[2026-10-28 12:00:00Z {level} {component}] {message} {dummy_url}\n'
        for component, level, message, event in cases
    )
    fixture += ''.join(
        f'[2026-10-28 12:00:00Z ERR  RunServer] System.Net.Http.{name}: {dummy_url}\n'
        for name in exceptions
    )
    fixture += '[2026-10-28 12:00:03Z INFO Worker] Ignored payload: ' + dummy_url + '\n'
    for kind in ('Runner', 'Worker'):
        (runner_events.DIRECTORY / (kind + '_20261028-120000-utc.log')).write_text(fixture)
    result = runner_events.snapshot()

for entry in result['logs'].values():
    assert entry['readable'], 'Synthetic diagnostic fixture must be readable'
    assert entry['counts'] == dict(Counter(event['event'] for event in expected))
    assert entry['events'] == expected
encoded = json.dumps(result)
assert 'DUMMY_SECRET_CANARY' not in encoded, 'Diagnostic output must omit message secrets'
assert 'https://' not in encoded, 'Diagnostic output must omit message URLs'
assert 'dummy-job' not in encoded, 'Diagnostic output must omit free-form job identifiers'
print('Runner event parser self-check passed')
