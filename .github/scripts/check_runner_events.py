# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

"""CI-only check of padded error headers and diagnostic output boundaries."""

import json
from pathlib import Path
import tempfile

import runner_events


with tempfile.TemporaryDirectory() as directory:
    runner_events.DIRECTORY = Path(directory)
    dummy_url = 'https://diagnostic.invalid/path?token=DUMMY_SECRET_CANARY'
    fixture = (
        '[2026-10-28 12:00:00Z ERR  JobDispatcher] '
        'Catch exception during renew runner job dummy-job. ' + dummy_url + '\n'
        '[2026-10-28 12:00:01Z ERR  JobDispatcher] '
        'System.Net.Http.HttpRequestException: ' + dummy_url + '\n'
        '[2026-10-28 12:00:02Z INFO JobDispatcher] '
        'Successfully renew job dummy-job, job is valid till 10/28/2026 12:10:02 PM\n'
        '[2026-10-28 12:00:03Z INFO Worker] Ignored payload: ' + dummy_url + '\n'
    )
    for kind in ('Runner', 'Worker'):
        (runner_events.DIRECTORY / (kind + '_20261028-120000-utc.log')).write_text(fixture)
    result = runner_events.snapshot()

for entry in result['logs'].values():
    assert entry['readable'], 'Synthetic diagnostic fixture must be readable'
    assert entry['counts'] == {'lease_error': 1, 'component_error': 1, 'lease_renewed': 1}
    assert entry['events'] == [
        {'utc': '2026-10-28 12:00:00Z', 'component': 'JobDispatcher', 'event': 'lease_error'},
        {'utc': '2026-10-28 12:00:01Z', 'component': 'JobDispatcher',
         'event': 'component_error', 'exception': 'HttpRequestException'},
        {'utc': '2026-10-28 12:00:02Z', 'component': 'JobDispatcher', 'event': 'lease_renewed'},
    ]
encoded = json.dumps(result)
assert 'DUMMY_SECRET_CANARY' not in encoded, 'Diagnostic output must omit message secrets'
assert 'https://' not in encoded, 'Diagnostic output must omit message URLs'
assert 'dummy-job' not in encoded, 'Diagnostic output must omit free-form job identifiers'
print('Runner event parser self-check passed')
