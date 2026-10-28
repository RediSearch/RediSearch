# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

"""Bounded CI-only lookup; retain selected lifecycle fields, never raw responses."""

from datetime import datetime, timezone
import json
from pathlib import Path
import re
import subprocess

INSTANCE = 'i-0a7f37e3c46dec9da'
REGION = 'eu-west-1'
result = {'instance': INSTANCE, 'region': REGION, 'observed_utc': datetime.now(timezone.utc).isoformat(), 'operations': []}


def finish():
    Path('runner-lifecycle.json').write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps(result))
    raise SystemExit(0)


def request(service, operation, arguments):
    row = {'operation': operation}
    result['operations'].append(row)
    try:
        response = subprocess.run(
            ['aws', service, operation, '--region', REGION, '--output', 'json',
             '--no-paginate', '--cli-connect-timeout', '10', '--cli-read-timeout', '20', *arguments],
            capture_output=True, text=True, timeout=35, check=False,
        )
    except (OSError, subprocess.TimeoutExpired):
        row['status'] = 'client_unavailable_or_timeout'
        finish()
    if response.returncode:
        error = response.stderr
        row['status'] = next((label for pattern, label in (
            (r'UnauthorizedOperation|AccessDenied', 'access_denied'),
            (r'InvalidInstanceID.NotFound', 'instance_not_found'),
            (r'ExpiredToken|InvalidClientTokenId|UnrecognizedClientException', 'authentication_error'),
            (r'Throttl', 'throttled'),
        ) if re.search(pattern, error)), 'other_error')
        finish()
    try:
        data = json.loads(response.stdout)
    except ValueError:
        row['status'] = 'invalid_response'
        finish()
    row['status'] = 'success'
    return data, row


def enum(value, choices):
    return value if value in choices else 'other_or_absent'


def timestamp(value):
    return value if isinstance(value, str) and re.fullmatch(r'[0-9T:Z+. -]{10,40}', value) else None


def state(value):
    return enum(value, ('pending', 'running', 'shutting-down', 'terminated', 'stopping', 'stopped'))


data, row = request('ec2', 'describe-instances', ['--instance-ids', INSTANCE])
instances = [i for r in data.get('Reservations', []) for i in r.get('Instances', []) if i.get('InstanceId') == INSTANCE]
row['instances'] = [{
    'state': state(i.get('State', {}).get('Name')),
    'launch_time': timestamp(i.get('LaunchTime')),
    'reason_code': enum(i.get('StateReason', {}).get('Code'), (
        'Server.InternalError', 'Server.ScheduledStop', 'Server.SpotInstanceShutdown',
        'Server.SpotInstanceTermination', 'Client.InstanceInitiatedShutdown',
        'Client.UserInitiatedShutdown', 'Client.VolumeLimitExceeded', 'Client.InternalError',
        'Client.InvalidSnapshot.NotFound', 'Client.InvalidSnapshotID.NotFound',
    )),
    'transition_time': next(iter(re.findall(r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} GMT', i.get('StateTransitionReason', ''))), None),
} for i in instances]
if not instances:
    row['status'] = 'instance_not_returned'
    finish()

data, row = request('ec2', 'describe-instance-status', ['--instance-ids', INSTANCE, '--include-all-instances'])
row['instances'] = [{
    'state': state(i.get('InstanceState', {}).get('Name')),
    'system_status': enum(i.get('SystemStatus', {}).get('Status'), ('ok', 'impaired', 'insufficient-data', 'not-applicable', 'initializing')),
    'instance_status': enum(i.get('InstanceStatus', {}).get('Status'), ('ok', 'impaired', 'insufficient-data', 'not-applicable', 'initializing')),
    'events': [{
        'code': enum(e.get('Code'), ('instance-reboot', 'system-reboot', 'system-maintenance', 'instance-retirement', 'instance-stop')),
        'not_before': timestamp(e.get('NotBefore')), 'not_after': timestamp(e.get('NotAfter')),
    } for e in i.get('Events', [])],
} for i in data.get('InstanceStatuses', []) if i.get('InstanceId') == INSTANCE]

data, row = request('cloudtrail', 'lookup-events', [
    '--lookup-attributes', 'AttributeKey=ResourceName,AttributeValue=' + INSTANCE,
    '--start-time', '2026-10-28T12:00:00Z', '--max-results', '50',
])
row['has_more_events'] = bool(data.get('NextToken'))
row['lifecycle_events'] = []
for event in data.get('Events', []):
    if event.get('EventName') not in ('TerminateInstances', 'StopInstances', 'RebootInstances', 'StartInstances'):
        continue
    try:
        detail = json.loads(event.get('CloudTrailEvent', '{}'))
    except ValueError:
        continue
    identity = detail.get('userIdentity', {})
    issuer = identity.get('sessionContext', {}).get('sessionIssuer', {}).get('arn', '')
    role = re.fullmatch(r'arn:aws:iam::\d{12}:role/([A-Za-z0-9+=,.@_/-]{1,128})', issuer)
    row['lifecycle_events'].append({
        'event': event['EventName'], 'time': timestamp(event.get('EventTime')),
        'caller_type': enum(identity.get('type'), ('AssumedRole', 'IAMUser', 'AWSService', 'Root', 'FederatedUser', 'AWSAccount')),
        'issuer_role': role.group(1) if role else None,
    })
finish()
