#!/usr/bin/env python3
# Copyright (c) 2006-Present, Redis Ltd. All rights reserved.
# Licensed under your choice of RSALv2, SSPLv1, or AGPLv3.
"""Summarize observed execution evidence without inferring migration attempts."""

import argparse
from collections import Counter
from datetime import datetime
import json
from pathlib import Path
import re

UUID = re.compile(r'^[0-9a-fA-F]{8}(?:-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}$')


def timestamp(value):
    try:
        return datetime.fromisoformat(value.replace('Z', '+00:00')) if value else None
    except (ValueError, AttributeError):
        return None


def collect(root, usage_files=(), ledger=None):
    commands = {}
    warnings = []
    for path in sorted(root.glob('**/*-command.json')):
        data = json.loads(path.read_text())
        cid = data.get('command_id')
        if not cid:
            continue
        row = commands.setdefault(cid, {'command_id': cid, 'labels': [], 'evidence': []})
        row['labels'].append(path.name.removesuffix('-command.json'))
        row['evidence'].append(str(path))
    for path in sorted(root.glob('**/*-result.json')):
        cid = path.name.removesuffix('-result.json')
        if not UUID.fullmatch(cid):
            continue
        data = json.loads(path.read_text())
        row = commands.setdefault(cid, {'command_id': cid, 'labels': [], 'evidence': []})
        result = {key: data.get(key) for key in (
            'Status', 'ResponseCode', 'ExecutionStartDateTime', 'ExecutionEndDateTime')}
        if 'result' in row and row['result'] != result:
            raise ValueError(f'Conflicting result snapshots for {cid}; supply one final snapshot')
        row['result'] = result
        row['evidence'].append(str(path))
    durations = []
    for row in commands.values():
        result = row.get('result', {})
        start = timestamp(result.get('ExecutionStartDateTime'))
        end = timestamp(result.get('ExecutionEndDateTime'))
        duration = None
        if start and end and start.tzinfo and end.tzinfo and end >= start:
            duration = (end - start).total_seconds()
            durations.append(duration)
        row['duration_seconds'] = duration
    usage = []
    seen = set()
    for path in usage_files:
        path = path.resolve()
        if path in seen:
            continue
        seen.add(path)
        totals = Counter()
        turns = 0
        for line in path.read_text().splitlines():
            event = json.loads(line)
            if event.get('type') == 'turn.completed':
                values = event.get('usage', {})
                keys = ('input_tokens', 'cached_input_tokens', 'output_tokens')
                if not all(isinstance(values.get(k), int) and values[k] >= 0 for k in keys):
                    raise ValueError(f'Incomplete token usage in {path}')
                totals.update({k: values[k] for k in keys})
                turns += 1
        usage.append({'evidence': str(path), 'completed_turns': turns,
                      'tokens': dict(totals) if turns else None})
    iterations = None
    if ledger:
        entries = [json.loads(line) for line in ledger.read_text().splitlines() if line.strip()]
        ids = set()
        for entry in entries:
            if not all(entry.get(k) for k in ('case', 'iteration_id', 'kind', 'outcome', 'evidence')):
                raise ValueError('Ledger entries need case, iteration_id, kind, outcome, evidence')
            identity = (entry['case'], entry['iteration_id'])
            if identity in ids:
                raise ValueError(f'Duplicate iteration identity: {identity}')
            ids.add(identity)
        iterations = {'recorded_iterations': len(entries), 'by_kind': dict(Counter(
            e['kind'] for e in entries)), 'by_outcome': dict(Counter(e['outcome'] for e in entries)),
            'by_case': dict(Counter(e['case'] for e in entries)),
            'evidence': str(ledger)}
    if not usage_files:
        warnings.append('No model usage logs supplied; token usage and model cost are unknown.')
    if ledger is None:
        warnings.append('No iteration ledger supplied; migration attempts and fixes are unknown.')
    return {
        'schema_version': 1,
        'scope': 'Available local records only; this is not a complete run history.',
        'ssm': {'unique_commands': len(commands),
                'statuses': dict(Counter(r.get('result', {}).get('Status') or 'Unknown'
                                         for r in commands.values())),
                'missing_results': sum('result' not in r for r in commands.values()),
                'commands_with_duration': len(durations),
                'summed_command_seconds': round(sum(durations), 3) if durations else None,
                'commands': sorted(commands.values(), key=lambda r: r['command_id'])},
        'model_usage': usage or None,
        'iterations': iterations,
        'model_cost': None,
        'warnings': warnings,
        'interpretation': [
            'SSM success is shell completion, not test success or migration acceptance.',
            'Commands include inspections and launches; background work outlives launch commands.',
            'Summed command durations are neither elapsed run time nor compute billing time.',
            'Cached input tokens are a subset of input tokens, not additional input.',
            'Usage covers supplied completed turns only; interrupted turns may incur unreported usage.'],
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('run_root', type=Path)
    parser.add_argument('--usage-jsonl', type=Path, action='append', default=[])
    parser.add_argument('--iteration-ledger', type=Path)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    if not args.run_root.is_dir():
        parser.error('run_root must be an existing directory')
    result = collect(args.run_root, args.usage_jsonl, args.iteration_ledger)
    args.output.write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps({k: v for k, v in result['ssm'].items() if k != 'commands'}))


if __name__ == '__main__':
    main()
