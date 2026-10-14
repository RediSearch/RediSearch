#!/usr/bin/env python3
# Copyright (c) 2006-Present, Redis Ltd. All rights reserved.
# Licensed under your choice of RSALv2, SSPLv1, or AGPLv3.
"""Launch independent, non-interactive POC reviews from completed handoffs."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import time


def prepare(case, base, template):
    if case.get('status') != 'ready':
        return None
    name = case['name']
    if not re.fullmatch(r'[a-z0-9][a-z0-9_-]*', name):
        raise ValueError('Invalid case name')
    for key in ('starting_sha', 'candidate_identity', 'reference_shas'):
        if not case.get(key):
            raise ValueError(f'{name}: missing {key}')
    shas = [case['starting_sha'], *case['reference_shas']]
    if any(not re.fullmatch(r'[0-9a-f]{40}', sha) for sha in shas):
        raise ValueError(f'{name}: full Git SHAs required')
    handoff = (base / case['handoff']).read_text()
    if not handoff.strip() or '<...' in handoff:
        raise ValueError(f'{name}: handoff is empty or contains placeholders')
    evidence = (base / case['completion_evidence']).read_text()
    if not evidence.strip():
        raise ValueError(f'{name}: missing completion evidence')
    return (template + '\n\nResolved handoff (overrides template placeholders):\n'
            + handoff + '\n\nPinned identities:\n' + json.dumps(case, indent=2)
            + '\n\nCompletion evidence (verify before review):\n' + evidence
            + '\n\nReturn the complete comparison report as your final response. '
            'The launcher saves it as comparison.md. Put supporting files only in '
            'this review workspace. Do not ask interactive questions: report missing '
            'inputs/access as incomplete. Do not inspect previous chat sessions.\n')


def execute(command, prompt, output, timeout):
    start = time.monotonic()
    timed_out = False
    with (output / 'events.jsonl').open('w') as events, (output / 'stderr.log').open('w') as err:
        proc = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=events,
                                stderr=err, text=True, start_new_session=True)
        try:
            proc.communicate(prompt, timeout=timeout)
        except subprocess.TimeoutExpired:
            timed_out = True
            os.killpg(proc.pid, signal.SIGTERM)
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                os.killpg(proc.pid, signal.SIGKILL)
                proc.wait()
    report = output / 'comparison.md'
    return {'exit_code': proc.returncode, 'timed_out': timed_out,
            'elapsed_seconds': round(time.monotonic() - start, 3),
            'report_present': report.exists() and report.stat().st_size > 0}


def execution_status(record):
    return ('report_produced' if record['exit_code'] == 0
            and record['report_present'] and not record['timed_out']
            else 'execution_incomplete')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('manifest', type=Path)
    parser.add_argument('--output', required=True, type=Path)
    parser.add_argument('--execute', action='store_true', help='Default: prepare prompts only')
    parser.add_argument('--model', help='Required for execution; recorded for reproducibility')
    parser.add_argument('--timeout', type=int, default=3600)
    parser.add_argument('--remote-access', action='store_true', help='Enable network for approved SSM access')
    args = parser.parse_args()
    if args.timeout <= 0 or (args.execute and not args.model):
        parser.error('Positive timeout and explicit --model for execution required')
    manifest = args.manifest.resolve()
    config = json.loads(manifest.read_text())
    template = (Path(__file__).resolve().parents[2] /
                'docs/migration-poc/comparison-prompt.md').read_text()
    names = [c['name'] for c in config['cases']]
    if len(names) != len(set(names)):
        parser.error('Duplicate case names')
    # Validate all eligible inputs before starting any paid model work.
    prepared = [(c, prepare(c, manifest.parent, template)) for c in config['cases']]
    if args.execute and not any(prompt is not None for _, prompt in prepared):
        parser.error('No cases are ready: prepare frozen handoffs and completion evidence first')
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    summary = []
    for case, prompt in prepared:
        if prompt is None:
            summary.append({'case': case['name'], 'status': 'skipped_not_ready'})
        else:
            target = output / case['name']
            target.mkdir()
            (target / 'prompt.md').write_text(prompt)
            command = ['codex', 'exec', '--ignore-user-config', '--ephemeral',
                       '--skip-git-repo-check', '--sandbox', 'workspace-write',
                       '-c', 'approval_policy="never"', '--json', '--color', 'never',
                       '-C', str(target), '-o', str(target / 'comparison.md')]
            if args.model:
                command += ['--model', args.model]
            if args.remote_access:
                command += ['-c', 'sandbox_workspace_write.network_access=true']
            command += ['-']
            record = {'case': case['name'], 'command': command,
                      'prompt_sha256': hashlib.sha256(prompt.encode()).hexdigest(),
                      'status': 'prepared'}
            if args.execute:
                try:
                    record.update(execute(command, prompt, target, args.timeout))
                    record['status'] = execution_status(record)
                except OSError as exc:
                    record.update(status='launch_failed', error=str(exc))
            (target / 'execution.json').write_text(json.dumps(record, indent=2) + '\n')
            summary.append(record)
        (output / 'summary.json').write_text(json.dumps(summary, indent=2) + '\n')
    print(json.dumps([{'case': r['case'], 'status': r['status']} for r in summary], indent=2))
    return int(any(r['status'] in ('execution_incomplete', 'launch_failed') for r in summary))


if __name__ == '__main__':
    raise SystemExit(main())
