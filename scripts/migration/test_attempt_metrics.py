# Copyright (c) 2006-Present, Redis Ltd. All rights reserved.
# Licensed under your choice of RSALv2, SSPLv1, or AGPLv3.
"""Keep missing evidence distinct from zero and shell success from acceptance."""

import json
from pathlib import Path
import tempfile
import unittest

from attempt_metrics import collect


class MetricsTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def write(self, name, data):
        path = self.root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(data) + '\n')
        return path

    def test_deduplicates_commands_and_preserves_missingness(self):
        cid = '11111111-1111-1111-1111-111111111111'
        for label in ('build', 'duplicate'):
            self.write(label + '-command.json', {'command_id': cid})
        self.write('missing-command.json', {'command_id': 'missing'})
        self.write(cid + '-result.json', {
            'Status': 'Success', 'ResponseCode': 0,
            'ExecutionStartDateTime': '2026-10-12T01:00:00Z',
            'ExecutionEndDateTime': '2026-10-12T01:00:02.500Z',
            'StandardOutputContent': 'tests FAILED',
        })
        result = collect(self.root)
        self.assertEqual(result['ssm']['unique_commands'], 2)
        self.assertEqual(result['ssm']['statuses'], {'Success': 1, 'Unknown': 1})
        self.assertEqual(result['ssm']['summed_command_seconds'], 2.5)
        self.assertEqual(result['ssm']['missing_results'], 1)
        self.assertIsNone(result['iterations'])
        self.assertIsNone(result['model_usage'])

    def test_usage_does_not_double_count_cached_tokens_or_same_file(self):
        usage = self.write('events.jsonl', {'type': 'turn.completed', 'usage': {
            'input_tokens': 100, 'cached_input_tokens': 80, 'output_tokens': 20}})
        result = collect(self.root, [usage, usage])
        self.assertEqual(len(result['model_usage']), 1)
        self.assertEqual(result['model_usage'][0]['tokens']['input_tokens'], 100)
        self.assertIsNone(result['model_cost'])

    def test_rejects_conflicting_status_snapshots(self):
        name = '11111111-1111-1111-1111-111111111111-result.json'
        self.write(name, {'Status': 'InProgress'})
        self.write('copy/' + name, {'Status': 'Success'})
        with self.assertRaises(ValueError):
            collect(self.root)

    def test_explicit_iterations_only_and_rejects_duplicates(self):
        entry = {'case': 'geo', 'iteration_id': '2', 'kind': 'repair',
                 'outcome': 'failed', 'evidence': 'tests.log'}
        ledger = self.write('iterations.jsonl', entry)
        self.assertEqual(collect(self.root, ledger=ledger)['iterations']['recorded_iterations'], 1)
        ledger.write_text(json.dumps(entry) + '\n' + json.dumps(entry) + '\n')
        with self.assertRaises(ValueError):
            collect(self.root, ledger=ledger)

    def test_unknown_duration_is_not_zero_duration_evidence(self):
        self.write('11111111-1111-1111-1111-111111111111-result.json', {'Status': 'Failed'})
        result = collect(self.root)['ssm']
        self.assertEqual(result['commands_with_duration'], 0)
        self.assertIsNone(result['summed_command_seconds'])
        self.assertIsNone(result['commands'][0]['duration_seconds'])


if __name__ == '__main__':
    unittest.main()
