# Copyright (c) 2006-Present, Redis Ltd. All rights reserved.
# Licensed under your choice of RSALv2, SSPLv1, or AGPLv3.
import json
from pathlib import Path
import sys
import subprocess
import tempfile
import unittest

from compare_pocs import execute, execution_status, prepare


class ReviewLauncherTests(unittest.TestCase):
    def test_incomplete_case_never_needs_reference_inputs(self):
        self.assertIsNone(prepare({'status': 'incomplete'}, Path('/missing'), 'template'))

    def test_execute_with_no_ready_cases_fails_before_creating_outputs(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            manifest = root / 'cases.json'
            manifest.write_text(json.dumps({'cases': [{'name': 'geo', 'status': 'incomplete'}]}))
            result = subprocess.run(
                [sys.executable, str(Path(__file__).with_name('compare_pocs.py')),
                 str(manifest), '--output', str(root / 'output'), '--execute',
                 '--model', 'not-called'], capture_output=True, text=True)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn('No cases are ready', result.stderr)
            self.assertFalse((root / 'output').exists())

    def test_ready_requires_pinned_reference(self):
        with self.assertRaises(ValueError):
            prepare({'status': 'ready', 'name': 'geo', 'starting_sha': 'a'*40,
                     'candidate_identity': 'digest', 'reference_shas': ['HEAD']}, Path('.'), '')

    def test_ready_prompt_contains_handoff_and_evidence(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root/'handoff.md').write_text('Approved contract')
            (root/'done.md').write_text('Frozen validation record')
            case = dict(status='ready', name='geo', starting_sha='a'*40,
                        candidate_identity='digest', reference_shas=['b'*40],
                        handoff='handoff.md', completion_evidence='done.md')
            result = prepare(case, root, 'Review rules')
            self.assertIn('Approved contract', result)
            self.assertIn('Frozen validation record', result)

    def test_timeout_retains_logs_and_does_not_claim_report(self):
        with tempfile.TemporaryDirectory() as directory:
            result = execute([sys.executable, '-c', 'import time; time.sleep(30)'],
                             '', Path(directory), 0.1)
            self.assertTrue(result['timed_out'])
            self.assertFalse(result['report_present'])
            self.assertNotEqual(result['exit_code'], 0)

    def test_timeout_with_partial_report_is_incomplete(self):
        self.assertEqual(execution_status(dict(exit_code=0, report_present=True,
                                               timed_out=True)), 'execution_incomplete')

    def test_process_success_is_not_a_review_pass(self):
        with tempfile.TemporaryDirectory() as directory:
            result = execute([sys.executable, '-c', 'print("event")'], '', Path(directory), 5)
            self.assertEqual(result['exit_code'], 0)
            self.assertFalse(result['report_present'])


if __name__ == '__main__':
    unittest.main()
