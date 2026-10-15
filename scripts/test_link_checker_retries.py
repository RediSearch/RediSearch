# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

"""Exercise transient HTTP retries locally, without depending on external sites."""

import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from unittest.mock import patch

from check_links import LinkChecker


class LinkRetryTest(unittest.TestCase):
    def check_statuses(self, statuses, expected_calls, expected_valid):
        calls = []

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                code = statuses[min(len(calls), len(statuses) - 1)]
                calls.append(code)
                self.send_response(code)
                self.end_headers()

            def log_message(self, *_):
                pass

        with ThreadingHTTPServer(('127.0.0.1', 0), Handler) as server:
            checker = LinkChecker()
            thread = threading.Thread(target=server.serve_forever)
            thread.start()
            try:
                with patch.object(checker, '_check_with_curl', return_value=(False, 'HTTP failure')):
                    valid, _ = checker._check_absolute_link(
                        f'http://127.0.0.1:{server.server_port}/source#L1')
                self.assertEqual(valid, expected_valid)
                self.assertEqual(len(calls), expected_calls)
            finally:
                checker.session.close()
                server.shutdown()
                thread.join()

    def test_transient_503_recovers(self):
        self.check_statuses([503, 200], 2, True)

    def test_persistent_503_still_fails(self):
        self.check_statuses([503], 3, False)

    def test_missing_page_is_not_retried(self):
        self.check_statuses([404], 1, False)


if __name__ == '__main__':
    unittest.main()
