# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

from pathlib import Path
import shutil
import subprocess
import unittest


class GeoWorkspaceCheck(unittest.TestCase):
    @unittest.skipUnless(shutil.which('cargo-hakari'), 'requires the CI cargo-hakari tool')
    def test_hakari_dependencies(self):
        root = Path(__file__).resolve().parents[2]
        result = subprocess.run(
            ['cargo', 'hakari', 'manage-deps', '--dry-run'],
            cwd=root / 'src' / 'redisearch_rs',
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        self.assertEqual(result.returncode, 0, result.stdout)

    def test_miri_geo_filter_lifecycle(self):
        root = Path(__file__).resolve().parents[2]
        result = subprocess.run(
            [
                'cargo', '+nightly-2026-05-01', 'miri', 'nextest', 'run',
                '--manifest-path', 'src/redisearch_rs/Cargo.toml',
                '-p', 'geo_ffi', '-p', 'geo',
                '-E', 'test(=filter_lifecycle) | test(=filter::radius_boundary)',
            ],
            cwd=root,
        )
        self.assertEqual(result.returncode, 0)

if __name__ == '__main__':
    unittest.main()
