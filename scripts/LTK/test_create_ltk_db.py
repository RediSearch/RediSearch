# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

from __future__ import annotations

import concurrent.futures
import importlib.util
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

SCRIPT = Path(__file__).with_name("create_ltk_db.py")
SPEC = importlib.util.spec_from_file_location("create_ltk_db", SCRIPT)
assert SPEC and SPEC.loader
setup = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = setup
SPEC.loader.exec_module(setup)


class DatabaseSetupTests(unittest.TestCase):
    def test_temporary_redis_launches_are_serialized(self) -> None:
        active_launches = 0
        max_active_launches = 0
        counter_lock = threading.Lock()

        def wait_for_source(*_args: object, **_kwargs: object) -> object:
            nonlocal active_launches, max_active_launches
            with counter_lock:
                active_launches += 1
                max_active_launches = max(max_active_launches, active_launches)
            time.sleep(0.02)
            with counter_lock:
                active_launches -= 1
            return object()

        args = SimpleNamespace(redis_server="redis-server")
        with (
            tempfile.TemporaryDirectory() as directory,
            mock.patch.object(setup, "reserve_source_port", side_effect=(10001, 10002)),
            mock.patch.object(setup.subprocess, "run"),
            mock.patch.object(setup, "wait_for_source", side_effect=wait_for_source),
        ):
            temp_dir = Path(directory)
            with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
                futures = [
                    executor.submit(
                        setup.launch_temporary_redis,
                        object(),
                        args,
                        temp_dir,
                        temp_dir / "dump.rdb",
                        temp_dir / f"redis-{index}.pid",
                        temp_dir / f"redis-{index}.log",
                    )
                    for index in range(2)
                ]
                ports = [future.result()[0] for future in futures]

        self.assertEqual(set(ports), {10001, 10002})
        self.assertEqual(max_active_launches, 1)


if __name__ == "__main__":
    unittest.main()
