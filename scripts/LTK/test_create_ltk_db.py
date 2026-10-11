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
    def test_cluster_uses_complete_ltk_search_profile(self) -> None:
        args = SimpleNamespace(
            search_timeout_ms=5000,
            username=None,
            password=None,
            tls=False,
        )
        config = {"search-workers-priority-bias-threshold": "6"}
        commands: list[tuple[object, ...]] = []

        class RedisClient:
            def __init__(self, **_kwargs: object):
                pass

            def execute_command(self, *command: object) -> object:
                commands.append(command)
                if command[:2] == ("CONFIG", "SET"):
                    for index in range(2, len(command), 2):
                        config[str(command[index])] = str(command[index + 1])
                    return b"OK"
                if command[:2] == ("CONFIG", "GET"):
                    return {name.encode(): config[name].encode() for name in command[2:]}
                raise AssertionError(command)

            def close(self) -> None:
                pass

        redis_module = SimpleNamespace(
            Redis=RedisClient,
            exceptions=SimpleNamespace(RedisError=RuntimeError),
        )
        nodes = [SimpleNamespace(host="127.0.0.1", port=7000)]
        setup.configure_cluster(redis_module, args, nodes)

        set_command = next(command for command in commands if command[:2] == ("CONFIG", "SET"))
        self.assertNotIn("search-workers-priority-bias-threshold", set_command)
        self.assertEqual(config, setup.expected_search_config(args))

    def test_immutable_profile_mismatch_does_not_mutate_cluster(self) -> None:
        commands: list[tuple[object, ...]] = []

        class RedisClient:
            def __init__(self, **_kwargs: object):
                pass

            def execute_command(self, *command: object) -> object:
                commands.append(command)
                return {b"search-workers-priority-bias-threshold": b"1"}

            def close(self) -> None:
                pass

        redis_module = SimpleNamespace(
            Redis=RedisClient,
            exceptions=SimpleNamespace(RedisError=RuntimeError),
        )
        args = SimpleNamespace(
            search_timeout_ms=2000,
            username=None,
            password=None,
            tls=False,
        )
        with self.assertRaisesRegex(RuntimeError, "recreate the cluster"):
            setup.configure_cluster(
                redis_module,
                args,
                [SimpleNamespace(host="127.0.0.1", port=7000)],
            )

        self.assertFalse(any(command[:2] == ("CONFIG", "SET") for command in commands))

    def test_rdb_preflight_verifies_expected_bucket_owner(self) -> None:
        calls: list[dict[str, object]] = []

        class S3Client:
            def head_object(self, **kwargs: object) -> None:
                calls.append(kwargs)

        args = SimpleNamespace(
            s3_bucket="bucket",
            s3_prefix="prefix",
            s3_expected_bucket_owner="123456789012",
        )
        setup.verify_rdb_objects(args, S3Client())

        self.assertEqual(len(calls), setup.RDB_COUNT)
        self.assertTrue(
            all(call["ExpectedBucketOwner"] == "123456789012" for call in calls)
        )

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
