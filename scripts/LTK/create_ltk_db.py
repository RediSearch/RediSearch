#!/usr/bin/env python3
"""Configure an OSS Redis Cluster and import the LTK baseline RDB shards."""

# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

from __future__ import annotations

import argparse
import concurrent.futures
import gzip
import ipaddress
import logging
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import time
from collections import deque
from pathlib import Path
from typing import Any


RDB_COUNT = 20
S3_BUCKET = "rl-automation"
S3_PREFIX = "rlauto/search-customer-scenarios/ltk/rdbs-without-index"
SOURCE_START_TIMEOUT_SEC = 300.0
LOGGER = logging.getLogger("ltk-db-setup")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("shard_ip", type=ipaddress.ip_address)
    parser.add_argument("--port", type=int, default=7000)
    parser.add_argument("--username", default=os.getenv("REDIS_USERNAME"))
    parser.add_argument(
        "--password",
        default=os.getenv("REDIS_PASSWORD") or os.getenv("REDISCLI_AUTH"),
        help="Redis password; prefer REDIS_PASSWORD instead of the command line",
    )
    parser.add_argument("--tls", action="store_true")
    parser.add_argument("--s3-bucket", default=S3_BUCKET)
    parser.add_argument("--s3-prefix", default=S3_PREFIX)
    parser.add_argument("--redis-server", default="redis-server")
    parser.add_argument("--redis-cli", default="redis-cli")
    parser.add_argument("--search-timeout-ms", type=int, default=2_000)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--start-ordinal", type=int, default=1)
    parser.add_argument(
        "--replace-existing",
        action="store_true",
        help="Allow a resumed import to replace keys already migrated from an interrupted RDB",
    )
    parser.add_argument("--log-level", choices=("DEBUG", "INFO", "WARNING"), default="INFO")
    args = parser.parse_args()
    if not 1 <= args.port <= 65535:
        parser.error("--port must be between 1 and 65535")
    if args.search_timeout_ms <= 0:
        parser.error("--search-timeout-ms must be positive")
    if args.workers <= 0:
        parser.error("--workers must be positive")
    if not 1 <= args.start_ordinal <= RDB_COUNT:
        parser.error(f"--start-ordinal must be between 1 and {RDB_COUNT}")
    if args.start_ordinal > 1 and not args.replace_existing:
        parser.error("--start-ordinal requires --replace-existing")
    if args.tls:
        parser.error("RDB import from a temporary non-TLS Redis does not support --tls")
    return args


def connection_kwargs(
    args: argparse.Namespace, *, decode_responses: bool = False
) -> dict[str, Any]:
    return {
        "username": args.username,
        "password": args.password,
        "ssl": args.tls,
        "socket_connect_timeout": 10.0,
        "socket_timeout": 60.0,
        "decode_responses": decode_responses,
    }


def configure_cluster(redis_module: Any, args: argparse.Namespace) -> list[Any]:
    cluster = redis_module.RedisCluster(
        host=str(args.shard_ip),
        port=args.port,
        **connection_kwargs(args),
    )
    try:
        nodes = list(cluster.get_nodes())
        if not nodes:
            raise RuntimeError("Redis Cluster returned no nodes")
        failures: list[str] = []
        for node in nodes:
            client = redis_module.Redis(
                host=node.host,
                port=node.port,
                **connection_kwargs(args),
            )
            try:
                response = client.execute_command(
                    "CONFIG",
                    "SET",
                    "search-timeout",
                    args.search_timeout_ms,
                    "search-on-timeout",
                    "fail",
                )
                if response not in (True, b"OK", "OK"):
                    failures.append(f"{node.host}:{node.port} returned {response!r}")
            except redis_module.exceptions.RedisError as error:
                failures.append(f"{node.host}:{node.port}: {error}")
            finally:
                client.close()
        if failures:
            raise RuntimeError("Search configuration failed:\n  " + "\n  ".join(failures))
        LOGGER.info(
            "Configured search-timeout=%d and search-on-timeout=fail on %d nodes",
            args.search_timeout_ms,
            len(nodes),
        )
        return nodes
    finally:
        cluster.close()


def assert_cluster_ready(redis_module: Any, args: argparse.Namespace, nodes: list[Any]) -> None:
    primary_keys = 0
    existing_indexes: list[str] = []
    for node in nodes:
        if getattr(node, "server_type", None) != "primary":
            continue
        client = redis_module.Redis(
            host=node.host,
            port=node.port,
            **connection_kwargs(args),
        )
        try:
            primary_keys += int(client.dbsize())
            indexes = client.execute_command("FT._LIST")
            if indexes:
                existing_indexes.append(f"{node.host}:{node.port}: {indexes!r}")
        finally:
            client.close()
    if primary_keys and not args.replace_existing:
        raise RuntimeError(
            f"cluster contains {primary_keys} keys on primary nodes; "
            "RDB import requires an empty cluster"
        )
    if primary_keys:
        LOGGER.warning(
            "Resuming against a cluster containing %d keys; existing keys may be replaced",
            primary_keys,
        )
    if existing_indexes:
        raise RuntimeError(
            "cluster already contains Search indexes:\n  " + "\n  ".join(existing_indexes)
        )


def rdb_object_key(args: argparse.Namespace, ordinal: int) -> str:
    filename = f"ltk-{ordinal}_of_{RDB_COUNT}.rdb.gz"
    return f"{args.s3_prefix.rstrip('/')}/{filename}"


def verify_rdb_objects(args: argparse.Namespace, s3: Any) -> None:
    LOGGER.info("Checking %d LTK baseline objects in S3", RDB_COUNT)
    for ordinal in range(1, RDB_COUNT + 1):
        s3.head_object(Bucket=args.s3_bucket, Key=rdb_object_key(args, ordinal))


def reserve_source_port() -> int:
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


def wait_for_source(redis_module: Any, port: int, timeout_sec: float) -> Any:
    client = redis_module.Redis(
        host="127.0.0.1",
        port=port,
        socket_connect_timeout=1.0,
        socket_timeout=5.0,
    )
    deadline = time.monotonic() + timeout_sec
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            if client.ping():
                return client
        except Exception as error:  # noqa: BLE001 - LOADING and connection errors are retried
            last_error = error
        time.sleep(0.5)
    client.close()
    raise RuntimeError(f"temporary Redis did not load its RDB within {timeout_sec}s: {last_error}")


def redis_cli_import_command(args: argparse.Namespace, source_port: int) -> list[str]:
    command = [
        args.redis_cli,
        "--cluster",
        "import",
        f"{args.shard_ip}:{args.port}",
        "--cluster-from",
        f"127.0.0.1:{source_port}",
        "--cluster-yes",
    ]
    if args.username:
        command[1:1] = ["--user", args.username]
    if args.replace_existing:
        command.append("--cluster-replace")
    return command


def run_cluster_import(args: argparse.Namespace, source_port: int) -> None:
    environment = os.environ.copy()
    if args.password:
        environment["REDISCLI_AUTH"] = args.password
    output_tail: deque[str] = deque(maxlen=50)
    with subprocess.Popen(
        redis_cli_import_command(args, source_port),
        env=environment,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    ) as process:
        assert process.stdout is not None
        for line in process.stdout:
            output_tail.append(line.rstrip())
    if process.returncode:
        diagnostic = "\n".join(output_tail)
        raise RuntimeError(
            f"redis-cli cluster import exited with status {process.returncode}"
            + (f":\n{diagnostic}" if diagnostic else "")
        )


def import_rdb(redis_module: Any, args: argparse.Namespace, s3: Any, ordinal: int) -> int:
    filename = f"ltk-{ordinal}_of_{RDB_COUNT}.rdb.gz"
    object_key = rdb_object_key(args, ordinal)
    with tempfile.TemporaryDirectory(prefix=f"ltk-rdb-{ordinal:02d}-") as temp_dir_name:
        temp_dir = Path(temp_dir_name)
        compressed_path = temp_dir / filename
        rdb_path = temp_dir / "dump.rdb"
        LOGGER.info("Downloading s3://%s/%s", args.s3_bucket, object_key)
        s3.download_file(args.s3_bucket, object_key, str(compressed_path))
        LOGGER.info("Decompressing %s", filename)
        with gzip.open(compressed_path, "rb") as source, rdb_path.open("wb") as destination:
            shutil.copyfileobj(source, destination, length=8 * 1024 * 1024)
        compressed_path.unlink()

        source_port = reserve_source_port()
        pidfile = temp_dir / "redis.pid"
        logfile = temp_dir / "redis.log"
        subprocess.run(
            [
                args.redis_server,
                "--port",
                str(source_port),
                "--bind",
                "127.0.0.1",
                "--protected-mode",
                "no",
                "--daemonize",
                "yes",
                "--dir",
                str(temp_dir),
                "--dbfilename",
                rdb_path.name,
                "--appendonly",
                "no",
                "--save",
                "",
                "--pidfile",
                str(pidfile),
                "--logfile",
                str(logfile),
            ],
            check=True,
        )
        source_client = None
        try:
            try:
                source_client = wait_for_source(redis_module, source_port, SOURCE_START_TIMEOUT_SEC)
            except RuntimeError as error:
                log_tail = logfile.read_text(errors="replace")[-4_000:] if logfile.is_file() else ""
                if log_tail:
                    raise RuntimeError(f"{error}\ntemporary Redis log tail:\n{log_tail}") from error
                raise
            key_count = int(source_client.dbsize())
            if key_count == 0:
                raise RuntimeError(f"{filename} loaded zero keys")
            LOGGER.info("Importing %s with %d keys into the cluster", filename, key_count)
            run_cluster_import(args, source_port)
            if int(source_client.dbsize()) != 0:
                raise RuntimeError(f"{filename} still has keys after cluster import")
            return key_count
        finally:
            if source_client is not None:
                try:
                    source_client.shutdown(nosave=True)
                except redis_module.exceptions.RedisError:
                    pass
                finally:
                    source_client.close()
            if pidfile.is_file():
                try:
                    pid = int(pidfile.read_text().strip())
                    os.kill(pid, 15)
                except (OSError, ValueError):
                    pass


def main() -> int:
    args = parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)s %(message)s",
    )
    try:
        import boto3
        import redis
    except ImportError as error:
        print(f"error: missing Python dependency: {error.name}", file=sys.stderr)
        return 2

    try:
        nodes = configure_cluster(redis, args)
        assert_cluster_ready(redis, args, nodes)
        s3 = boto3.client("s3")
        verify_rdb_objects(args, s3)
        imported_keys = 0
        ordinals = range(args.start_ordinal, RDB_COUNT + 1)
        LOGGER.info(
            "Importing RDBs %d-%d with %d workers",
            args.start_ordinal,
            RDB_COUNT,
            args.workers,
        )
        with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as executor:
            futures = {
                executor.submit(import_rdb, redis, args, s3, ordinal): ordinal
                for ordinal in ordinals
            }
            try:
                for future in concurrent.futures.as_completed(futures):
                    ordinal = futures[future]
                    key_count = future.result()
                    imported_keys += key_count
                    LOGGER.info(
                        "Completed RDB %d/%d: source keys=%d",
                        ordinal,
                        RDB_COUNT,
                        key_count,
                    )
            except BaseException:
                for future in futures:
                    future.cancel()
                raise
        LOGGER.info("LTK baseline import completed: source keys=%d", imported_keys)
    except Exception as error:  # noqa: BLE001 - top-level diagnostic
        LOGGER.error("LTK database setup failed: %s: %s", type(error).__name__, error)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
