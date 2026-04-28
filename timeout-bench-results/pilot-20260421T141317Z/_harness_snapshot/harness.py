"""Minimal harness for timeout benchmark: start SA or OSS cluster, load data."""
from __future__ import annotations
import os
import shutil
import signal
import socket
import subprocess
import time
from contextlib import closing
from dataclasses import dataclass, field

import redis


# Prefer the OSS-upstream redis-server when available. Redis 8.4 ships a
# `rlec_version:-` field in `INFO server` which makes RediSearch's
# `DetectClusterType()` misclassify the instance as Enterprise (type=1) and
# refuse to serve FT commands until an Enterprise topology is set.
_OSS_CANDIDATES = [
    os.environ.get("REDIS_SERVER", ""),
    "/tmp/oss-redis/src/redis-server",
]
REDIS_SERVER = next((p for p in _OSS_CANDIDATES if p and os.path.isfile(p)),
                   shutil.which("redis-server") or "/usr/local/bin/redis-server")


def _port_free(port: int) -> bool:
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        return s.connect_ex(("127.0.0.1", port)) != 0


def _alloc_port(start: int) -> int:
    p = start
    while not _port_free(p):
        p += 1
        if p > start + 2000:
            raise RuntimeError("no free port")
    return p


@dataclass
class Shard:
    port: int
    workdir: str
    proc: subprocess.Popen | None = None

    def conn(self) -> redis.Redis:
        return redis.Redis(host="127.0.0.1", port=self.port, decode_responses=True,
                           socket_timeout=120, socket_connect_timeout=10)


@dataclass
class Cluster:
    module_so: str
    num_shards: int                  # 1 => SA
    workers: int = 4
    base_port: int = 18000
    log_dir: str = "/tmp/timeout-bench-logs"
    shards: list[Shard] = field(default_factory=list)

    # ------------------------------------------------------------------ lifecycle
    def start(self) -> None:
        os.makedirs(self.log_dir, exist_ok=True)
        port = _alloc_port(self.base_port)
        for i in range(self.num_shards):
            port = _alloc_port(port)
            wd = os.path.join(self.log_dir, f"shard-{port}")
            os.makedirs(wd, exist_ok=True)
            s = Shard(port=port, workdir=wd)
            self.shards.append(s)
            self._spawn(s)
            port += 1
        for s in self.shards:
            self._wait_ready(s)
        if self.num_shards > 1:
            self._form_cluster()

    def _spawn(self, s: Shard) -> None:
        logf = os.path.join(s.workdir, "redis.log")
        cmd = [
            REDIS_SERVER,
            "--port", str(s.port),
            "--daemonize", "no",
            "--save", "",
            "--appendonly", "no",
            "--dir", s.workdir,
            "--logfile", logf,
            "--protected-mode", "no",
            "--enable-module-command", "yes",
            "--loadmodule", self.module_so,
            "WORKERS", str(self.workers),
            "ON_TIMEOUT", "FAIL",
        ]
        if self.num_shards > 1:
            cmd += ["--cluster-enabled", "yes",
                    "--cluster-config-file", os.path.join(s.workdir, "nodes.conf"),
                    "--cluster-node-timeout", "5000"]
        s.proc = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    def _wait_ready(self, s: Shard, timeout: float = 20.0) -> None:
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                if s.conn().ping():
                    return
            except Exception:
                time.sleep(0.1)
        raise RuntimeError(f"shard on port {s.port} did not become ready")

    def _form_cluster(self) -> None:
        slots = 16384
        per = slots // self.num_shards
        for i, s in enumerate(self.shards):
            c = s.conn()
            for t in self.shards:
                c.execute_command("CLUSTER", "MEET", "127.0.0.1", t.port)
            start = i * per
            end = slots - 1 if i == self.num_shards - 1 else (i + 1) * per - 1
            c.execute_command("CLUSTER", "ADDSLOTSRANGE", start, end)
        deadline = time.time() + 30
        while time.time() < deadline:
            ok = sum(1 for s in self.shards
                     if "cluster_state:ok" in s.conn().execute_command("CLUSTER", "INFO"))
            if ok == self.num_shards:
                break
            time.sleep(0.2)
        else:
            raise RuntimeError("cluster did not become ok")
        for s in self.shards:
            try:
                s.conn().execute_command("SEARCH.CLUSTERREFRESH")
            except Exception:
                pass
        time.sleep(1.0)

    def stop(self) -> None:
        for s in self.shards:
            try:
                s.conn().shutdown(nosave=True)
            except Exception:
                pass
            if s.proc is not None:
                try:
                    s.proc.send_signal(signal.SIGTERM)
                    s.proc.wait(timeout=5)
                except Exception:
                    s.proc.kill()
        self.shards.clear()

    # ------------------------------------------------------------------ helpers
    def coordinator(self) -> redis.Redis:
        return self.shards[0].conn()

    def all_conns(self) -> list[redis.Redis]:
        return [s.conn() for s in self.shards]
