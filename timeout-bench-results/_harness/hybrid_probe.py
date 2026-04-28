"""Probe FT.HYBRID syntax against a running redis-server on $PORT."""
import os, struct, subprocess, sys, time, shutil
import redis

RS = "/tmp/oss-redis/src/redis-server"
SOS = {
    "old": "/home/ubuntu/RediSearch-v8.6.0/bin/linux-x64-release/search-community/redisearch.so",
    "new": "/home/ubuntu/RediSearch/bin/linux-x64-release/search-community/redisearch.so",
}


def zero_vec(dim: int, i: int) -> bytes:
    v = [0.0] * dim
    v[0] = float(i)
    return struct.pack(f"<{dim}f", *v)


def run(tag: str, so: str) -> None:
    port = 17400
    workdir = "/tmp/rs-hybprobe"
    shutil.rmtree(workdir, ignore_errors=True)
    os.makedirs(workdir)
    subprocess.run(
        [RS, "--daemonize", "yes", "--port", str(port),
         "--dir", workdir, "--save", "", "--protected-mode", "no",
         "--enable-module-command", "yes",
         "--loadmodule", so, "WORKERS", "4", "ON_TIMEOUT", "FAIL",
         "--logfile", f"{workdir}/rs.log"],
        check=True)
    time.sleep(1.0)
    r = redis.Redis(host="127.0.0.1", port=port)

    print(f"\n=== {tag} : {so} ===")
    try:
        r.execute_command(
            "FT.CREATE", "hidx",
            "SCHEMA",
            "t", "TEXT",
            "tag", "TAG",
            "v", "VECTOR", "FLAT", "6", "TYPE", "FLOAT32", "DIM", "8",
                          "DISTANCE_METRIC", "COSINE")
        for i in range(1, 21):
            r.execute_command(
                "HSET", f"d:{i}",
                "t", f"hello world number {i}",
                "tag", "tagA" if i % 2 == 0 else "tagB",
                "v", zero_vec(8, i))

        probe_vec = zero_vec(8, 1)

        for label, args in [
            ("SEARCH hello + VSIM PARAMS", [
                "FT.HYBRID", "hidx",
                "SEARCH", "hello",
                "VSIM", "@v", "$vec",
                "PARAMS", "2", "vec", probe_vec,
                "LIMIT", "0", "10", "TIMEOUT", "2000"]),
            ("SEARCH * + VSIM + LOAD 1 t", [
                "FT.HYBRID", "hidx",
                "SEARCH", "*",
                "VSIM", "@v", "$vec",
                "COMBINE", "RRF", "0",
                "LOAD", "1", "t",
                "PARAMS", "2", "vec", probe_vec,
                "LIMIT", "0", "10", "TIMEOUT", "2000"]),
            ("SEARCH * + VSIM + LOAD *", [
                "FT.HYBRID", "hidx",
                "SEARCH", "*",
                "VSIM", "@v", "$vec",
                "LOAD", "*",
                "PARAMS", "2", "vec", probe_vec,
                "LIMIT", "0", "10", "TIMEOUT", "2000"]),
        ]:
            try:
                resp = r.execute_command(*args)
                print(f"  [{label}]: {type(resp).__name__}  "
                      f"len={len(resp) if hasattr(resp,'__len__') else '?'}")
            except redis.ResponseError as e:
                print(f"  [{label}]: ERR {e}")
    finally:
        try:
            r.shutdown(nosave=True)
        except Exception:
            pass
    time.sleep(0.3)


subprocess.run(["pkill", "-f", "redis-server"], check=False)
time.sleep(0.5)
for tag, so in SOS.items():
    run(tag, so)
