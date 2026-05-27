#!/usr/bin/env python3
"""Replay MS MARCO HSET commands and time index+ingest until idle.

Usage: load.py <csv_path> <num_docs> <redis_port> [mode] [n_threads] [pipeline_size]
  mode: 'live'  - FT.CREATE first, then ingest (default; matches production path)
        'index' - ingest first (no index), then FT.CREATE & time only indexing
"""
import csv, sys, time, threading
import redis

csv.field_size_limit(10 * 1024 * 1024)

csv_path, n_str, port_str = sys.argv[1], sys.argv[2], sys.argv[3]
mode = sys.argv[4] if len(sys.argv) > 4 else "live"
n_threads = int(sys.argv[5]) if len(sys.argv) > 5 else 8
pipe_size = int(sys.argv[6]) if len(sys.argv) > 6 else 200
N = int(n_str)
PORT = int(port_str)
assert mode in ("live", "index"), f"bad mode {mode}"

INDEX = "ms_marco_idx"
SCHEMA = ("FT.CREATE", INDEX, "ON", "HASH", "PREFIX", "1", "doc:", "SCHEMA",
          "url", "TEXT", "title", "TEXT", "WEIGHT", "2.0",
          "headings", "TEXT", "WEIGHT", "1.5",
          "body", "TEXT", "tags", "TAG", "SEPARATOR", ",")


def load_docs():
    out = []
    with open(csv_path, newline="") as f:
        for i, row in enumerate(csv.reader(f)):
            if i >= N:
                break
            if len(row) < 6 or row[3] != "HSET":
                continue
            out.append((row[4], row[5:]))
    return out


def worker(docs, start, stop):
    r = redis.Redis(host="127.0.0.1", port=PORT, decode_responses=False)
    pipe = r.pipeline(transaction=False)
    sent = 0
    for i in range(start, stop):
        key, kv = docs[i]
        pipe.execute_command("HSET", key, *kv)
        sent += 1
        if sent % pipe_size == 0:
            pipe.execute()
            pipe = r.pipeline(transaction=False)
    pipe.execute()


r = redis.Redis(host="127.0.0.1", port=PORT, decode_responses=True)
r.ping()
try:
    r.execute_command("FT.DROPINDEX", INDEX, "DD")
except redis.ResponseError:
    pass
r.flushall()
# disable persistence noise
try: r.config_set("save", "")
except redis.ResponseError: pass
try: r.config_set("appendonly", "no")
except redis.ResponseError: pass

docs = load_docs()
total = len(docs)


def fan_out_ingest():
    threads = []
    chunk = (total + n_threads - 1) // n_threads
    for i in range(n_threads):
        start, stop = i * chunk, min(total, (i + 1) * chunk)
        if start >= stop:
            continue
        t = threading.Thread(target=worker, args=(docs, start, stop))
        t.start()
        threads.append(t)
    for t in threads:
        t.join()


def wait_indexed():
    while True:
        info = r.execute_command("FT.INFO", INDEX)
        d = dict(zip(info[0::2], info[1::2]))
        num_docs = int(d.get("num_docs", 0))
        indexing = int(d.get("indexing", 0))
        if num_docs >= total and indexing == 0:
            return
        time.sleep(0.02)


if mode == "live":
    r.execute_command(*SCHEMA)
    t0 = time.perf_counter()
    fan_out_ingest()
    t_send = time.perf_counter() - t0
    wait_indexed()
    t_total = time.perf_counter() - t0
else:  # index
    fan_out_ingest()  # pre-load docs, untimed
    t0 = time.perf_counter()
    r.execute_command(*SCHEMA)
    wait_indexed()
    t_total = time.perf_counter() - t0
    t_send = 0.0
print(f"mode={mode} docs={total} threads={n_threads} send_s={t_send:.3f} total_s={t_total:.3f}")
