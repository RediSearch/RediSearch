#!/usr/bin/env python3
# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).
"""Emit RESP HSET commands for the tag-and-numeric-range benchmark.

usage: gen_dataset.py <num_docs> <planted tag sizes, comma separated> <t0_ms> <span_ms>

The same documents are written twice, so that the only difference between the two key
prefixes is the order in which the documents reach the index:
  a:<n>  inserted in an order unrelated to the timestamp
  b:<n>  inserted in timestamp order

Fields: p (the tag to filter on), e (a group key), ts (timestamp in ms, uniform over the
span), tss (same value, for a SORTABLE field). Each planted size S gives one tag value
"m<S>" carried by exactly S documents; every other document has a unique tag value.
"""
import random
import sys

num_docs = int(sys.argv[1])
planted_sizes = [int(x) for x in sys.argv[2].split(",")]
t0_ms = int(sys.argv[3])
span_ms = int(sys.argv[4])

# Fixed seed: runs must be comparable with each other.
random.seed(18689)
timestamps = [t0_ms + random.randrange(span_ms) for _ in range(num_docs)]

tags = [None] * num_docs
shuffled = list(range(num_docs))
random.shuffle(shuffled)
pos = 0
for size in planted_sizes:
    for i in shuffled[pos:pos + size]:
        tags[i] = "m%d" % size
    pos += size

out = sys.stdout.buffer


def emit(prefix, order):
    write = out.write
    for n, i in enumerate(order):
        ts = str(timestamps[i]).encode()
        args = [
            b"HSET", ("%s%d" % (prefix, n)).encode(),
            b"p", (tags[i] or "u%d" % i).encode(),
            b"e", ("e%d" % (i % 200000)).encode(),
            b"ts", ts,
            b"tss", ts,
        ]
        write(b"*%d\r\n" % len(args) + b"".join(b"$%d\r\n%s\r\n" % (len(a), a) for a in args))


emit("a:", range(num_docs))
emit("b:", sorted(range(num_docs), key=timestamps.__getitem__))
