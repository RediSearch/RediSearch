#!/usr/bin/env python3
# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).
"""gdb script: sample the stacks of every thread of the attached process.

Run as: SAMPLE_OUT=<file> SAMPLE_SECS=<n> gdb -nx -q -batch -p <pid> -x gdb_sampler.py

A sampling profiler for machines where perf is not available to unprivileged users. One
gdb session stays attached for the whole run, because attaching costs seconds and a fresh
attach per sample would yield too few samples to be useful.
"""
import os
import random
import subprocess
import time

import gdb

out_path = os.environ["SAMPLE_OUT"]
secs = float(os.environ.get("SAMPLE_SECS", "60"))
for cmd in ("set pagination off", "set print address off", "set print frame-arguments none",
            "set confirm off"):
    gdb.execute(cmd)
pid = gdb.selected_inferior().pid
random.seed(1)
end = time.time() + secs
samples = 0
with open(out_path, "w") as out:
    while time.time() < end:
        # SIGINT stops the process under gdb and is not passed on, so the server never
        # sees it. The delay is random so that sampling cannot lock onto a periodic query.
        delay = random.uniform(0.01, 0.05)
        subprocess.Popen(["sh", "-c", "sleep %.3f; kill -INT %d" % (delay, pid)])
        gdb.execute("continue", to_string=True)
        out.write(gdb.execute("thread apply all bt 60", to_string=True))
        out.write("=== sample\n")
        samples += 1
print("samples: %d" % samples)
