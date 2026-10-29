# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

"""Gate comparable geo-filter batches against a baseline from the same host.

The baseline is write-once: later stages cannot silently replace the C measurements.
A regression exceeds both 20% and 5 ns/op, allowing timer noise for tiny operations.
"""

import hashlib
import json
import math
import os
from pathlib import Path
import subprocess
import sys


def measure(command, env=None):
    """Retain all output while extracting the benchmark's machine-readable record."""
    result = subprocess.run(command, env=env, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    print(result.stdout, end="", flush=True)
    result.check_returncode()
    records = [json.loads(line.removeprefix("GEO_BENCH "))
               for line in result.stdout.splitlines() if line.startswith("GEO_BENCH ")]
    if len(records) != 1:
        raise RuntimeError(f"Expected one measurement, got {len(records)}")
    return records[0]


def main():
    stage, binary, baseline_path = sys.argv[1:]
    path = Path(baseline_path)
    if stage != "swap":
        raise ValueError(f"Unknown stage: {stage}")
    # The swap deletes the C source; its parent is the admitted FFI stage.
    source = subprocess.check_output(["git", "show", "HEAD^:src/geo_index.c"])
    baseline = json.loads(path.read_text())
    if baseline["source_sha256"] != hashlib.sha256(source).hexdigest():
        raise RuntimeError("The saved baseline is for different C source")
    retained = path.with_suffix(".ffi-baseline")
    # Earlier stages retain their drivers in Git; after the swap C lives only in these artifacts.
    env = os.environ.copy()
    env["DYLD_LIBRARY_PATH" if sys.platform == "darwin" else "LD_LIBRARY_PATH"] = str(retained.resolve())
    current_c = measure([str(retained / "rstest"), "--gtest_filter=GeoIndex/GeoFilterTest.Benchmark/0"], env)
    if current_c["implementation"] != "c":
        raise RuntimeError("The baseline executable must execute C")
    candidate = measure([binary, "--gtest_filter=GeoIndex.Benchmark"])
    if candidate["implementation"] != "rust":
        raise RuntimeError("The candidate must execute Rust")
    failures = []
    for case in ("parse", "validate", "radius", "lifecycle"):
        old, new = baseline[case], candidate[case]
        if not (math.isfinite(old) and math.isfinite(new) and old > 0 and new > 0):
            raise RuntimeError(f"Invalid timing for {case}: {old}, {new}")
        limit = max(old * 1.20, old + 5)
        print(f"{case}: C {old:.2f} ns/op; Rust {new:.2f} ns/op; ratio {new / old:.3f}; limit {limit:.2f}")
        print(f"rust-port-bench name={case} c={current_c[case]} rust={new}")
        if new > limit:
            failures.append(case)
    if failures:
        raise RuntimeError(f"Performance regression: {', '.join(failures)}")


if __name__ == "__main__":
    main()
