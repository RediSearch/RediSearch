# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

"""Record a write-once C baseline for geo-filter benchmarks on this host."""

import hashlib
import json
from pathlib import Path
import subprocess
import sys


def main():
    stage, binary, baseline_path = sys.argv[1:]
    if stage != "baseline":
        raise ValueError(f"Unknown stage: {stage}")
    path = Path(baseline_path)
    source_hash = hashlib.sha256(Path("src/geo_index.c").read_bytes()).hexdigest()
    if path.exists():
        baseline = json.loads(path.read_text())
        if baseline["source_sha256"] != source_hash:
            raise RuntimeError("The saved baseline is for different C source")
        print(f"Reusing unchanged C baseline: {baseline}")
        return
    result = subprocess.run([binary, "--gtest_filter=GeoIndex.Benchmark"], text=True,
                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    print(result.stdout, end="", flush=True)
    result.check_returncode()
    records = [json.loads(line.removeprefix("GEO_BENCH "))
               for line in result.stdout.splitlines() if line.startswith("GEO_BENCH ")]
    if len(records) != 1:
        raise RuntimeError(f"Expected one measurement, got {len(records)}")
    baseline = records[0]
    if baseline["implementation"] != "c":
        raise RuntimeError("The baseline must execute C")
    baseline["source_sha256"] = source_hash
    with path.open("x") as output:
        json.dump(baseline, output, indent=2)


if __name__ == "__main__":
    main()
