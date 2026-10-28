# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

"""Stream Linux resource evidence while a CI step is still reachable."""

from datetime import datetime, timezone
from pathlib import Path
import subprocess
import time


def sample():
    """Record host pressure and container-visible limits without process arguments."""
    print(f"[runner-resources] {datetime.now(timezone.utc).isoformat()}")
    sources = {
        "/proc/loadavg": (),
        "/proc/meminfo": ("MemTotal:", "MemAvailable:", "SwapTotal:", "SwapFree:"),
        "/proc/vmstat": ("oom_kill ",),
        "/proc/pressure/cpu": (),
        "/proc/pressure/memory": (),
        "/proc/pressure/io": (),
        # In a job container these describe its visible cgroup, not necessarily
        # the runner service's cgroup. Host counters above provide separate evidence.
        "/sys/fs/cgroup/memory.current": (),
        "/sys/fs/cgroup/memory.max": (),
        "/sys/fs/cgroup/memory.events": (),
    }
    for source, prefixes in sources.items():
        try:
            lines = Path(source).read_text().splitlines()
            values = [line for line in lines if not prefixes or line.startswith(prefixes)]
            print(f"{source}: {'; '.join(values)}")
        except OSError as error:
            print(f"{source}: unavailable ({error.strerror})")
    try:
        # comm excludes command-line arguments, which can contain credentials.
        processes = subprocess.run(
            ["ps", "-eo", "pid,ppid,comm,rss,pcpu", "--sort=-rss"],
            capture_output=True, text=True, timeout=5, check=True,
        )
        print("\n".join(processes.stdout.splitlines()[:9]))
    except (OSError, subprocess.SubprocessError) as error:
        print(f"process snapshot unavailable ({type(error).__name__})")


if __name__ == "__main__":
    while True:
        sample()
        time.sleep(30)
