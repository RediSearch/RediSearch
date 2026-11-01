"""Measure one isolated CI phase without changing its compiler/test settings."""

import csv
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time
import urllib.request

phase = sys.argv[1]
command = sys.argv[3:]
assert sys.argv[2] == '--' and phase in ('prepare', 'rust') and command
out = Path('performance-results')
out.mkdir(exist_ok=True)
metadata = {key: os.environ.get(key) for key in (
    'BENCH_INSTANCE', 'BENCH_JOBS', 'BENCH_REPEAT', 'BENCH_IMAGE',
    'GITHUB_SHA', 'RUNNER_NAME', 'CARGO_BUILD_JOBS', 'LTO', 'RUSTFLAGS',
)}
# IMDS only identifies the experimental machine; no credential endpoint is accessed.
try:
    base = 'http://169.254.169.254/latest/'
    req = urllib.request.Request(base + 'api/token', method='PUT',
                                 headers={'X-aws-ec2-metadata-token-ttl-seconds': '60'})
    token = urllib.request.urlopen(req, timeout=2).read().decode()
    for key in ('instance-type', 'ami-id', 'placement/availability-zone'):
        req = urllib.request.Request(base + 'meta-data/' + key,
                                     headers={'X-aws-ec2-metadata-token': token})
        metadata[key] = urllib.request.urlopen(req, timeout=2).read().decode()
except Exception as error:
    metadata['metadata_error'] = type(error).__name__
if 'instance-type' in metadata and metadata['instance-type'] != os.environ['BENCH_INSTANCE']:
    raise SystemExit('Runner instance type does not match the requested experiment')
metadata['cpu_count'] = os.cpu_count()
metadata['mem_total_kib'] = int(next(line.split()[1] for line in Path('/proc/meminfo').read_text().splitlines() if line.startswith('MemTotal:')))
expected_gib = 16 if os.environ['BENCH_INSTANCE'] == 'm6i.xlarge' else 32
if not expected_gib * 0.9 < metadata['mem_total_kib'] / 1048576 < expected_gib * 1.01:
    raise SystemExit('Runner memory does not match the requested experiment')
if metadata['cpu_count'] != 4:
    raise SystemExit('Experiment requires four visible CPUs')
(out / (phase + '-environment.json')).write_text(json.dumps(metadata, indent=2))

started = time.monotonic()
minimum_available = None
peak_anon = 0
timed_out = False
with (out / (phase + '.log')).open('w') as log, (out / (phase + '-samples.csv')).open('w') as samples:
    writer = csv.writer(samples)
    writer.writerow(['elapsed_s', 'mem_available_kib', 'anon_pages_kib', 'dirty_kib', 'load1', 'disk_read_sectors'])
    process = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
    next_report = 0
    while True:
        elapsed = time.monotonic() - started
        memory = {line.split(':')[0]: int(line.split()[1])
                  for line in Path('/proc/meminfo').read_text().splitlines()}
        available = memory['MemAvailable']
        minimum_available = available if minimum_available is None else min(minimum_available, available)
        peak_anon = max(peak_anon, memory['AnonPages'])
        disk_reads = sum(int(fields[5]) for fields in (line.split() for line in Path('/proc/diskstats').read_text().splitlines()) if (Path('/sys/block') / fields[2]).exists())
        writer.writerow([round(elapsed, 2), available, memory['AnonPages'], memory['Dirty'], os.getloadavg()[0], disk_reads])
        samples.flush()
        if elapsed >= next_report:
            print(f'{phase}: {elapsed:.0f}s, available={available / 1048576:.2f} GiB', flush=True)
            next_report = elapsed + 60
        if process.poll() is not None:
            break
        if elapsed > 100 * 60:
            timed_out = True
            os.killpg(process.pid, signal.SIGTERM)
            try:
                process.wait(timeout=15)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait()
            break
        time.sleep(5)
result = dict(metadata, phase=phase, elapsed_seconds=time.monotonic() - started,
              exit_code=process.returncode, timed_out=timed_out,
              min_available_kib=minimum_available, peak_anon_kib=peak_anon)
(out / (phase + '-result.json')).write_text(json.dumps(result, indent=2))
print(json.dumps(result, indent=2))
print('\n'.join((out / (phase + '.log')).read_text(errors='replace').splitlines()[-60:]))
raise SystemExit(124 if timed_out else process.returncode)
