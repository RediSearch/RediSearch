# Lazy key-map backfill measurements

Compared the historical-ID scan with direct bucket-chain traversal and background
capacity reservation. Runs used WORKERS 0 and 2, with and without a concurrent
writer updating an indexed numeric field. A query client repeatedly searched one
indexed key with INKEYS and checked the result throughout construction.

The workloads were one million live documents; 100,000 live documents after two
million assigned IDs; and 200,000 live documents with a 1,000-bucket limit to
exercise long collision chains. The baseline ran once per combination and the
revised implementation twice; the figures below use the worst observed values.

## Results

- Worst long-chain build: 1.70 seconds before, 23.2 milliseconds after.
- Maximum batch execution time under the spec write lock across all workloads:
  26.4 milliseconds before, 1.6 milliseconds after. Lock acquisition wait is excluded.
- Churn workload build: up to 12.1 milliseconds before, 6.2 milliseconds after.
- The revised million-document build took up to 58 milliseconds.
- The slowest revised first query took 27.1 milliseconds. Query and write p99
  after readiness remained below 0.3 milliseconds in this workload.

Direct traversal removes repeated collision-chain lookups and scanning historical
ID gaps. Reserving capacity avoids repeated large rehashes during backfill. These
results motivated replacing the original backfill implementation.

## Setup and limitations

Measurements used an Intel Core Ultra 7 255U on Linux x86-64 with CPU
energy-performance preference set to performance, Redis development commit
`559d143b` with jemalloc, and release-optimized module builds with ENABLE_ASSERT.
GC was disabled to isolate backfill. Each case used a fresh Redis process; the
writer ran from 300 milliseconds before activation until 300 milliseconds after
readiness. Temporary timing instrumentation was used only for measurement.

This was a single-query-client/single-writer experiment, not a production latency
SLO. Short construction windows produced few client samples. Repeated lock
acquisitions can delay a client longer than one batch, and allocation, page faults,
concurrent growth, and a busy reindex pool can increase latency. Batching bounds
traversal work, not wall-clock lock time. First use remains a tradeoff for avoiding
the map entirely on indexes that never use INKEYS.
