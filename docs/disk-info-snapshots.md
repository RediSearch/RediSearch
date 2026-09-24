# Disk metrics in Redis INFO

With matching RediSearch Enterprise and Flex Redis BigModule V3 support, Redis INFO reads
background disk snapshots instead of collecting native properties on its caller.
FT.INFO and operational quota checks retain their live paths. RAM accounting,
index walks, existing locks and formatting are unchanged.

Cold Search disk fields report zero until sampled. Last-good fields survive
collection errors, retaining their timestamps. Consumers should inspect the
search_disk_metrics_cache readiness, age and error fields before interpreting
zero or old values. Samples are not atomic across column families or indexes.

V3 rejection selects the legacy synchronous path. After successful V3
negotiation, worker failure leaves cached degraded mode; cron retries startup
and never collects native metrics. Resume schedules target rebuilding on cron
after the callback stack has unwound. Drop/shutdown retire targets before native
handle destruction.

The matching RediSearch Enterprise implementation documents the complete field
contract in docs/info-metrics.md and carries executable cross-repository
regressions under tests/info_snapshots. Those tests drive these RediSearch
callbacks through actual INFO,
FT.INFO, drop/recreate, failed/successful fork and reopen operations. Run them with
the matching Flex Redis and RediSearch Enterprise artifacts; a standalone
RAM-only RediSearch test does not exercise the disk API.
