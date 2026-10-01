# Disk metrics in Redis INFO

With matching RediSearch Enterprise and Flex Redis BigModule V2 support, Redis INFO reads
background disk snapshots instead of collecting native properties on its caller.
FT.INFO and operational quota checks retain their live paths. RAM accounting,
index walks, existing locks and formatting are unchanged.

Cold Search disk fields report zero until sampled. Last-good fields survive
collection errors, retaining their timestamps. Consumers should inspect the
search_disk_metrics_cache readiness, age and error fields before interpreting
zero or old values. Samples are not atomic across column families or indexes.

V2 rejection selects the legacy synchronous path. After successful V2
negotiation, failed or delayed collection retains cached values. Redis owns
scheduling through its dedicated Flex metrics BIO worker; RediSearch cron never
collects native metrics. Drop/shutdown retire targets before native handle
destruction.

The matching RediSearch Enterprise implementation documents the complete field
contract in docs/info-metrics.md and carries executable cross-repository
regressions under tests/info_snapshots. Those tests drive these RediSearch
callbacks through actual INFO,
FT.INFO, drop/recreate, failed/successful fork and reopen operations. Run them with
the matching Flex Redis and RediSearch Enterprise artifacts; a standalone
RAM-only RediSearch test does not exercise the disk API.

BigModule V2 registers a single `collectMetrics(void)` callback. Redis invokes it
on BIO, stops submissions and drains the worker before lifecycle changes.
RediSearch forwards to a separately allocated RSE collector context; it does not
handle pause/resume/fork actions or rebuild targets on cron. Both sides must use
the matching unmerged V2 layout. RSE and Flex have no private metrics threads.

The BigModule cached-usage getter reads one module-wide total published by RSE's
background pass. It is O(1), without index iteration or native property reads.
The broader Search INFO sections still aggregate per-index snapshots and RAM
statistics. Live quota/admission usage remains separate.
