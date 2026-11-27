# Return profiling diagnostics after the execution deadline

## Why

`FT.PROFILE` with `ON_TIMEOUT FAIL` can lose its diagnostic output when a blocked-client timeout replies with an error. A slow query is precisely when those diagnostics are useful. This draft implements the timer-based direction selected for [MOD-18986](https://redislabs.atlassian.net/browse/MOD-18986).

## What changes

For PROFILE SEARCH and AGGREGATE, expiration signals execution to stop without completing the client reply. The normal worker flow returns the existing profile envelope and timeout warnings. FAIL continues to buffer query rows and discards them when execution times out. Profile collection can outlive `TIMEOUT`.

Scope includes standalone/shard requests, distributed coordinators, and internal aggregate cursor reads on `8.10`. PROFILE HYBRID remains outside this draft. There are no new public arguments, configuration options, or persistence changes.
