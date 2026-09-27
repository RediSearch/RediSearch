# Keep distributed aggregate profiling responsive under RETURN-STRICT

## Why

[MOD-17891](https://redislabs.atlassian.net/browse/MOD-17891) reports that distributed
aggregate profiling under RETURN-STRICT can block Redis's main thread while
serializing shard profiles. The result-ready handoff does not establish that all
profile replies are available.

## What Changes

Keep RETURN-STRICT. When printing the distributed aggregate profile, drain only
replies already queued at the start of serialization. Do not wait for outstanding
shards or extend the drain for newly arriving replies. Return the profiles collected
by the pipeline and this bounded drain, which may be incomplete even when the query
did not time out. Global timeout configuration and query results are unchanged.

This follows the maintainer's direction to use nonblocking draining rather than
the earlier request-local RETURN proposal. Discussion belongs on
[MOD-17891](https://redislabs.atlassian.net/browse/MOD-17891), with the implementation
and review in [PR #11569](https://github.com/RediSearch/RediSearch/pull/11569).
