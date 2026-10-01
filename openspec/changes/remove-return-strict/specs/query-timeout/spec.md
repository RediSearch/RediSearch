# Query timeout policy delta

## Removed requirement: RETURN-STRICT

On 8.8-rse, RETURN-STRICT is not an accepted ON_TIMEOUT or search-on-timeout value. Startup/module-load configuration and runtime updates reject it, regardless of letter case. A rejected runtime update preserves the previous policy.

## Preserved requirements

RETURN and FAIL remain accepted, with their existing defaults and execution semantics. This includes standalone and distributed search, aggregation, hybrid queries, profiling, and cursor reads. The removal does not change persisted index data or successful reply formats.
