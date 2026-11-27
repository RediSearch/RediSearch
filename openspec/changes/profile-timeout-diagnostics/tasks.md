# Draft implementation checklist

- [x] Separate execution timeout from cancellation and timer lifetime from worker lifetime.
- [x] Wire PROFILE SEARCH/AGGREGATE on standalone/shards and coordinators.
- [x] Preserve profile collection and internal cursor cleanup after execution timeout.
- [x] Add RESP2/RESP3 regression tests for queued execution, delayed shard profiles, and cancellation.
- [x] Add tests for internal cursor reads, buffered rows, and actual timer expiration.
- [ ] Validate coordinator reduction boundaries and these regression tests in CI.
- [ ] Pass CI build, lint, standalone tests, and coordinator tests.
- [ ] Complete independent review and maintainer review before graduation from draft.
