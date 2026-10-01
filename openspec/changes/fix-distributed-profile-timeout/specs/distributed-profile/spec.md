# Distributed aggregate profiles under RETURN-STRICT

When a distributed `FT.PROFILE ... AGGREGATE` request uses RETURN-STRICT, profile
serialization MUST NOT wait for additional shard replies. It MUST consume available
replies using nonblocking pops until the first empty pop or EOF, including replies
that arrive during the drain. It MUST NOT impose a queue-length snapshot limit.

The reply includes profiles already collected by the pipeline and this nonblocking
drain. Shard profile information may be incomplete even without a timeout, for
example after an early LIMIT. Full and LIMITED profiling use the same rule.

RETURN-STRICT and the effective request timeout MUST remain in force. Global
ON_TIMEOUT and TIMEOUT MUST remain unchanged. Ordinary query result semantics,
command syntax, and RESP2/RESP3 reply shapes are unchanged. Other timeout policies
retain their existing behavior.

## Acceptance scenarios

- Distributed STRICT aggregate profiling completes on a healthy cluster and Redis
  responds to PING on every shard afterward.
- Both full and LIMITED profiles work with RESP2 and RESP3, with disabled or finite
  query timeouts and with early LIMIT or full aggregation.
- Query results are correct and the profile envelope remains valid even when
  some shard profiles are absent.
- Global ON_TIMEOUT and TIMEOUT are identical before and after these requests.

- Replies enqueued during the drain remain eligible until the first empty pop.
