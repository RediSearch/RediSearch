# PROFILE execution timeout

## Changed behavior

A background PROFILE SEARCH or AGGREGATE request using FAIL MUST preserve its profile response when its execution deadline expires. The deadline MUST request execution stop without replying or marking the request cancelled. A timed-out result buffer MUST NOT be returned as successful rows.

The response MUST retain its RESP2 results/profile array or RESP3 Results/Profile map and represent timeout through warnings. Available shard diagnostics, including early shard timeout errors, MUST remain visible. Distributed profile collection MAY continue beyond the execution deadline and MUST remain interruptible by cancellation.

Queued requests whose timer fires MUST retain reply ownership until their worker produces a profile or encounters an independent error. Internal profile cursor reads MUST close a timed-out cursor and return its diagnostics through the existing envelope.

TIMEOUT 0 MUST arm no execution timer. Successful requests MUST retain their existing result/profile format. Ordinary commands, other timeout policies, and PROFILE HYBRID MUST retain their existing behavior.
