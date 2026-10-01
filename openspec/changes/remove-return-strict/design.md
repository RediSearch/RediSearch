# Design

Remove RETURN-STRICT from the configuration enum, parser, and Redis configuration registration. Delete its standalone and coordinator search, aggregate, hybrid, and cursor callbacks, inline fallbacks, result-claim paths, and post-timeout drains.

Preserve the existing RETURN clock deadlines and FAIL blocked-client timeouts, including cursor timeout-budget restoration and request cancellation. Keep shared coordinator-search result ownership and request lifecycle code. Preserve disk-facing structure layouts and legacy loader-handshake entrypoints because disk extensions are built separately; no remaining timeout policy installs that handshake.

Rejecting the removed value makes configuration errors visible. Mapping it to RETURN was rejected because it would silently replace a blocked-client deadline with a clock-based policy. Keeping it as an alias would not remove the option.

Remove strict-only tests. Continue exercising shared stored-reply paths with FAIL, and verify mixed-case rejection through both runtime interfaces and both MODULE LOADEX configuration forms. No persistence or wire-format changes are intended.
