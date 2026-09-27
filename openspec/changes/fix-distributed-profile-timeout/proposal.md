# Prevent distributed aggregate profiling from blocking the main thread

## Why

[MOD-17891](https://redislabs.atlassian.net/browse/MOD-17891) reports that distributed
aggregate profiling under RETURN-STRICT can block Redis's main thread while
serializing shard profiles. The result-ready notification does not establish that
all profile replies are available.

Proposal discussion: [MOD-17891 comment](https://redislabs.atlassian.net/browse/MOD-17891?focusedCommentId=2186300).
Maintainer design approval is pending; this change contains no implementation.

## What Changes

Distributed `FT.PROFILE ... AGGREGATE`, including `LIMITED`, uses RETURN for its
coordinator request when the configured policy is RETURN-STRICT. Global timeout
configuration is preserved. Profiling therefore uses cooperative timeout handling
and can take longer than the requested timeout. It gains no new guarantee of
complete shard profile collection.

Reuse the existing RETURN path. Do not add profile-completion synchronization or
waits. The exact scope and invariants are in the [delta spec](specs/distributed-profile/spec.md).
