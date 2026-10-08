# Migration task: varint

Migrate varint encoding/decoding and its production C consumers to Rust. Propose a coherent implementation and integration boundary; preserve byte representation and ownership behavior.

Starting points: Discover varint implementation, headers, callers and tests at the starting SHA.

Use the shared prompt and migration skill. Derive requirements and scope from the
starting revision, preserve behavior, and flag unresolved compatibility or resource
tradeoffs in a batch. Add missing edge-case tests. Final review is separate.
