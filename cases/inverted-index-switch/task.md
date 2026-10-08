# Migration task: inverted-index-switch

Switch the C inverted-index consumers to the existing Rust implementation at this baseline. Include required integration changes; do not reimplement already available Rust functionality.

Starting points: Discover inverted-index implementations, FFI, consumers, GC, iterators and tests at the starting SHA.

Use the shared prompt and migration skill. Derive requirements and scope from the
starting revision, preserve behavior, and flag unresolved compatibility or resource
tradeoffs in a batch. Add missing edge-case tests. Final review is separate.
