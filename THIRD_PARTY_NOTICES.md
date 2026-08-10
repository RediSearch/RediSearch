# Third-Party Notices

This file records package-specific third-party license usage that must be
reflected in release dependency and SBOM inventories. RediSearch's Rust
workspace license policy is enforced by `cargo deny check licenses` using
`src/redisearch_rs/deny.toml`.

| Package | Version | License | Use |
| --- | --- | --- | --- |
| `priority-queue` | `2.7.0` | `MPL-2.0` | Provides the double-ended priority queue used by `COLLECT DISTINCT` to deduplicate by projected value while retaining the best `SORTBY` representative. |
