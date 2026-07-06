# Third-Party Notices

This file records package-specific third-party license usage that must be
reflected in release dependency and SBOM inventories. RediSearch's Rust
workspace license policy is enforced by `cargo deny check licenses` using
`src/redisearch_rs/deny.toml`.

| Package | Version | License | Use |
| --- | --- | --- | --- |
| `cbindgen` | `0.29.2` | `MPL-2.0` | Generates C headers from Rust FFI crates for the C integration layer. |
