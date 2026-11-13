# Third-Party Notices

This file records package-specific third-party license usage that must be
reflected in release dependency and SBOM inventories. RediSearch's Rust
workspace license policy is enforced by `cargo deny check licenses` using
`src/redisearch_rs/deny.toml`.

| Package | Version | License | Use |
| --- | --- | --- | --- |
| `priority-queue` | `2.7.0` | `MPL-2.0` | Provides the double-ended priority queue used by `COLLECT DISTINCT` to deduplicate by projected value while retaining the best `SORTBY` representative. |

## Scalable Vector Search (SVS)

VecSim uses SVS at commit `919925a56218da10ee4ae0cb8a2124ee96fdf3fd`.
The source checkout in `deps/ScalableVectorSearch` is licensed under Apache-2.0;
its [license](deps/ScalableVectorSearch/LICENSE) remains with the dependency.

Builds using the precompiled SVS distribution instead use binaries covered by
the Intel Simplified Software License (October 2022), included as `LICENSE` in
the downloaded archive. That distribution also includes dependency notices
under `share/doc/`. Retain those license and notice files when redistributing
the binaries; the Apache-2.0 source license does not replace the binary license.
The migration changes neither the SVS revision nor the selected distribution.
