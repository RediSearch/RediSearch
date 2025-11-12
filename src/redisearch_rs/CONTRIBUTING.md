## Dependencies

Dependencies should be added to the `Cargo.toml` file in the root of the workspace.
They can then be used by workspace members via:

```toml
[dependencies]
thiserror.workspace = true
```

Dependency versions should be updated:
- ASAP in case of security advisories.
- Whenever we need newer features or bug fixes released in a newer version.
- Once in a while, via [`cargo upgrade`](https://crates.io/crates/cargo-edit), if neither of the two things above have happened.