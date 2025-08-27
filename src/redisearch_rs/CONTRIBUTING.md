## Dependencies

Dependencies should be added to the `Cargo.toml` file in the root of the workspace.
They can then be used in the crates `Cargo.toml` files using:

```toml
[dependencies]
thiserror.workspace = true
```

They should be updated:
- ASAP in case of advisories security vulnerabilities.
- whenever we need newer features or bug fixes from the new version.
- once in a while if neither of the two things above have happened.