pub mod ffi;

#[cfg(test)]
mod tests {
    #[test]
    // `cargo llvm-cov` will fail to generate a coverage report if there
    // are no tests.
    // Adding a placeholder test to work around the issue.
    // We can delete this placeholder once we merge Rust code
    // with actual tests.
    fn dummy() {}
}
