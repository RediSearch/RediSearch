use std::path::{Path, PathBuf};

/// A simple struct that holds the path prefix for database paths.
/// This replaces the old SearchDisk which owned a shared database.
pub struct PathPrefix {
    prefix: PathBuf,
}

impl PathPrefix {
    /// Creates a new PathPrefix with the given path.
    pub fn new(prefix: impl Into<PathBuf>) -> Self {
        Self {
            prefix: prefix.into(),
        }
    }

    /// Returns the prefix as a Path reference.
    pub fn as_path(&self) -> &Path {
        &self.prefix
    }
}
