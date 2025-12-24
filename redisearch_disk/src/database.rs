use std::path::Path;
use std::sync::Arc;

use speedb::{
    ColumnFamilyDescriptor, DBWithThreadMode, Error as SpeedbError, MultiThreaded,
    Options as SpeedbDbOptions,
};

/// Alias to make it easy to refer to multi-threaded Speedb databases.
pub type Speedb = DBWithThreadMode<MultiThreaded>;

/// New type to make it easy to pass around the Speedb database
#[derive(Clone)]
pub struct SpeedbMultithreadedDatabase(Arc<Speedb>);

impl std::ops::Deref for SpeedbMultithreadedDatabase {
    type Target = Speedb;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl SpeedbMultithreadedDatabase {
    /// Opens a Speedb database at the given path with multi-threaded support.
    pub fn open(opts: &SpeedbDbOptions, path: impl AsRef<Path>) -> Result<Self, SpeedbError> {
        let db = Speedb::open(opts, path)?;
        Ok(Self(Arc::new(db)))
    }

    /// Lists the column families in a Speedb database at the given path.
    pub fn list_cf(
        opts: &SpeedbDbOptions,
        path: impl AsRef<Path>,
    ) -> Result<Vec<String>, SpeedbError> {
        Speedb::list_cf(opts, path)
    }

    /// Opens a Speedb database with the given column families at the given path
    pub fn open_cf(
        opts: &SpeedbDbOptions,
        path: impl AsRef<Path>,
        cf_name: impl IntoIterator<Item = impl AsRef<str>>,
    ) -> Result<Self, SpeedbError> {
        let db = Speedb::open_cf(opts, path, cf_name)?;
        Ok(Self(Arc::new(db)))
    }

    /// Opens a Speedb database with the given column family descriptors at the given path.
    /// This allows specifying custom options for each column family.
    pub fn open_cf_descriptors(
        opts: &SpeedbDbOptions,
        path: impl AsRef<Path>,
        cfs: impl IntoIterator<Item = ColumnFamilyDescriptor>,
    ) -> Result<Self, SpeedbError> {
        let db = Speedb::open_cf_descriptors(opts, path, cfs)?;
        Ok(Self(Arc::new(db)))
    }
}
