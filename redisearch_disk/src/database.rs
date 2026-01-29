use std::mem::ManuallyDrop;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use speedb::{
    ColumnFamilyDescriptor, DBWithThreadMode, Error as SpeedbError, MultiThreaded,
    Options as SpeedbDbOptions,
};
use tracing::{debug, error};

pub use speedb::ColumnFamilyGuard;

/// Alias to make it easy to refer to multi-threaded Speedb databases.
pub type Speedb = DBWithThreadMode<MultiThreaded>;

/// Manages the lifecycle of a Speedb database instance.
///
/// Uses `ManuallyDrop` to ensure the database is closed before deleting files.
/// When dropped, if `marked_for_deletion` is true, destroys the database files.
struct SpeedbDatabaseLifecycle {
    db: ManuallyDrop<Speedb>,
    path: PathBuf,
    marked_for_deletion: AtomicBool,
}

/// New type to make it easy to pass around the Speedb database
#[derive(Clone)]
pub struct SpeedbMultithreadedDatabase(Arc<SpeedbDatabaseLifecycle>);

impl std::ops::Deref for SpeedbMultithreadedDatabase {
    type Target = Speedb;

    fn deref(&self) -> &Self::Target {
        &self.0.db
    }
}

impl SpeedbMultithreadedDatabase {
    /// Opens a Speedb database at the given path with multi-threaded support.
    pub fn open(opts: &SpeedbDbOptions, path: impl AsRef<Path>) -> Result<Self, SpeedbError> {
        let path = path.as_ref();
        let db = Speedb::open(opts, path)?;
        Ok(Self(Arc::new(SpeedbDatabaseLifecycle {
            db: ManuallyDrop::new(db),
            path: path.to_path_buf(),
            marked_for_deletion: AtomicBool::new(false),
        })))
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
        let path = path.as_ref();
        let db = Speedb::open_cf(opts, path, cf_name)?;
        Ok(Self(Arc::new(SpeedbDatabaseLifecycle {
            db: ManuallyDrop::new(db),
            path: path.to_path_buf(),
            marked_for_deletion: AtomicBool::new(false),
        })))
    }

    /// Opens a Speedb database with the given column family descriptors at the given path.
    /// This allows specifying custom options for each column family.
    pub fn open_cf_descriptors(
        opts: &SpeedbDbOptions,
        path: impl AsRef<Path>,
        cfs: impl IntoIterator<Item = ColumnFamilyDescriptor>,
    ) -> Result<Self, SpeedbError> {
        let path = path.as_ref();
        let db = Speedb::open_cf_descriptors(opts, path, cfs)?;
        Ok(Self(Arc::new(SpeedbDatabaseLifecycle {
            db: ManuallyDrop::new(db),
            path: path.to_path_buf(),
            marked_for_deletion: AtomicBool::new(false),
        })))
    }

    /// Marks the database for deletion. When the last reference is dropped,
    /// the database files will be destroyed.
    pub fn mark_for_deletion(&self) {
        self.0.marked_for_deletion.store(true, Ordering::Release);
    }

    /// Returns the raw database pointer for FFI.
    ///
    /// # Safety
    /// The returned pointer is valid for the lifetime of this database.
    /// The caller must not close or destroy the database through this pointer.
    pub fn as_raw_db(&self) -> *mut speedb::ffi_types::rocksdb_t {
        self.0.db.as_raw_db()
    }

    /// Returns the raw column family handle pointer for FFI.
    ///
    /// # Safety
    /// The returned pointer is valid for the lifetime of this database.
    /// The caller must not destroy the column family through this pointer.
    pub fn cf_handle_raw(
        &self,
        name: &str,
    ) -> Option<*mut speedb::ffi_types::rocksdb_column_family_handle_t> {
        use speedb::AsColumnFamilyRef;
        self.cf_handle(name).map(|cf| cf.inner())
    }
}

impl Drop for SpeedbDatabaseLifecycle {
    fn drop(&mut self) {
        // First, manually drop the database to close it
        // Safety: We're in Drop, so this is the last time we'll access this field
        unsafe {
            ManuallyDrop::drop(&mut self.db);
        }

        // Now that the database is closed, check if we should delete the files
        if self.marked_for_deletion.load(Ordering::Acquire) {
            let path = &self.path;
            debug!(path = ?path, "database marked for deletion, destroying files");

            // Destroy the database files
            let opts = SpeedbDbOptions::default();
            if let Err(e) = Speedb::destroy(&opts, path) {
                error!(path = ?path, error = ?e, "failed to destroy database files");
            } else {
                debug!(path = ?path, "database files destroyed successfully");
            }
        }
    }
}
