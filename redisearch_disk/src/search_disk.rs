use document::DocumentType;
use ffi::t_docId;
use std::path::Path;
use std::{fmt::Display, sync::Arc};

use speedb::{
    BlockBasedOptions, Cache, DBWithThreadMode, Error as SpeedbError, MultiThreaded,
    Options as SpeedbDbOptions, SliceTransform,
};

use crate::document_id_key::DocumentIdKey;
use crate::index_spec::deleted_ids::DeletedIdsStore;
use crate::merge_op::DeletedIdsMergeOperator;

/// Trait extension to convert various types into byte slices for use as keys.
pub trait AsKeyExt {
    /// Get this value as a Speedb key.
    fn as_key(&self) -> Vec<u8>;
}

/// Trait extension to convert various types from byte slices for use as keys.
pub trait FromKeyExt {
    /// Construct this value from a Speedb key.
    fn from_key(key: &[u8]) -> Self;
}

/// Trait extension to convert various types into/from byte slices for use as keys.
pub trait KeyExt: AsKeyExt + FromKeyExt {}

impl AsKeyExt for t_docId {
    fn as_key(&self) -> Vec<u8> {
        let doc_id_key: DocumentIdKey = (*self).into();
        doc_id_key.as_key()
    }
}

impl FromKeyExt for t_docId {
    fn from_key(key: &[u8]) -> Self {
        let doc_id_key = DocumentIdKey::from_key(key);
        doc_id_key.as_num()
    }
}

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
}

pub struct SearchDisk {
    // NOTE(enricozb): This type alias specifically uses multi-threaded speedb
    // connection, because the receivers of its methods all take in `&self`.
    // This was done to be better protected against UB in rust caused by
    // obtaining multiple `&mut` references through the search API.
    database: SpeedbMultithreadedDatabase,
}

impl SearchDisk {
    const PREFIX_EXTRACTOR_NAME: &str = "InvertedIndexPrefixExtractor";
    const KEY_DELIMITER: u8 = b'_';
    const DOC_TABLE_CACHE_SIZE: usize = 30 * 1024 * 1024; // 30 MB;
    const DB_WRITE_BUFFER_SIZE: usize = 5 * 1024 * 1024; // 5 MB;
    const DOC_TABLE_COLUMN_FAMILY_NAME: &str = "doc_table";
    const INVERTED_INDEX_COLUMN_FAMILY_NAME: &str = "inverted_index";
    const DOC_TABLE_BLOOM_FILTER_BITS_PER_KEY: f64 = 10.0;

    /// Constructs a new speedb database in `db_path`, which must exist as an
    /// empty directory.
    pub fn new<P: AsRef<Path>>(db_path: P) -> Result<Self, SpeedbError> {
        let mut db_options = SpeedbDbOptions::default();
        db_options.create_if_missing(true);
        db_options.create_missing_column_families(true);
        // FIXME(enricozb): the c++ poc sets
        //   options.drop_read_cache = false;
        // but this doesn't exist in the rust api.
        db_options.set_write_buffer_size(Self::DB_WRITE_BUFFER_SIZE);

        let database = SpeedbMultithreadedDatabase::open(&db_options, db_path)?;

        Ok(Self { database })
    }

    /// Creates a new temporary speedb database.
    #[cfg(test)]
    #[cfg(not(miri))]
    fn new_temp() -> (tempfile::TempDir, Self) {
        let tempdir = tempfile::TempDir::new().unwrap();
        let search_disk = SearchDisk::new(tempdir.path()).unwrap();

        (tempdir, search_disk)
    }

    /// Creates the doc table and inverted indexes column families for an index if they don't already exist.
    pub fn create_column_families_for_index(
        &self,
        index_name: impl Display,
        doc_type: DocumentType,
        deleted_ids: DeletedIdsStore,
    ) -> Result<(String, String), SpeedbError> {
        let (doc_table_cf_name, doc_table_cf_options) =
            Self::doc_table_column_family(&index_name, doc_type);
        let (inverted_index_cf_name, inverted_index_cf_options) =
            Self::inverted_index_column_family(&index_name, doc_type, deleted_ids);

        if self.database.cf_handle(&doc_table_cf_name).is_none() {
            self.database
                .create_cf(doc_table_cf_name.clone(), &doc_table_cf_options)?;
        }
        if self.database.cf_handle(&inverted_index_cf_name).is_none() {
            self.database
                .create_cf(inverted_index_cf_name.clone(), &inverted_index_cf_options)?;
        }

        Ok((doc_table_cf_name, inverted_index_cf_name))
    }

    /// Returns the underlying Speedb database.
    pub fn database(&self) -> SpeedbMultithreadedDatabase {
        self.database.clone()
    }

    /// Returns the column family name and database options for an index's
    /// `doc_table` column family.
    fn doc_table_column_family(
        index_name: impl Display,
        doc_type: DocumentType,
    ) -> (String, SpeedbDbOptions) {
        let mut block_based_options = BlockBasedOptions::default();
        let block_cache = Cache::new_lru_cache(Self::DOC_TABLE_CACHE_SIZE);
        block_based_options.set_block_cache(&block_cache);

        // the second parameter is ignored
        // see: https://github.com/facebook/rocksdb/blob/35148aca91cda84d6fa9b295eb5500d6d965dca6/include/rocksdb/filter_policy.h#L155
        block_based_options.set_bloom_filter(Self::DOC_TABLE_BLOOM_FILTER_BITS_PER_KEY, false);
        block_based_options.set_cache_index_and_filter_blocks(true);
        // FIXME(enricozb): the c++ poc sets
        //   blockBasedOptions.block_align = true;
        // but this doesn't exist in the rust api.
        let mut cf_options = SpeedbDbOptions::default();
        cf_options.set_block_based_table_factory(&block_based_options);

        let cf_name =
            Self::column_family_name(index_name, doc_type, Self::DOC_TABLE_COLUMN_FAMILY_NAME);

        (cf_name, cf_options)
    }

    /// Returns the column family name and database options for an index's
    /// `inverted_index` column family.
    fn inverted_index_column_family(
        index_name: impl Display,
        doc_type: DocumentType,
        deleted_ids: DeletedIdsStore,
    ) -> (String, SpeedbDbOptions) {
        let index_name = index_name.to_string();
        let mut cf_options = SpeedbDbOptions::default();
        cf_options.set_merge_operator(
            &index_name,
            DeletedIdsMergeOperator::full_merge_fn(deleted_ids.clone()),
            DeletedIdsMergeOperator::partial_merge_fn(deleted_ids),
        );
        cf_options.set_prefix_extractor(Self::inverted_index_prefix_extractor());
        cf_options.set_block_based_table_factory(&BlockBasedOptions::default());

        let cf_name = Self::column_family_name(
            index_name,
            doc_type,
            Self::INVERTED_INDEX_COLUMN_FAMILY_NAME,
        );

        (cf_name, cf_options)
    }

    /// Returns the column family name for an index.
    fn column_family_name(
        index_name: impl Display,
        doc_type: DocumentType,
        column_family: &'static str,
    ) -> String {
        format!("{index_name}:{doc_type}:{column_family}")
    }

    /// Returns a prefix extractor that strips the final doc_id from the indexes
    /// in speedb: `"foo_40" -> "foo_"`.
    fn inverted_index_prefix_extractor() -> SliceTransform {
        SliceTransform::create(
            Self::PREFIX_EXTRACTOR_NAME,
            strip_after_last_delimiter,
            Some(contains_key_delimiter),
        )
    }
}

/// Strip everything after the last [`Self::KEY_DELIMITER`], which is the key
/// without the last doc_id, but keeping the delimiter.
fn strip_after_last_delimiter(src: &[u8]) -> &[u8] {
    let last_key_delimiter_idx = src
        .iter()
        .rposition(|byte| *byte == SearchDisk::KEY_DELIMITER)
        .expect("slice doesn't contain KEY_DELIMITER");

    &src[..last_key_delimiter_idx + 1]
}

/// Returns whether `src` contains a [`SearchDisk::KEY_DELIMITER`].
fn contains_key_delimiter(src: &[u8]) -> bool {
    src.contains(&SearchDisk::KEY_DELIMITER)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[cfg(not(miri))]
    fn test_db_basic() {
        let (_tempdir, search_disk) = SearchDisk::new_temp();

        search_disk
            .create_column_families_for_index("foo", DocumentType::Hash, DeletedIdsStore::new())
            .unwrap();

        let db_path = search_disk.database.path();
        let column_families =
            SpeedbMultithreadedDatabase::list_cf(&SpeedbDbOptions::default(), db_path).unwrap();

        assert_eq!(
            column_families,
            ["default", "foo:hash:doc_table", "foo:hash:inverted_index"]
        );
    }

    #[test]
    fn test_strip_after_last_delimiter_basic() {
        let key = b"foo_40";
        let expected = b"foo_";
        assert_eq!(strip_after_last_delimiter(key), expected);
    }

    #[test]
    fn test_strip_after_last_delimiter_multiple_delimiters() {
        let key = b"alpha_beta_gamma_123";
        let expected = b"alpha_beta_gamma_";
        assert_eq!(strip_after_last_delimiter(key), expected);
    }

    #[test]
    #[should_panic(expected = "slice doesn't contain KEY_DELIMITER")]
    fn test_strip_after_last_delimiter_panics_without_delimiter() {
        strip_after_last_delimiter(b"nodelimiterhere");
    }
}
