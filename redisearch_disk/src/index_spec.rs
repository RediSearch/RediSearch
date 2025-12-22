//! This module defines the IndexSpec struct, which represents the specification of an index,
//! including its name and document type. Each `IndexSpec` also has its own unique inverted index
//! mapping terms to postings lists and a document table mapping document IDs to document metadata.

pub mod deleted_ids;
pub mod doc_table;
pub mod inverted_index;

use crate::index_spec::deleted_ids::DeletedIdsStore;
use document::DocumentType;

use crate::search_disk::SpeedbMultithreadedDatabase;

use self::doc_table::DocTable;
use self::inverted_index::InvertedIndex;

/// The type used to represent document keys. A document key is a unique identifier which main
/// Redis can use to refer to a document. In most cases, this is just the document's Redis key.
/// However, in some cases (e.g., when using hashes or JSON documents), the document key may
/// be a composite of the Redis key and a path within the document.
pub type Key = Vec<u8>;

/// Asserts that IndexSpec is Send + Sync. This is important because IndexSpec may be shared
/// across multiple threads in a multithreaded environment.
const _: () = {
    const fn assert_thread_safe<T: Send + Sync>() {}

    assert_thread_safe::<IndexSpec>();
};

/// The IndexSpec struct represents the specification of an index, including its name and document
/// type. It contains an inverted index mapping terms to postings lists and a document table
/// mapping document IDs to document metadata.
pub struct IndexSpec {
    /// The name of the index
    name: String,
    /// The inverted index mapping terms to the inverted index blocks
    inverted_index: InvertedIndex,
    /// The document table mapping document IDs to document metadata
    doc_table: DocTable,
}

impl IndexSpec {
    /// Creates a new IndexSpec with the given name.
    pub fn new(
        name: String,
        document_type: DocumentType,
        database: SpeedbMultithreadedDatabase,
        doc_table_cf_name: String,
        inverted_index_cf_name: String,
        deleted_ids: DeletedIdsStore,
    ) -> Self {
        Self {
            name,
            doc_table: DocTable::new(
                document_type,
                database.clone(),
                doc_table_cf_name,
                deleted_ids,
            ),
            inverted_index: InvertedIndex::new(database, inverted_index_cf_name),
        }
    }

    /// Returns the name of the index.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns a reference to the inverted index for this index.
    pub fn inverted_index(&self) -> &InvertedIndex {
        &self.inverted_index
    }

    /// Returns a reference to the document table for this index.
    pub fn doc_table(&self) -> &DocTable {
        &self.doc_table
    }
}
