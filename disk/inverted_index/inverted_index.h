/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */
#pragma once
#include "disk/database.h"
#include "disk/document.h"
#include "disk/inverted_index/entry_metadata.h"
#include <sstream>
#include <iomanip>
#include <boost/endian/conversion.hpp>
#include <rocksdb/slice.h>

namespace rocksdb {
    class Iterator;
};

namespace search::disk {

/**
 * @brief Utility class for storing a single document in the inverted index
 *
 * This class provides static methods for serializing and writing document
 * information to the inverted index. It handles the formatting of keys and
 * values for efficient storage and retrieval.
 */
struct SingleDocument {
    static constexpr const char* KEY_DELIMITER = "_";

    /**
     * @brief Serializes a document ID into a key format
     *
     * Creates a key by appending the document ID to the provided stream,
     * with zero-padding to ensure correct lexicographic ordering.
     *
     * @param stream Output stream to write the key to
     * @param docId Document ID to serialize
     */
    static void SerializeKey(std::ostream& stream, const DocumentID& docId) {
        docId.SerializeAsKey(stream);
    }

    /**
     * @brief Serializes a document into a value format
     *
     * Creates a value by writing the document count (always 1 for single documents)
     * followed by the serialized document data.
     *
     * @param stream Output stream to write the value to
     * @param doc Document to serialize
     */
    static void SerializeValue(std::ostream& stream, const Document& doc) {
        // we write the number of documents in the block
        // this allows us to later compact two blocks together
        // and know how many documents we have in each block
        const uint32_t rawCount = boost::endian::native_to_big(uint32_t(1));
        stream.write(reinterpret_cast<const char*>(&rawCount), sizeof(rawCount));
        doc.Serialize(stream, stream);
    }

    /**
     * @brief Writes a document to the inverted index
     *
     * Creates a key-value pair for the document and writes it to the specified
     * column family with the given prefix.
     *
     * @param col Column family to write to
     * @param prefix Prefix for the key (typically index_term)
     * @param doc Document to write
     * @return true if the write was successful, false otherwise
     */
    static bool Write(Column& col, const std::string& prefix, Document& doc);
};

struct InvertedIndexBlock {
    /** Current version of the serialization format */
    static constexpr const uint8_t VERSION = 1;

    /** Current value from the iterator */
    rocksdb::Slice value_;

    /** Version of the current block */
    uint8_t version_;

    /** View of document IDs in the current block */
    std::string_view docIds_;

    /** View of metadata in the current block */
    std::string_view metadata_;

    /** Cached last ID in the block */
    DocumentID cachedLastId_;

    size_t RemainingDocumentCount() const { return docIds_.size() / sizeof(t_docId); }
    bool Empty() const { return docIds_.size() < sizeof(t_docId) || metadata_.size() < sizeof(t_fieldMask); }
    std::optional<DocumentID> CurrentId() const;
    DocumentID LastId() const;
    std::optional<Document> Next();
    bool SkipTo(DocumentID docId);

    void Reset() {
        docIds_ = std::string_view();
        metadata_ = std::string_view();
    }

    void Advance(size_t count) {
        docIds_.remove_prefix(count * sizeof(t_docId));
        metadata_.remove_prefix(count * sizeof(t_fieldMask));
    }

    static std::optional<InvertedIndexBlock> Deserialize(rocksdb::Slice value);
    static std::string Create(std::ostringstream& ids, std::ostringstream& metadata, uint32_t count) {
        std::ostringstream block;
        block << uint8_t(VERSION);
        const uint32_t bigEndianCount = boost::endian::native_to_big(count);
        block.write(reinterpret_cast<const char*>(&bigEndianCount), sizeof(bigEndianCount));
        block << ids.str() << metadata.str();
        return block.str();
    }
    template <typename DocumentIterator>
    static std::string Create(DocumentIterator begin, DocumentIterator end) {
        const size_t count = std::distance(begin, end);
        // count is expected to be small enough to fit in uint32_t
        std::ostringstream ids;
        std::ostringstream metadata;
        for (auto it = begin; it != end; ++it) {
            it->Serialize(ids, metadata);
        }
        return Create(ids, metadata, count);
    }

    static std::string Create(std::initializer_list<Document> docs) {
        return Create(docs.begin(), docs.end());
    }
};

/**
 * @brief Iterator for traversing documents in the inverted index
 *
 * This class provides methods for iterating through documents associated
 * with a specific index and term in the inverted index.
 */
class InvertedIndexIterator {
public:
    /**
     * @brief Advances to the next document in the inverted index
     * @return The next document, or nullopt if there are no more documents
     */
    std::optional<Document> Next();

    /**
     * @brief Skips to a specific document ID in the inverted index
     * @param docId Document ID to skip to
     * @return The first document with an ID greater than or equal to the specified ID or nullopt if there are no more documents
     */
    std::optional<Document> SkipTo(DocumentID docId);

    /**
     * @brief Rewinds the iterator to the beginning
     */
    void Rewind();

    /**
     * @brief Aborts the iterator
     */
    void Abort();

    /**
     * @brief Returns the ID of the last document read by the iterator
     * @return The ID of the last document read by the iterator, or nullopt if no documents were read
     */
    std::optional<DocumentID> LastDocId() const { return lastDocId_; }

    /**
     * @brief Checks if the iterator has a next document
     * @return true if there is a next document, false otherwise
     */
    bool HasNext() const { return block_.has_value(); }

    /**
     * @brief Creates a new inverted index iterator
     *
     * @param iter RocksDB iterator for the inverted index column family
     * @param index Name of the index to iterate
     * @param term Term within the index to iterate
     * @return A new inverted index iterator, or nullptr if creation failed
     */
    static InvertedIndexIterator* Create(rocksdb::Iterator* iter, const std::string& indexName, const char* term);

    /**
     * @brief Checks if the iterator is valid
     * @return true if the iterator is valid, false otherwise
     */
    bool Valid() const;

    /**
     * @brief Estimates the number of results in the iterator
     */
    size_t EstimateNumResults();
private:
    /**
     * @brief Constructs an inverted index iter     *
     * @param iter RocksDator
B iterator (ownership is transferred)
     * @param prefix Prefix for the keys to iterate (index_term)
     */
    InvertedIndexIterator(std::unique_ptr<rocksdb::Iterator> iter, std::string prefix, InvertedIndexBlock first, size_t countEstimation);
    std::optional<DocumentID> currentId() const;
    std::optional<DocumentID> lastIdFromKey() const;
    void advanceToNextBlock();

    /** Prefix for the keys to iterate */
    std::string prefix_;

    /** Estimated number of documents in the iterator */
    size_t countEstimation_;

    /** Current key from the iterator */
    rocksdb::Slice key_;

    /** Cached last document ID */
    std::optional<DocumentID> lastDocId_;

    /** Current block of data from the iterator */
    std::optional<InvertedIndexBlock> block_;

    /** Underlying RocksDB iterator */
    std::unique_ptr<rocksdb::Iterator> iter_;
};

} // namespace search::disk
