/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */
#pragma once
#include "disk/document_id.h"
#include "disk/inverted_index/entry_metadata.h"
#include <sstream>

namespace search::disk {

/**
 * @brief Represents a document stored in the disk-based search index
 *
 * The Document structure combines a document ID with its associated metadata.
 * It provides serialization and deserialization methods for storing and retrieving
 * documents from the disk database.
 */
struct Document {
    /** The unique identifier for the document */
    DocumentID docId;

    /** Metadata associated with the document (e.g., field mask) */
    EntryMetadata metadata;

    DocumentID GetID() const { return docId; }
    t_fieldMask GetFieldMask() const { return metadata.fieldMask; }

    /**
     * @brief Serializes the document to a binary stream
     *
     * @param stream The output stream to write the serialized document to
     */
    void Serialize(std::ostream& idStream, std::ostream& metadataStream) const {
        docId.SerializeAsValue(idStream);
        metadata.Serialize(metadataStream);
    }

    /**
     * @brief Deserializes a document from binary data
     *
     * @param docIds String view containing serialized document IDs
     * @param metadata String view containing serialized metadata
     * @return Document The deserialized document
     */
    static Document Deserialize(std::string_view& docIds, std::string_view& metadata) {
        Document doc;
        doc.docId = DocumentID::DeserializeFromValue(docIds);
        doc.metadata = EntryMetadata::Deserialize(metadata);
        return doc;
    }
};

} // namespace search::disk