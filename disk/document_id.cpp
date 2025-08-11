#include "disk/document_id.h"
#include <charconv>
#include <iomanip>
#include "rmutil/rm_assert.h"


namespace search::disk {

void DocumentID::SerializeAsKey(std::ostream& stream) const {
    /** Maximum size for document ID padding in the key */
    static constexpr const size_t MAX_DOC_ID_SIZE = 20;
    // we need to pad with 0s so the lexicographic iterator ordering will be correct
    stream << std::setw(MAX_DOC_ID_SIZE) << std::setfill('0') << id;
}

std::optional<DocumentID> DocumentID::DeserializeFromKey(std::string_view key, std::string_view prefix) {
    // Check if the key starts with the expected prefix
    if (!key.starts_with(prefix)) {
        return std::nullopt;
    }

    // Remove the prefix and parse the remaining part
    std::string_view keyWithoutPrefix = key.substr(prefix.size());
    return DeserializeFromKey(keyWithoutPrefix);
}

std::optional<DocumentID> DocumentID::DeserializeFromKey(std::string_view key) {
    t_docId id = 0;
    std::from_chars_result result = std::from_chars(key.data(), key.data() + key.size(), id);
    if (result.ec != std::errc()) {
        return std::nullopt;
    }
    return DocumentID{id};
}

size_t DocumentID::EstimateCount(rocksdb::Iterator& iter, const rocksdb::Slice& prefix) {
    if (!iter.Valid()) {
        return 0;
    }
    if (!iter.key().starts_with(prefix)) {
        return 0;
    }
    const std::string restorePoint = iter.key().ToString();
    std::string_view startKey = restorePoint;
    startKey.remove_prefix(prefix.size());
    const std::optional<DocumentID> startDocId = DocumentID::DeserializeFromKey(startKey);
    if (!startDocId) {
        return 0;
    }

    std::stringstream ss;
    RS_ASSERT(prefix.size() > 0);
    std::string_view prefixOfPrefix = prefix.ToStringView().substr(0, prefix.size() - 1);
    const char nextChar = prefix[prefix.size() - 1] + 1;
    ss << prefixOfPrefix << nextChar;
    const std::string end = ss.str();

    iter.Seek(end);
    if (!iter.Valid()) {
        iter.SeekToLast();
    } else {
        iter.Prev();
    }

    const bool valid = iter.key().starts_with(prefix);
    std::string lastKey = iter.key().ToString();
    // seek back to where we were
    iter.Seek(restorePoint);
    if (!valid) {
        return 1;
    }
    std::string_view endKey = lastKey;
    endKey.remove_prefix(prefix.size());
    const std::optional<DocumentID> endDocId = DocumentID::DeserializeFromKey(endKey);
    if (!endDocId) {
        return 1;
    }

    return endDocId->id - startDocId->id + 1;
}

} // namespace search::disk