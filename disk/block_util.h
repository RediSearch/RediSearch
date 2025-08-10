#pragma once
#include <system_error>
#include <boost/endian/conversion.hpp>
#include "disk/document_id.h"
#include "rmutil/rm_assert.h"
#include <algorithm>

namespace search::disk {
template <typename T, typename U = T>
static std::error_code ReadElement(std::string_view& buffer, T& element) {
    if (buffer.size() < sizeof(T)) {
        return std::make_error_code(std::errc::message_size);
    }
    const U* elements = reinterpret_cast<const U*>(buffer.data());
    const U value = boost::endian::big_to_native(elements[0]);
    element = reinterpret_cast<const T&>(value);
    buffer.remove_prefix(sizeof(T));
    return std::error_code();
}

// Caller should verify buffer size is at least sizeof(t_docId) and aligned to it
static DocumentID DeserializeLastId(std::string_view& buffer) {
    std::string_view last = buffer.substr(buffer.size() - sizeof(t_docId));
    return DocumentID::DeserializeFromValue(last);
}

template <typename BlockType>
bool SkipToDocumentId(BlockType& block, std::string_view docIds, DocumentID docId) {
    const auto bigEndianLess = [] (t_docId rawLeft, t_docId rawRight) -> bool
    {
        const t_docId left = boost::endian::big_to_native(rawLeft);
        const t_docId right = boost::endian::big_to_native(rawRight);
        return left < right;
    };
    const t_docId* start = reinterpret_cast<const t_docId*>(docIds.data());
    const t_docId* end = reinterpret_cast<const t_docId*>(docIds.data() + docIds.size());
    const t_docId bigEndianDocId = boost::endian::native_to_big(docId.id);
    const t_docId* it = std::lower_bound(start, end, bigEndianDocId, bigEndianLess);
    if (it == end) {
        // reached the end of the block and we didn't find a larger docId
        // shouldn't really happen since the block last id should be bigger than docId
        // only option is we ran out of docIds
        block.Reset();
        RS_ABORT("reached the end of the block, caller should have checked doc id was in block and block was not empty in advance");
        return false;
    }
    const size_t index = std::distance(start, it);
    if (index) {
        block.Advance(index);
    }
    return true;
}

// We have an array of indexes which provide ranges into the data array
// indexes: [4, 7, 10]
// data: "hellosamram"
// [0, 4]: "hello"
// [4, 7]: "sam"
// [7, 10]: "ram"DynamicDataView
// the indexes are uint32_t and are in big endian format
struct DynamicDataView {
    size_t prevPosition;
    std::string_view indexes;
    std::string_view data;

    DynamicDataView(std::string_view indexes, std::string_view data) : prevPosition(0), indexes(indexes), data(data) {}

    void Reset() {
        prevPosition = 0;
        indexes = std::string_view();
        data = std::string_view();
    }

    std::string_view Next() {
        if (indexes.size() < sizeof(uint32_t)) {
            return std::string_view();
        }

        const uint32_t* bigEndianIndexes = reinterpret_cast<const uint32_t*>(indexes.data());
        const uint32_t endPosition = boost::endian::big_to_native(bigEndianIndexes[0]);

        const std::string_view result = data.substr(0, endPosition - prevPosition);
        RS_ASSERT(!result.empty());
        RS_ASSERT(result[0] != '\0');
        indexes.remove_prefix(sizeof(uint32_t));
        data.remove_prefix(result.size());
        prevPosition = endPosition;
        return result;
    }

    void Advance(size_t index) {
        if (index == 0) {
            return;
        }
        const size_t elementCount = index + 1;
        const size_t byteCount = elementCount * sizeof(uint32_t);
        RS_ASSERT(byteCount < indexes.size());
        if (byteCount >= indexes.size()) {
            return;
        }

        // e.g: 
        // indexes: [4, 7, 10, 13]
        // data: "hellosamramdam"
        // prevPosition: 0
        // elementCount: 2
        // indexes we want to remove: [4, 7]
        // data we want to remove: "hellosam"
        // prevPosition after: 7 -> want to compute the length of "ram" when Next will be called
        const uint32_t* bigEndianIndexes = reinterpret_cast<const uint32_t*>(indexes.data());
        const uint32_t startIndex = boost::endian::big_to_native(bigEndianIndexes[0]);
        // problem is that start index relies on the first element
        // advancing by elementCount means we need to take the element at bigEndianIndexes[elementCount + 1]
        const uint32_t endIndex = boost::endian::big_to_native(bigEndianIndexes[elementCount]);
        prevPosition = boost::endian::big_to_native(bigEndianIndexes[elementCount - 1]);
        indexes.remove_prefix(elementCount * sizeof(uint32_t));
        data.remove_prefix(endIndex - startIndex);
    }
};

}