/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#include "query_iterator.hpp"
#include "doc_table.h"
#include "util/timeout.h"

#include <utility>    // std::move
#include <iterator>   // ranges::distance
#include <algorithm>  // ranges::lower_bound

namespace RediSearch {
namespace GeoShape {

auto CPPQueryIterator::base() noexcept -> QueryIterator * {
  return &base_;
}

IteratorStatus CPPQueryIterator::read_single() noexcept {
  if (!has_next()) {
    return ITERATOR_EOF;
  }
  t_docId docId = iter_[index_++];
  const t_fieldIndex fieldIndex = filterCtx_.field.index;
  if (sctx_ && fieldIndex != RS_INVALID_FIELD_INDEX && !DocTable_CheckFieldExpirationPredicate(&sctx_->spec->docs, docId, fieldIndex, filterCtx_.predicate, &sctx_->time.current)) {
    return ITERATOR_NOTFOUND;
  }

  base_.current->docId = docId;
  base_.lastDocId = docId;
  return ITERATOR_OK;
}

IteratorStatus CPPQueryIterator::read() noexcept {
  uint32_t timeoutCounter = 0;
  IteratorStatus rc = ITERATOR_OK;
  do {
    if (TimedOut_WithCounter(&sctx_->time.timeout, &timeoutCounter)) {
      return ITERATOR_TIMEOUT;
    }
    rc = read_single();
  } while (rc == ITERATOR_NOTFOUND);
  return rc;
}
IteratorStatus CPPQueryIterator::skip_to(t_docId docId) {
  if (!has_next()) {
    return ITERATOR_EOF;
  }
  if (docId > iter_.back()) {
    base_.atEOF = true;
    return ITERATOR_EOF;
  }

  const auto it = std::ranges::lower_bound(std::ranges::next(std::ranges::begin(iter_), index_),
                                           std::ranges::end(iter_), docId);
  index_ = std::ranges::distance(std::ranges::begin(iter_), it + 1);
  if (!has_next()) {
    base_.atEOF = true;
  }

  base_.current->docId = *it;
  base_.lastDocId = *it;

  if (*it == docId) {
    return ITERATOR_OK;
  }
  return ITERATOR_NOTFOUND;
}
t_docId CPPQueryIterator::current() const noexcept {
  return base_.current->docId;
}
bool CPPQueryIterator::has_next() const noexcept {
  return index_ < len();
}
std::size_t CPPQueryIterator::len() const noexcept {
  return iter_.size();
}
void CPPQueryIterator::rewind() noexcept {
  base_.atEOF = false;
  base_.current->docId = 0;
  index_ = 0;
}

namespace {
IteratorStatus QIter_Read(QueryIterator *ctx) {
  return reinterpret_cast<CPPQueryIterator *>(ctx)->read();
}
IteratorStatus QIter_SkipTo(QueryIterator *ctx, t_docId docId) {
  return reinterpret_cast<CPPQueryIterator *>(ctx)->skip_to(docId);
}
void QIter_Free(QueryIterator *self) {
  using alloc_type = Allocator::TrackingAllocator<CPPQueryIterator>;
  const auto qi = reinterpret_cast<CPPQueryIterator *const>(self);
  auto alloc = alloc_type{qi->iter_.get_allocator()};
  IndexResult_Free(self->current);
  std::allocator_traits<alloc_type>::destroy(alloc, qi);
  std::allocator_traits<alloc_type>::deallocate(alloc, qi, 1);
}
std::size_t QIter_NumEstimated(QueryIterator *ctx) {
  return reinterpret_cast<CPPQueryIterator const *>(ctx)->len();
}
void QIter_Rewind(QueryIterator *ctx) {
  reinterpret_cast<CPPQueryIterator *>(ctx)->rewind();
}

}  // anonymous namespace

QueryIterator CPPQueryIterator::init_base() {
  return QueryIterator{
      .type = ID_LIST_ITERATOR,
      .atEOF = false,
      .lastDocId = 0,
      .current = NewVirtualResult(0, RS_FIELDMASK_ALL),
      .NumEstimated = QIter_NumEstimated,
      .Read = QIter_Read,
      .SkipTo = QIter_SkipTo,
      .Revalidate = Default_Revalidate,
      .Free = QIter_Free,
      .Rewind = QIter_Rewind,
  };
}
}  // namespace GeoShape
}  // namespace RediSearch
