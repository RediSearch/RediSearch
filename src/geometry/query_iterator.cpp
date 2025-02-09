/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "query_iterator.hpp"
#include "doc_table.h"
#include "util/timeout.h"

#include <utility>    // std::move
#include <iterator>   // ranges::distance
#include <algorithm>  // ranges::lower_bound

namespace RediSearch {
namespace GeoShape {

auto QueryIterator::base() noexcept -> IndexIterator * {
  return &base_;
}

int QueryIterator::read_single(RSIndexResult *&hit) noexcept {
  if (!base_.isValid) {
    return INDEXREAD_EOF;
  }
  t_docId docId = iter_[index_++];
  const t_fieldIndex fieldIndex = filterCtx_.field.value.index;
  if (sctx_ && fieldIndex != RS_INVALID_FIELD_INDEX && !DocTable_VerifyFieldExpirationPredicate(&sctx_->spec->docs, docId, &fieldIndex, 1, filterCtx_.predicate, &sctx_->time.current)) {
    return INDEXREAD_NOTFOUND;
  }

  base_.current->docId = docId;
  hit = base_.current;
  return INDEXREAD_OK;
}

int QueryIterator::read(RSIndexResult *&hit) noexcept {
  if (index_ >= len()) {
    base_.isValid = false;
    return INDEXREAD_EOF;
  }
  size_t timeoutCounter = 0;
  int rc = INDEXREAD_OK;
  do {
    if (TimedOut_WithCounter(&sctx_->time.timeout, &timeoutCounter)) {
      return INDEXREAD_TIMEOUT;
    }
    rc = read_single(hit);
  } while (rc == INDEXREAD_NOTFOUND);
  return rc;
}
int QueryIterator::skip_to(t_docId docId, RSIndexResult *&hit) {
  if (!base_.isValid) {
    return INDEXREAD_EOF;
  }
  if (docId > iter_.back()) {
    base_.isValid = false;
    return INDEXREAD_EOF;
  }

  const auto it = std::ranges::lower_bound(std::ranges::next(std::ranges::begin(iter_), index_),
                                           std::ranges::end(iter_), docId);
  index_ = std::ranges::distance(std::ranges::begin(iter_), it + 1);
  if (index_ >= len()) {
    base_.isValid = false;
  }

  base_.current->docId = *it;
  hit = base_.current;

  if (*it == docId) {
    return INDEXREAD_OK;
  }
  return INDEXREAD_NOTFOUND;
}
t_docId QueryIterator::current() const noexcept {
  return base_.current->docId;
}
std::size_t QueryIterator::len() const noexcept {
  return iter_.size();
}
void QueryIterator::rewind() noexcept {
  base_.isValid = true;
  base_.current->docId = 0;
  base_.LastDocId = 0;
  index_ = 0;
}

namespace {
int QIter_Read(IndexIterator *base, RSIndexResult **hit) {
  return reinterpret_cast<QueryIterator *>(base)->read(*hit);
}
int QIter_SkipTo(IndexIterator *base, t_docId docId, RSIndexResult **hit) {
  return reinterpret_cast<QueryIterator *>(base)->skip_to(docId, *hit);
}
void QIter_Free(IndexIterator *self) {
  using alloc_type = Allocator::TrackingAllocator<QueryIterator>;
  const auto qi = reinterpret_cast<QueryIterator *const>(self);
  auto alloc = alloc_type{qi->iter_.get_allocator()};
  IndexResult_Free(self->current);
  std::allocator_traits<alloc_type>::destroy(alloc, qi);
  std::allocator_traits<alloc_type>::deallocate(alloc, qi, 1);
}
std::size_t QIter_Len(IndexIterator *base) {
  return reinterpret_cast<QueryIterator const *>(base)->len();
}
void QIter_Rewind(IndexIterator *base) {
  reinterpret_cast<QueryIterator *>(base)->rewind();
}

}  // anonymous namespace

IndexIterator QueryIterator::init_base(QueryIterator *ctx) {
  return IndexIterator{
      .type = ID_LIST_ITERATOR,
      .isValid = true,
      .isAborted = false,
      .LastDocId = 0,
      .current = NewVirtualResult(0, RS_FIELDMASK_ALL),
      .NumEstimated = QIter_Len,
      .Read = QIter_Read,
      .SkipTo = QIter_SkipTo,
      .Free = QIter_Free,
      .Rewind = QIter_Rewind,
  };
}
}  // namespace GeoShape
}  // namespace RediSearch
