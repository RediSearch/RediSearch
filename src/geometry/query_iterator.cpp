/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "query_iterator.hpp"
#include "doc_table.h"

#include <utility>    // std::move
#include <iterator>   // ranges::distance
#include <algorithm>  // ranges::lower_bound

namespace RediSearch {
namespace GeoShape {

auto QueryIterator::base() noexcept -> IndexIterator * {
  return &base_;
}

int QueryIterator::read_single(RSIndexResult *&hit) noexcept {
  if (!base_.isValid || !has_next()) {
    return INDEXREAD_EOF;
  }
  t_docId docId = iter_[index_++];
  if (indexSpec_ && !DocTable_VerifyFieldIndexExpirationPredicate(&indexSpec_->docs, docId, filterCtx_)) {
    return INDEXREAD_NOTFOUND;
  }

  base_.current->docId = docId;
  hit = base_.current;
  return INDEXREAD_OK;
}

int QueryIterator::read(RSIndexResult *&hit) noexcept {
  // TODO: need to handle timeouts
  int rc = INDEXREAD_OK;
  do {
    rc = read_single(hit);
  } while (rc == INDEXREAD_NOTFOUND);
  return rc;
}
int QueryIterator::skip_to(t_docId docId, RSIndexResult *&hit) {
  if (!base_.isValid || !has_next()) {
    return INDEXREAD_EOF;
  }
  if (docId > iter_.back()) {
    base_.isValid = false;
    return INDEXREAD_EOF;
  }

  const auto it = std::ranges::lower_bound(std::ranges::next(std::ranges::begin(iter_), index_),
                                           std::ranges::end(iter_), docId);
  index_ = std::ranges::distance(std::ranges::begin(iter_), it + 1);
  if (!has_next()) {
    abort();
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
int QueryIterator::has_next() const noexcept {
  return index_ < len();
}
std::size_t QueryIterator::len() const noexcept {
  return iter_.size();
}
void QueryIterator::abort() noexcept {
  base_.isValid = false;
}
void QueryIterator::rewind() noexcept {
  base_.isValid = true;
  base_.current->docId = 0;
  index_ = 0;
}

namespace {
int QIter_Read(void *ctx, RSIndexResult **hit) {
  return static_cast<QueryIterator *>(ctx)->read(*hit);
}
int QIter_SkipTo(void *ctx, t_docId docId, RSIndexResult **hit) {
  return static_cast<QueryIterator *>(ctx)->skip_to(docId, *hit);
}
t_docId QIter_LastDocId(void *ctx) {
  return static_cast<QueryIterator const *>(ctx)->current();
}
int QIter_HasNext(void *ctx) {
  return static_cast<QueryIterator const *>(ctx)->has_next();
}
void QIter_Free(IndexIterator *self) {
  using alloc_type = Allocator::TrackingAllocator<QueryIterator>;
  const auto qi = static_cast<QueryIterator *const>(self->ctx);
  auto alloc = alloc_type{qi->iter_.get_allocator()};
  IndexResult_Free(self->current);
  std::allocator_traits<alloc_type>::destroy(alloc, qi);
  std::allocator_traits<alloc_type>::deallocate(alloc, qi, 1);
}
std::size_t QIter_Len(void *ctx) {
  return static_cast<QueryIterator const *>(ctx)->len();
}
void QIter_Abort(void *ctx) {
  static_cast<QueryIterator *>(ctx)->abort();
}
void QIter_Rewind(void *ctx) {
  static_cast<QueryIterator *>(ctx)->rewind();
}

}  // anonymous namespace

IndexIterator QueryIterator::init_base(QueryIterator *ctx) {
  return IndexIterator{
      .isValid = 1,
      .ctx = ctx,
      .current = NewVirtualResult(0, RS_FIELDMASK_ALL),
      .type = ID_LIST_ITERATOR,
      .NumEstimated = QIter_Len,
      .Read = QIter_Read,
      .SkipTo = QIter_SkipTo,
      .LastDocId = QIter_LastDocId,
      .HasNext = QIter_HasNext,
      .Free = QIter_Free,
      .Len = QIter_Len,
      .Abort = QIter_Abort,
      .Rewind = QIter_Rewind,
  };
}
}  // namespace GeoShape
}  // namespace RediSearch
