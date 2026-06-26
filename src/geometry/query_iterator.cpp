/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#include "query_iterator.hpp"
#include "geometry_api.h"
#include "doc_table.h"
#include "iterators_ffi.h"
#include "search_ctx.h"
#include "spec.h"
#include "util/timeout.h"
#include "rqe_iterator_type.h"
#include "types_ffi.h"

#include <iterator>   // ranges::distance

namespace RediSearch {
namespace GeoShape {

bool CPPQueryIterator::should_check_field_expiration(const RedisSearchCtx *sctx,
                                                     const FieldFilterContext *filterCtx) noexcept {
  // Mirrors the hoisted gate in HybridIterator / InvIndIterator: all inputs are
  // iterator-invariant, so snapshot the AND once here. A non-NULL `ttl` is a
  // sufficient and tight gate by itself: the table holds field-level entries
  // only and is destroyed when the last one leaves the index.
  return sctx && filterCtx->field.index != RS_INVALID_FIELD_INDEX &&
         sctx->spec->docs.ttl;
}

auto CPPQueryIterator::base() noexcept -> QueryIterator * {
  return &base_;
}

IteratorStatus CPPQueryIterator::read_single() noexcept {
  if (!has_next()) {
    base_.atEOF = true;
    return ITERATOR_EOF;
  }
  t_docId docId = iter_[index_++];
  if (check_field_expiration_
      && !DocTable_CheckFieldExpirationPredicate(&sctx_->spec->docs, docId,
                                                 filterCtx_.field.index,
                                                 filterCtx_.predicate, &sctx_->time.current)) {
    return ITERATOR_NOTFOUND;
  }

  base_.current->docId = docId;
  base_.lastDocId = docId;
  return ITERATOR_OK;
}

IteratorStatus CPPQueryIterator::read() noexcept {
  uint32_t timeoutCounter = initTimeoutCounter_;
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

  const t_docId found = *it;
  if (check_field_expiration_
      && !DocTable_CheckFieldExpirationPredicate(&sctx_->spec->docs, found,
                                                 filterCtx_.field.index,
                                                 filterCtx_.predicate, &sctx_->time.current)) {
    // The matched entry's field has expired. Fall back to read(), which loops
    // past expired entries to the next valid one (updating current/lastDocId,
    // and setting atEOF on EOF), so the iterator never settles on an expired
    // doc. read() resumes from index_, i.e. the entry right after this expired
    // match.
    const IteratorStatus rc = read();
    if (rc == ITERATOR_OK) {
      // A valid entry beyond docId was found; the target itself did not match.
      return ITERATOR_NOTFOUND;
    }
    return rc;
  }

  if (!has_next()) {
    base_.atEOF = true;
  }

  base_.current->docId = found;
  base_.lastDocId = found;

  if (found == docId) {
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
std::size_t QIter_NumEstimated(const QueryIterator *ctx) {
  return reinterpret_cast<CPPQueryIterator const *>(ctx)->len();
}
void QIter_Rewind(QueryIterator *ctx) {
  reinterpret_cast<CPPQueryIterator *>(ctx)->rewind();
}
ValidateStatus QIter_Revalidate(QueryIterator *ctx, IndexSpec *) {
  auto *qi = reinterpret_cast<CPPQueryIterator *>(ctx);
  qi->check_field_expiration_ =
      CPPQueryIterator::should_check_field_expiration(qi->sctx_, &qi->filterCtx_);
  return VALIDATE_OK;
}

}  // anonymous namespace

QueryIterator CPPQueryIterator::init_base() {
  return QueryIterator{
      .type = IteratorType_GeoShape,
      .atEOF = false,
      .lastDocId = 0,
      .current = NewVirtualResult(0, RS_FIELDMASK_ALL),
      .NumEstimated = QIter_NumEstimated,
      .Read = QIter_Read,
      .SkipTo = QIter_SkipTo,
      .Revalidate = QIter_Revalidate,
      .Suspend = Default_Suspend,
      .Free = QIter_Free,
      .Rewind = QIter_Rewind,
      .PrintProfile = GeoShape_PrintProfile,
  };
}
}  // namespace GeoShape
}  // namespace RediSearch

extern "C" QueryIterator *NewGeometryQueryIterator_Bench(const RedisSearchCtx *sctx,
                                                         const FieldFilterContext *filterCtx,
                                                         t_docId *ids, std::size_t num,
                                                         std::size_t *allocated) {
  using RediSearch::GeoShape::CPPQueryIterator;
  using alloc_type = RediSearch::Allocator::TrackingAllocator<CPPQueryIterator>;
  auto alloc = alloc_type{*allocated};
  const auto qi = std::allocator_traits<alloc_type>::allocate(alloc, 1);
  // Use REDISEARCH_UNINITIALIZED counter to skip timeout checks when skipTimeoutChecks is set,
  // mirroring RTree::query.
  const uint32_t timeoutCounter =
      (sctx && sctx->time.skipTimeoutChecks) ? REDISEARCH_UNINITIALIZED : 0;
  const auto range = std::ranges::subrange{ids, ids + num};
  std::allocator_traits<alloc_type>::construct(alloc, qi, sctx, filterCtx, range, *allocated,
                                               timeoutCounter);
  return qi->base();
}
