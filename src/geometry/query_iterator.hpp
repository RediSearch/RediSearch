/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#pragma once

#include "../index_iterator.h"
#include "allocator.hpp"

#include <vector>    // std::vector
#include <ranges>    // ranges::input_range
#include <utility>   // std::move, std::forward
#include <iterator>  // std::begin, std::end, ranges::distance
#include <algorithm> // ranges::sort, ranges::lower_bound

struct GeometryQueryIterator {
  using container_type = std::vector<t_docId, rm_allocator<t_docId>>;
  IndexIterator base_;
  container_type iter_;
  size_t index_;

  using Self = GeometryQueryIterator;
  [[nodiscard]] void *operator new(std::size_t) {
    return rm_allocator<Self>().allocate(1);
  }
  void operator delete(void *p) noexcept {
    rm_allocator<Self>().deallocate(static_cast<Self *>(p), 1);
  }

  explicit GeometryQueryIterator() = default;
  template <std::ranges::input_range R>
  explicit GeometryQueryIterator(R &&range)
      : GeometryQueryIterator(
            container_type{std::begin(std::forward<R>(range)), std::end(std::forward<R>(range))}) {
  }
  explicit GeometryQueryIterator(container_type &&docs)
      : base_{init_base()}, iter_{std::move(docs)}, index_{0} {
    base_.ctx = this;
    std::ranges::sort(iter_);
  }

  GeometryQueryIterator(const Self &) = delete;
  explicit GeometryQueryIterator(Self &&) = default;
  Self &operator=(const Self &) = delete;
  Self &operator=(Self &&) = default;
  ~GeometryQueryIterator() {
    IndexResult_Free(base_.current);
  }

  IndexIterator *base() {
    return &base_;
  }

  int read(RSIndexResult *&hit) {
    if (!base_.isValid || !has_next()) {
      return INDEXREAD_EOF;
    }

    base_.current->docId = iter_[index_++];
    hit = base_.current;
    return INDEXREAD_OK;
  }
  int skip_to(t_docId docId, RSIndexResult *&hit) {
    if (!base_.isValid || !has_next()) {
      return INDEXREAD_EOF;
    }
    if (docId > iter_.back()) {
      base_.isValid = false;
      return INDEXREAD_EOF;
    }

    auto it = std::ranges::lower_bound(iter_.cbegin() + index_, iter_.cend(), docId);
    index_ = std::ranges::distance(iter_.cbegin(), it + 1);
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
  t_docId current() const {
    return base_.current->docId;
  }
  int has_next() const {
    return index_ < len();
  }
  size_t len() const {
    return iter_.size();
  }
  void abort() {
    base_.isValid = false;
  }
  void rewind() {
    base_.isValid = true;
    base_.current->docId = 0;
    index_ = 0;
  }

  static IndexIterator init_base();
};

namespace {
int QIter_Read(void *ctx, RSIndexResult **hit) {
  return static_cast<GeometryQueryIterator *>(ctx)->read(*hit);
}
int QIter_SkipTo(void *ctx, t_docId docId, RSIndexResult **hit) {
  return static_cast<GeometryQueryIterator *>(ctx)->skip_to(docId, *hit);
}
t_docId QIter_LastDocId(void *ctx) {
  return static_cast<GeometryQueryIterator const *>(ctx)->current();
}
int QIter_HasNext(void *ctx) {
  return static_cast<GeometryQueryIterator const *>(ctx)->has_next();
}
void QIter_Free(IndexIterator *self) {
  auto it = static_cast<GeometryQueryIterator *>(self->ctx);
  delete it;
}
size_t QIter_Len(void *ctx) {
  return static_cast<GeometryQueryIterator const *>(ctx)->len();
}
void QIter_Abort(void *ctx) {
  static_cast<GeometryQueryIterator *>(ctx)->abort();
}
void QIter_Rewind(void *ctx) {
  static_cast<GeometryQueryIterator *>(ctx)->rewind();
}
}  // anonymous namespace

IndexIterator GeometryQueryIterator::init_base() {
  auto ii = IndexIterator{
      .isValid = 1,
      .ctx = nullptr,
      .current = NewVirtualResult(0),
      .mode = MODE_SORTED,
      .type = ID_LIST_ITERATOR,
      .NumEstimated = QIter_Len,
      .GetCriteriaTester = nullptr,
      .Read = QIter_Read,
      .SkipTo = QIter_SkipTo,
      .LastDocId = QIter_LastDocId,
      .HasNext = QIter_HasNext,
      .Free = QIter_Free,
      .Len = QIter_Len,
      .Abort = QIter_Abort,
      .Rewind = QIter_Rewind,
  };
  return ii;
}