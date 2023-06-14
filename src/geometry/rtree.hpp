/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#pragma once

#include "rtdoc.hpp"
#include "query_iterator.hpp"
#include <unordered_map>
#include <optional>
#include <string_view>
#include <ranges>
// #include <boost/unordered/unordered_map.hpp> //is faster than std::unordered_map?

namespace bg = boost::geometry;
namespace bgi = bg::index;
using Cartesian = bg::cs::cartesian;
using Geographic = bg::cs::geographic<bg::degree>;

template <typename coord_system>
struct RTree {
  using parameter_type = bgi::quadratic<16>;
  using doc_type = RTDoc<coord_system>;
  using poly_type = typename RTDoc<coord_system>::poly_type;
  using rtree_internal = bgi::rtree<doc_type, parameter_type, RTDoc_Indexable<coord_system>,
                                    RTDoc_EqualTo<coord_system>, rm_allocator<doc_type>>;
  using docLookup_internal =
      std::unordered_map<t_docId, poly_type, std::hash<t_docId>, std::equal_to<t_docId>,
                         rm_allocator<std::pair<const t_docId, poly_type>>>;

  rtree_internal rtree_;
  docLookup_internal docLookup_;

  explicit RTree() = default;
  explicit RTree(rtree_internal const& rt) noexcept : rtree_{rt} {
  }

  [[nodiscard]] std::optional<std::reference_wrapper<const poly_type>> lookup(
      t_docId id) const {
    if (auto it = docLookup_.find(id); it != docLookup_.end()) {
      return it->second;
    }
    return std::nullopt;
  }

  void insert(const poly_type& poly, t_docId id) {
    rtree_.insert({poly, id});
    docLookup_.insert({id, poly});
  }

  int insertWKT(const char *wkt, size_t len, t_docId id, RedisModuleString **err_msg) {
    try {
      auto geometry = Polygon<coord_system>::from_wkt(std::string_view{wkt, len});
      insert(geometry, id);
      return 0;
    } catch (const std::exception& e) {
      if (err_msg) {
        *err_msg = RedisModule_CreateString(nullptr, e.what(), strlen(e.what()));
      }
      return 1;
    }
  }

  bool remove(t_docId id) {
    if (auto doc = this->lookup(id); doc.has_value()) {
      rtree_.remove(doc_type{doc.value(), id});
      docLookup_.erase(id);
      return true;
    }
    return false;
  }

  bool removeId(t_docId id) {
    return docLookup_.erase(id) != 0;
  }

  bool remove(const doc_type& doc) {
    return rtree_.remove(doc);
  }

  int remove(const char *wkt, size_t len, t_docId id) {
    try {
      if (auto opt = this->lookup(id); opt.has_value()) {
        removeId(id);
        return remove(RTDoc<coord_system>{opt.value(), id});
      } else {
        auto geometry = Polygon<coord_system>::from_wkt(std::string_view{wkt,len});
        return remove(RTDoc<coord_system>{geometry, id});
      }
    } catch (...) {
      return -1;
    }
  }

  [[nodiscard]] static string geometry_to_string(const poly_type& geometry) {
    using sstream = std::basic_stringstream<char, std::char_traits<char>, rm_allocator<char>>;
    sstream ss{};
    ss << bg::wkt(geometry);
    return ss.str();
  }

  void dump(RedisModuleCtx* ctx) const {
    size_t lenTop = 0;
    RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);

    RedisModule_ReplyWithStringBuffer(ctx, "type", strlen("type"));
    RedisModule_ReplyWithStringBuffer(ctx, "boost_rtree", strlen("boost_rtree"));
    lenTop += 2;

    RedisModule_ReplyWithStringBuffer(ctx, "ptr", strlen("ptr"));
    char addr[1024] = {0};
    sprintf(addr, "%p", &rtree_);
    RedisModule_ReplyWithStringBuffer(ctx, addr, strlen(addr));
    lenTop += 2;

    RedisModule_ReplyWithStringBuffer(ctx, "num_docs", strlen("num_docs"));
    RedisModule_ReplyWithLongLong(ctx, static_cast<long long>(rtree_.size()));
    lenTop += 2;

    RedisModule_ReplyWithStringBuffer(ctx, "docs", strlen("docs"));
    RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
    lenTop += 2;

    constexpr auto pred_true = [](auto&&...) { return true; };
    constexpr auto query_true = bgi::satisfies(pred_true);
    size_t lenDocs = 0;
    std::for_each(rtree_.qbegin(query_true), rtree_.qend(), [&](doc_type const& doc) {
      lenDocs += 1;
      size_t lenValues = 0;
      RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);

      RedisModule_ReplyWithStringBuffer(ctx, "id", strlen("id"));
      RedisModule_ReplyWithLongLong(ctx, doc.id());
      lenValues += 2;

      if (auto geometry = lookup(doc.id()); geometry.has_value()) {
        RedisModule_ReplyWithStringBuffer(ctx, "geometry", strlen("geometry"));
        auto str = geometry_to_string(geometry.value());
        RedisModule_ReplyWithStringBuffer(ctx, str.data(), str.size());
        lenValues += 2;
      }
      RedisModule_ReplyWithStringBuffer(ctx, "rect", strlen("rect"));
      auto str = doc.rect_to_string();
      RedisModule_ReplyWithStringBuffer(ctx, str.data(), str.size());
      lenValues += 2;

      RedisModule_ReplySetArrayLength(ctx, lenValues);
    });
    RedisModule_ReplySetArrayLength(ctx, lenDocs);
    RedisModule_ReplySetArrayLength(ctx, lenTop);
  }

  [[nodiscard]] size_t size() const noexcept {
    return rtree_.size();
  }

  [[nodiscard]] bool is_empty() const noexcept {
    return rtree_.empty();
  }

  void clear() noexcept {
    rtree_.clear();
  }

  [[nodiscard]] size_t report() const noexcept {
    return rtree_.get_allocator().report();
  }

  using ResultsVec = std::vector<doc_type, rm_allocator<doc_type>>;
  static IndexIterator *generate_query_iterator(ResultsVec&& results) {
    auto ids = GeometryQueryIterator::container(results.size());
    std::ranges::transform(results, ids.begin(), [](auto&& doc) { return doc.id(); });
    auto gqi = new GeometryQueryIterator(std::move(ids));
    return gqi->base();
  }

  IndexIterator *query(RTDoc const& queryDoc, QueryType queryType) const {
    auto geometry = lookup(queryDoc.id());
    return generate_query_iterator(query(queryDoc, queryType, geometry.value()));
  }

  IndexIterator *query(const char *wkt, size_t len, enum QueryType queryType, RedisModuleString **err_msg) const {
    try {
      auto geometry = Polygon<coord_system>::from_wkt(wkt);
      return generate_query_iterator(query(RTDoc<coord_system>{geometry, 0}, queryType, geometry));
    } catch(const std::exception& e) {
      if (err_msg) {
        *err_msg = RedisModule_CreateString(nullptr, e.what(), strlen(e.what()));
      }
      return nullptr;
    }
  }

  [[nodiscard]] ResultsVec query(doc_type const& queryDoc, QueryType queryType,
                                 const poly_type& queryGeometry) const {
    switch (queryType) {
      case QueryType::CONTAINS:
        return contains(queryDoc, queryGeometry);
      case QueryType::WITHIN:
        return within(queryDoc, queryGeometry);
      default:
        return ResultsVec{};
    }
  }

  [[nodiscard]] ResultsVec contains(doc_type const& queryDoc,
                                    const poly_type& queryGeometry) const {
    auto results = query(bgi::contains(queryDoc.rect_));
    std::erase_if(results, [&](auto const& doc) {
      auto geometry = lookup(doc.id());
      return geometry && !bg::within(queryGeometry, geometry.value().get());
    });
    return results;
  }
  [[nodiscard]] ResultsVec within(doc_type const& queryDoc,
                                  const poly_type& queryGeometry) const {
    auto results = query(bgi::within(queryDoc.rect_));
    std::erase_if(results, [&](auto const& doc) {
      auto geometry = lookup(doc.id());
      return geometry && !bg::within(geometry.value().get(), queryGeometry);
    });
    return results;
  }

  template <typename Predicate>
  [[nodiscard]] ResultsVec query(Predicate p) const {
    ResultsVec results{};
    rtree_.query(p, std::back_inserter(results));
    return results;
  }

  using Self = RTree;
  [[nodiscard]] void* operator new(std::size_t) {
    return rm_allocator<Self>().allocate(1);
  }
  void operator delete(void* p) noexcept {
    rm_allocator<Self>().deallocate(static_cast<Self*>(p), 1);
  }

  [[nodiscard]] static size_t reportTotal() noexcept {
    return rm_allocator<Self>().report();
  }
};

#define X(variant) template class RTree<variant>;
GEO_VARIANTS(X)
#undef X
