/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#pragma once

#include "rtdoc.hpp"
#include "query_iterator.hpp"
#include "geometry_types.h"

#include <vector>         // std::vector, std::erase_if
#include <utility>        // std::pair
#include <iterator>       // std::back_inserter
#include <optional>       // std::optional
#include <algorithm>      // ranges::for_each, views::transform
#include <exception>      // std::exception
#include <functional>     // std::hash, std::equal_to, std::reference_wrapper
#include <string_view>    // std::string_view
#include <unordered_map>  // std::unordered_map
// #include <boost/unordered/unordered_map.hpp> //is faster than std::unordered_map?
template <typename coord_system>
struct RTree {
  using doc_type = RTDoc<coord_system>;
  using rtree_type = bgi::rtree<
      /* Value */ doc_type,
      /* Parameters */ bgi::quadratic<16>,
      /* IndexableGetter */ RTDoc_Indexable<coord_system>,
      /* EqualTo */ RTDoc_EqualTo<coord_system>,
      /* Allocator */ rm_allocator<doc_type>>;

  using geom_type = doc_type::geom_type;
  using docLookup_type =
      std::unordered_map<t_docId, geom_type, std::hash<t_docId>, std::equal_to<t_docId>,
                         rm_allocator<std::pair<const t_docId, geom_type>>>;

  rtree_type rtree_;
  docLookup_type docLookup_;

  explicit RTree() = default;

  [[nodiscard]] auto lookup(t_docId id) const
      -> std::optional<std::reference_wrapper<const geom_type>> {
    if (auto it = docLookup_.find(id); it != docLookup_.end()) {
      return it->second;
    }
    return std::nullopt;
  }

  template <typename Geometry>
  void insert(Geometry const& geom, t_docId id) {
    rtree_.insert(doc_type{geom, id});
    docLookup_.insert(std::pair{id, geom_type{geom}});
  }

  [[nodiscard]] static geom_type from_wkt(std::string_view wkt) {
    geom_type geom{};
    if (wkt.starts_with("POINT")) {
      typename doc_type::point_type p{};
      bg::read_wkt(wkt.data(), p);
      if (bg::is_empty(p)) {
        throw std::runtime_error{"attempting to create empty geometry"};
      }
      geom = p;
    } else if (wkt.starts_with("POLYGON")) {
      typename doc_type::poly_type p{};
      bg::read_wkt(wkt.data(), p);
      if (bg::is_empty(p)) {
        throw std::runtime_error{"attempting to create empty geometry"};
      }
      geom = p;
    } else {
      throw std::runtime_error{"unknown geometry type"};
    }
    return geom;
  }

  int insertWKT(const char* wkt, size_t len, t_docId id, RedisModuleString** err_msg) {
    try {
      auto geom = from_wkt(std::string_view{wkt, len});
      insert(geom, id);
      return 0;
    } catch (const std::exception& e) {
      if (err_msg) {
        *err_msg = RedisModule_CreateString(nullptr, e.what(), strlen(e.what()));
      }
      return 1;
    }
  }

  bool remove(const doc_type& doc) {
    return rtree_.remove(doc);
  }

  bool remove(t_docId id) {
    if (auto doc = lookup(id); doc.has_value()) {
      remove(doc_type{doc.value(), id});
      docLookup_.erase(id);
      return true;
    }
    return false;
  }

  int remove(const char* wkt, size_t len, t_docId id) {
    try {
      if (auto opt = lookup(id); opt.has_value()) {
        docLookup_.erase(id);
        return remove(doc_type{opt.value(), id});
      } else {
        auto geom = from_wkt(std::string_view{wkt, len});
        return remove(doc_type{geom, id});
      }
    } catch (...) {
      return -1;
    }
  }

  template <typename Geometry>
  [[nodiscard]] static string geometry_to_string(Geometry const& geom) {
    sstream ss{};
    ss << bg::wkt(geom);
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

    constexpr auto pred_true = [](...) { return true; };
    const auto query_true = bgi::satisfies(pred_true);
    size_t lenDocs = 0;
    std::ranges::for_each(rtree_.qbegin(query_true), rtree_.qend(), [&](doc_type const& doc) {
      lenDocs += 1;
      size_t lenValues = 0;
      RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);

      RedisModule_ReplyWithStringBuffer(ctx, "id", strlen("id"));
      RedisModule_ReplyWithLongLong(ctx, doc.id());
      lenValues += 2;

      if (auto geom = lookup(doc.id()); geom.has_value()) {
        RedisModule_ReplyWithStringBuffer(ctx, "geoshape", strlen("geoshape"));
        auto str =
            std::visit([](auto&& geom) { return geometry_to_string(geom); }, geom.value().get());
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
  static IndexIterator* generate_query_iterator(ResultsVec&& results) {
    auto geometry_query_iterator = new GeometryQueryIterator(
        results | std::views::transform([](auto&& doc) { return doc.id(); }));
    return geometry_query_iterator->base();
  }

  template <typename Predicate>
  [[nodiscard]] ResultsVec query(Predicate p) const {
    ResultsVec results{};
    rtree_.query(p, std::back_inserter(results));
    return results;
  }

  static constexpr auto filter_results = [](auto&& g1, auto&& g2) {
    if constexpr (std::is_same_v<typename doc_type::point_type, std::decay_t<decltype(g2)>>) {
      return false;
    } else {
      return !bg::within(g1, g2);
    }
  };

  [[nodiscard]] ResultsVec contains(doc_type const& queryDoc, geom_type const& queryGeom) const {
    auto results = query(bgi::contains(queryDoc.rect_));
    std::erase_if(results, [&](auto const& doc) {
      auto geom = lookup(doc.id());
      return geom && std::visit(filter_results, queryGeom, geom.value().get());
    });
    return results;
  }

  [[nodiscard]] ResultsVec within(doc_type const& queryDoc, geom_type const& queryGeom) const {
    auto results = query(bgi::within(queryDoc.rect_));
    std::erase_if(results, [&](auto const& doc) {
      auto geom = lookup(doc.id());
      return geom && std::visit(filter_results, geom.value().get(), queryGeom);
    });
    return results;
  }

  [[nodiscard]] ResultsVec query(doc_type const& queryDoc, QueryType queryType,
                                 geom_type const& queryGeom) const {
    switch (queryType) {
      case QueryType::CONTAINS:
        return contains(queryDoc, queryGeom);
      case QueryType::WITHIN:
        return within(queryDoc, queryGeom);
      default:
        return ResultsVec{};
    }
  }

  // IndexIterator* query(doc_type const& queryDoc, QueryType queryType) const {
  //   auto geom = lookup(queryDoc.id());
  //   return generate_query_iterator(query(queryDoc, queryType, geom.value()));
  // }

  IndexIterator* query(const char* wkt, size_t len, QueryType query_type,
                       RedisModuleString** err_msg) const {
    try {
      auto query_geom = from_wkt(std::string_view{wkt, len});
      auto query_doc = doc_type{query_geom, 0};
      return generate_query_iterator(query(query_doc, query_type, query_geom));
    } catch (const std::exception& e) {
      if (err_msg) {
        *err_msg = RedisModule_CreateString(nullptr, e.what(), strlen(e.what()));
      }
      return nullptr;
    }
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
