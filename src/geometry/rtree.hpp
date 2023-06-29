/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#pragma once

#define BOOST_ALLOW_DEPRECATED_HEADERS
#include <boost/geometry.hpp>
#undef BOOST_ALLOW_DEPRECATED_HEADERS
#include "allocator.hpp"
#include "query_iterator.hpp"
#include "geometry_types.h"

#include <string>         // std::string, std::char_traits
#include <vector>         // std::vector, std::erase_if
#include <variant>        // std::variant, std::visit
#include <utility>        // std::pair
#include <sstream>        // std::stringstream
#include <iterator>       // std::back_inserter
#include <optional>       // std::optional
#include <algorithm>      // ranges::for_each, views::transform
#include <exception>      // std::exception
#include <functional>     // std::hash, std::equal_to, std::reference_wrapper
#include <string_view>    // std::string_view
#include <unordered_map>  // std::unordered_map
// #include <boost/unordered/unordered_map.hpp> //is faster than std::unordered_map?

namespace bg = boost::geometry;
namespace bgm = bg::model;
namespace bgi = bg::index;

using Cartesian = bg::cs::cartesian;
using Geographic = bg::cs::geographic<bg::degree>;

using string = std::basic_string<char, std::char_traits<char>, rm_allocator<char>>;
using sstream = std::basic_stringstream<char, std::char_traits<char>, rm_allocator<char>>;

template <typename coord_system>
struct RTree {
  using point_type = bgm::point<double, 2, coord_system>;
  using poly_type = bgm::polygon<
      /* point_type       */ point_type,
      /* is_clockwise     */ true,  // TODO: GEOMETRY - do we need to call bg::correct(poly) ?
      /* is_closed        */ true,
      /* points container */ std::vector,
      /* rings_container  */ std::vector,
      /* points_allocator */ rm_allocator,
      /* rings_allocator  */ rm_allocator>;
  using geom_type = std::variant<point_type, poly_type>;

  using rect_type = bgm::box<point_type>;
  using doc_type = std::pair<rect_type, t_docId>;
  using rtree_type = bgi::rtree<doc_type, bgi::quadratic<16>, bgi::indexable<doc_type>,
                                bgi::equal_to<doc_type>, rm_allocator<doc_type>>;

  using docLookup_type =
      std::unordered_map<t_docId, geom_type, std::hash<t_docId>, std::equal_to<t_docId>,
                         rm_allocator<std::pair<const t_docId, geom_type>>>;

  rtree_type rtree_;
  docLookup_type docLookup_;

  explicit RTree() = default;

  [[nodiscard]] static constexpr doc_type make_doc(geom_type const& geom, t_docId id = 0) {
    return doc_type{
        std::visit([](auto&& geom) {
          if constexpr (std::is_same_v<point_type, std::decay_t<decltype(geom)>>) {
            constexpr auto EPSILON = 0.00001;
            point_type p = geom;
            return rect_type{p, point_type{bg::get<0>(p) + EPSILON, bg::get<1>(p) + EPSILON}};
          } else {
            return bg::return_envelope<rect_type>(geom);
          }
        }, geom), id};
  }
  [[nodiscard]] static constexpr rect_type get_rect(doc_type const& doc) {
    return doc.first;
  }
  [[nodiscard]] static constexpr t_docId get_id(doc_type const& doc) {
    return doc.second;
  }

  [[nodiscard]] auto lookup(t_docId id) const
      -> std::optional<std::reference_wrapper<const geom_type>> {
    if (auto it = docLookup_.find(id); it != docLookup_.end()) {
      return it->second;
    }
    return std::nullopt;
  }
  [[nodiscard]] auto lookup(doc_type const& doc) const {
    return lookup(get_id(doc));
  }

  void insert(geom_type const& geom, t_docId id) {
    rtree_.insert(make_doc(geom, id));
    docLookup_.insert(std::pair{id, geom});
  }

  [[nodiscard]] static geom_type from_wkt(std::string_view wkt) {
    geom_type geom{};
    if (wkt.starts_with("POINT")) {
      point_type p{};
      bg::read_wkt(wkt.data(), p);
      geom = p;
    } else if (wkt.starts_with("POLYGON")) {
      poly_type p{};
      bg::read_wkt(wkt.data(), p);
      geom = p;
    } else {
      throw std::runtime_error{"unknown geometry type"};
    }
    std::visit([](auto&& geom) {
      if (bg::is_empty(geom)) {
        throw std::runtime_error{"attempting to create empty geometry"};
      }
      if (!bg::is_valid(geom)) {
        throw std::runtime_error{"invalid geometry"};
      }
    }, geom);
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
    if (auto geom = lookup(id); geom.has_value()) {
      remove(make_doc(geom.value(), id));
      docLookup_.erase(id);
      return true;
    }
    return false;
  }

  [[nodiscard]] static string geometry_to_string(geom_type const& geom) {
    return std::visit(
        [](auto&& geom) {
          sstream ss{};
          ss << bg::wkt(geom);
          return ss.str();
        },
        geom);
  }

  [[nodiscard]] static string doc_to_string(doc_type const& doc) {
    sstream ss{};
    ss << bg::wkt(get_rect(doc));
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
      RedisModule_ReplyWithLongLong(ctx, get_id(doc));
      lenValues += 2;

      if (auto geom = lookup(doc); geom.has_value()) {
        RedisModule_ReplyWithStringBuffer(ctx, "geoshape", strlen("geoshape"));
        auto str = geometry_to_string(geom.value());
        RedisModule_ReplyWithStringBuffer(ctx, str.data(), str.size());
        lenValues += 2;
      }
      RedisModule_ReplyWithStringBuffer(ctx, "rect", strlen("rect"));
      auto str = doc_to_string(doc);
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
        results | std::views::transform([](auto&& doc) { return get_id(doc); }));
    return geometry_query_iterator->base();
  }

  template <typename Predicate>
  [[nodiscard]] ResultsVec query(Predicate p) const {
    ResultsVec results{};
    rtree_.query(p, std::back_inserter(results));
    return results;
  }

  static constexpr auto filter_results = [](auto&& g1, auto&& g2) {
    if constexpr (std::is_same_v<point_type, std::decay_t<decltype(g2)>> &&
                  !std::is_same_v<point_type, std::decay_t<decltype(g1)>>) {
      return false;
    } else {
      return !bg::within(g1, g2);
    }
  };

  [[nodiscard]] ResultsVec contains(doc_type const& queryDoc, geom_type const& queryGeom) const {
    auto results = query(bgi::contains(get_rect(queryDoc)));
    std::erase_if(results, [&](auto const& doc) {
      auto geom = lookup(doc);
      return geom && std::visit(filter_results, queryGeom, geom.value().get());
    });
    return results;
  }

  [[nodiscard]] ResultsVec within(doc_type const& queryDoc, geom_type const& queryGeom) const {
    auto results = query(bgi::within(get_rect(queryDoc)));
    std::erase_if(results, [&](auto const& doc) {
      auto geom = lookup(doc);
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

  IndexIterator* query(const char* wkt, size_t len, QueryType query_type,
                       RedisModuleString** err_msg) const {
    try {
      auto query_geom = from_wkt(std::string_view{wkt, len});
      return generate_query_iterator(query(make_doc(query_geom), query_type, query_geom));
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
