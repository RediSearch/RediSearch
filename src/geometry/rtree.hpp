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
#include <boost/unordered/unordered_flat_map.hpp>  //is faster than std::unordered_map?

namespace bg = boost::geometry;
namespace bgm = bg::model;
namespace bgi = bg::index;

using Cartesian = bg::cs::cartesian;
using Geographic = bg::cs::geographic<bg::degree>;

using string = std::basic_string<char, std::char_traits<char>, rm_allocator<char>>;
using sstream = std::basic_stringstream<char, std::char_traits<char>, rm_allocator<char>>;

template <typename T>
[[nodiscard]] static string to_string(T const& t, rm_allocator<char>&& a) {
  auto ss = sstream{std::ios_base::in | std::ios_base::out, a};
  ss << t;
  return ss.str();
}

template <typename coord_system>
struct RTree {
  using point_type =
      bgm::point<double, 2, coord_system>;  // TODO: GEOMETRY - dimension template param (2 or 3)
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

  using LUT_value_type = std::pair<t_docId const, geom_type>;
  using LUT_type = boost::unordered_flat_map<t_docId, geom_type, std::hash<t_docId>,
                      std::equal_to<t_docId>, rm_allocator<LUT_value_type>>;

  using ResultsVec = std::vector<doc_type, rm_allocator<doc_type>>;

  rm_allocator<void> alloc_;
  rtree_type rtree_;
  LUT_type docLookup_;

  RTree() = delete;
  explicit RTree(std::size_t& alloc_ref)
    : alloc_{alloc_ref}
    , rtree_{bgi::quadratic<16>{}, bgi::indexable<doc_type>{},
             bgi::equal_to<doc_type>{}, rm_allocator<doc_type>{alloc_ref}}
    , docLookup_{0, rm_allocator<LUT_value_type>{alloc_ref}} {
  }
  [[nodiscard]] static constexpr rect_type make_mbr(geom_type const& geom) {
    return std::visit(
        [](auto&& geom) {
          if constexpr (std::is_same_v<point_type, std::decay_t<decltype(geom)>>) {
            constexpr auto EPSILON = 1e-10;
            point_type p = geom;
            return rect_type{p, point_type{bg::get<0>(p) + EPSILON, bg::get<1>(p) + EPSILON}};
          } else {
            return bg::return_envelope<rect_type>(geom);
          }
        },
        geom);
  }
  [[nodiscard]] static constexpr doc_type make_doc(geom_type const& geom, t_docId id = 0) {
    return doc_type{make_mbr(geom), id};
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
    docLookup_.insert(LUT_value_type{id, geom});
  }

  [[nodiscard]] static geom_type from_wkt(std::string_view wkt) {
    geom_type geom{};
    if (wkt.starts_with("POI")) {
      geom = bg::from_wkt<point_type>(wkt.data());
    } else if (wkt.starts_with("POL")) {
      geom = bg::from_wkt<poly_type>(wkt.data());
    } else {
      throw std::runtime_error{"unknown geometry type"};
    }
    std::visit(
        [](auto&& geom) {
          if (bg::is_empty(geom)) {
            throw std::runtime_error{"attempting to create empty geometry"};
          }
          if (!bg::is_valid(geom)) {  // TODO: GEOMETRY - add flag to allow user to ascertain
                                      // validity of input
            throw std::runtime_error{"invalid geometry"};
          }
        },
        geom);
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

  [[nodiscard]] static auto geometry_to_string(geom_type const& geom, rm_allocator<char>&& a) {
    return std::visit([&](auto&& geom) {
      return to_string(bg::wkt(geom), std::move(a));
    }, geom);
  }
  [[nodiscard]] static auto doc_to_string(doc_type const& doc, rm_allocator<char>&& a) {
    return to_string(bg::wkt(get_rect(doc)), std::move(a));
  }

  void dump(RedisModuleCtx* ctx) const {
    size_t lenTop = 0;
    RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);

    RedisModule_ReplyWithStringBuffer(ctx, "type", strlen("type"));
    RedisModule_ReplyWithStringBuffer(ctx, "boost_rtree", strlen("boost_rtree"));
    lenTop += 2;

    RedisModule_ReplyWithStringBuffer(ctx, "ptr", strlen("ptr"));
    auto addr = to_string(&rtree_, rm_allocator<char>{alloc_.allocated_});
    RedisModule_ReplyWithStringBuffer(ctx, addr.c_str(), addr.length());
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
        auto str = geometry_to_string(geom.value(), rm_allocator<char>{alloc_.allocated_});
        RedisModule_ReplyWithStringBuffer(ctx, str.c_str(), str.length());
        lenValues += 2;
      }
      RedisModule_ReplyWithStringBuffer(ctx, "rect", strlen("rect"));
      auto str = doc_to_string(doc, rm_allocator<char>{alloc_.allocated_});
      RedisModule_ReplyWithStringBuffer(ctx, str.c_str(), str.length());
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

  static IndexIterator* generate_query_iterator(ResultsVec&& results,
                                                rm_allocator<GeometryQueryIterator>&& a) {
    auto geometry_query_iterator = std::construct_at(a.allocate(1), 
      results | std::views::transform([](auto&& doc) { return get_id(doc); }),
      rm_allocator<doc_type>{a.allocated_}
    );
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
      return generate_query_iterator(
        query(make_doc(query_geom), query_type, query_geom),
        rm_allocator<GeometryQueryIterator>{alloc_.allocated_}
      );
    } catch (const std::exception& e) {
      if (err_msg) {
        *err_msg = RedisModule_CreateString(nullptr, e.what(), strlen(e.what()));
      }
      return nullptr;
    }
  }

  [[nodiscard]] size_t reportTotal() noexcept {
    return alloc_.report();
  }
};

#define X(variant) template class RTree<variant>;
GEO_VARIANTS(X)
#undef X
