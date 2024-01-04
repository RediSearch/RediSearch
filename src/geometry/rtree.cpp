/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "rtree.hpp"

#include <string>     // std::string, std::char_traits
#include <sstream>    // std::stringstream
#include <algorithm>  // ranges::for_each, views::transform
#include <exception>  // std::exception
#include <execution>  // std::unseq
#include <numeric>    // std::transform_reduce

namespace RediSearch {
namespace GeoShape {

namespace {
template <typename cs, typename rect_type = RTree<cs>::rect_type>
constexpr auto make_mbr = [](auto&& geom) -> rect_type {
  using point_type = typename RTree<cs>::point_type;
  if constexpr (std::is_same_v<point_type, std::decay_t<decltype(geom)>>) {
    constexpr auto EPSILON = 1e-10;
    auto p1 = point_type{bg::get<0>(geom) - EPSILON, bg::get<1>(geom) - EPSILON};
    auto p2 = point_type{bg::get<0>(geom) + EPSILON, bg::get<1>(geom) + EPSILON};
    return rect_type{p1, p2};
  } else {
    return bg::return_envelope<rect_type>(geom);
  }
};
template <typename cs, typename geom_type = RTree<cs>::geom_type,
          typename doc_type = RTree<cs>::doc_type>
constexpr auto make_doc(geom_type const& geom, t_docId id = 0) -> doc_type {
  return doc_type{std::visit(make_mbr<cs>, geom), id};
}
template <typename cs, typename doc_type = RTree<cs>::doc_type,
          typename rect_type = RTree<cs>::rect_type>
constexpr auto get_rect(doc_type const& doc) -> rect_type {
  return doc.first;
}
template <typename cs, typename doc_type = RTree<cs>::doc_type>
constexpr auto get_id(doc_type const& doc) -> t_docId {
  return doc.second;
}

using string = std::basic_string<char, std::char_traits<char>, Allocator::Allocator<char>>;
template <typename T>
auto to_string(T const& t) -> string {
  using sstream = std::basic_stringstream<char, std::char_traits<char>, Allocator::Allocator<char>>;
  auto ss = sstream{};
  ss << t;
  return ss.str();
}
template <typename cs, typename geom_type = RTree<cs>::geom_type>
auto geometry_to_string(geom_type const& geom) -> string {
  return std::visit([](auto&& geom) -> string { return to_string(bg::wkt(geom)); }, geom);
}
template <typename cs, typename doc_type = RTree<cs>::doc_type>
auto doc_to_string(doc_type const& doc) -> string {
  return to_string(bg::wkt(get_rect<cs>(doc)));
}

template <typename cs, typename geom_type = RTree<cs>::geom_type>
auto from_wkt(std::string_view wkt) -> geom_type {
  geom_type geom{};
  if (wkt.starts_with("POI")) {
    using point_type = RTree<cs>::point_type;
    geom = bg::from_wkt<point_type>(std::string{wkt});
  } else if (wkt.starts_with("POL")) {
    using poly_type = RTree<cs>::poly_type;
    geom = bg::from_wkt<poly_type>(std::string{wkt});
  } else {
    throw std::runtime_error{"unknown geometry type"};
  }
  std::visit(
      [](auto&& geom) -> void {
        if (bg::is_empty(geom)) {
          throw std::runtime_error{"attempting to create empty geometry"};
        }
        // TODO: GEOMETRY - add flag to allow user to ascertain validity of input
        if (bg::correct(geom); !bg::is_valid(geom)) {
          throw std::runtime_error{"invalid geometry"};
        }
      },
      geom);
  return geom;
}

template <typename cs>
constexpr auto geometry_reporter = [](auto&& geom) -> std::size_t {
  using point_type = typename RTree<cs>::point_type;
  if constexpr (std::is_same_v<point_type, std::decay_t<decltype(geom)>>) {
    return 0ul;
  } else {
    auto const& inners = geom.inners();
    auto outer_size = geom.outer().get_allocator().report();
    return std::transform_reduce(
        std::execution::unseq, std::begin(inners), std::end(inners), outer_size, std::plus{},
        [](auto const& hole) -> std::size_t { return hole.get_allocator().report(); });
  }
};

template <typename cs>
constexpr auto filter_results = [](auto&& geom1, auto&& geom2) -> bool {
  using point_type = typename RTree<cs>::point_type;
  if constexpr (std::is_same_v<point_type, std::decay_t<decltype(geom2)>> &&
                !std::is_same_v<point_type, std::decay_t<decltype(geom1)>>) {
    return false;
  } else {
    return !bg::within(geom1, geom2);
  }
};
}  // anonymous namespace

template <typename cs>
RTree<cs>::RTree()
    : allocated_{sizeof *this},
      rtree_{{}, {}, {}, doc_alloc{allocated_}},
      docLookup_{0, lookup_alloc{allocated_}} {
}

template <typename cs>
auto RTree<cs>::lookup(t_docId id) const -> boost::optional<geom_type const&> {
  if (auto it = docLookup_.find(id); it != docLookup_.end()) {
    return it->second;
  }
  return {};
}
template <typename cs>
auto RTree<cs>::lookup(doc_type const& doc) const -> boost::optional<geom_type const&> {
  return lookup(get_id<cs>(doc));
}

template <typename cs>
void RTree<cs>::insert(geom_type const& geom, t_docId id) {
  docLookup_.insert(lookup_type{id, geom});
  rtree_.insert(make_doc<cs>(geom, id));
  allocated_ += std::visit(geometry_reporter<cs>, geom);
}

template <typename cs>
int RTree<cs>::insertWKT(std::string_view wkt, t_docId id, RedisModuleString** err_msg) {
  try {
    insert(from_wkt<cs>(wkt), id);
    return 0;
  } catch (const std::exception& e) {
    if (err_msg) {
      *err_msg = RedisModule_CreateString(nullptr, e.what(), std::strlen(e.what()));
    }
    return 1;
  }
}

template <typename cs>
bool RTree<cs>::remove(t_docId id) {
  if (auto geom = lookup(id); geom.has_value()) {
    allocated_ -= std::visit(geometry_reporter<cs>, *geom);
    rtree_.remove(make_doc<cs>(*geom, id));
    docLookup_.erase(id);
    return true;
  }
  return false;
}

template <typename cs>
void RTree<cs>::dump(RedisModuleCtx* ctx) const {
  std::size_t lenTop = 0;
  RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);

  RedisModule_ReplyWithStringBuffer(ctx, "type", std::strlen("type"));
  RedisModule_ReplyWithStringBuffer(ctx, "boost_rtree", std::strlen("boost_rtree"));
  lenTop += 2;

  RedisModule_ReplyWithStringBuffer(ctx, "ptr", std::strlen("ptr"));
  auto addr = to_string(&rtree_);
  RedisModule_ReplyWithStringBuffer(ctx, addr.c_str(), addr.length());
  lenTop += 2;

  RedisModule_ReplyWithStringBuffer(ctx, "num_docs", std::strlen("num_docs"));
  RedisModule_ReplyWithLongLong(ctx, static_cast<long long>(rtree_.size()));
  lenTop += 2;

  RedisModule_ReplyWithStringBuffer(ctx, "docs", std::strlen("docs"));
  RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
  lenTop += 2;

  std::size_t lenDocs = 0;
  std::ranges::for_each(rtree_, [&](doc_type const& doc) -> void {
    lenDocs += 1;
    std::size_t lenValues = 0;
    RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);

    RedisModule_ReplyWithStringBuffer(ctx, "id", std::strlen("id"));
    RedisModule_ReplyWithLongLong(ctx, get_id<cs>(doc));
    lenValues += 2;

    if (auto geom = lookup(doc); geom.has_value()) {
      RedisModule_ReplyWithStringBuffer(ctx, "geoshape", std::strlen("geoshape"));
      auto str = geometry_to_string<cs>(*geom);
      RedisModule_ReplyWithStringBuffer(ctx, str.c_str(), str.length());
      lenValues += 2;
    }
    RedisModule_ReplyWithStringBuffer(ctx, "rect", std::strlen("rect"));
    auto str = doc_to_string<cs>(doc);
    RedisModule_ReplyWithStringBuffer(ctx, str.c_str(), str.length());
    lenValues += 2;

    RedisModule_ReplySetArrayLength(ctx, lenValues);
  });
  RedisModule_ReplySetArrayLength(ctx, lenDocs);
  RedisModule_ReplySetArrayLength(ctx, lenTop);
}

template <typename cs>
std::size_t RTree<cs>::report() const noexcept {
  return allocated_;
}

template <typename cs>
template <typename Predicate, typename Filter>
auto RTree<cs>::apply_predicate(Predicate&& p, Filter&& f) const -> query_results {
  auto results = query_results{rtree_.qbegin(std::forward<Predicate>(p)), rtree_.qend(),
                               Allocator::TrackingAllocator<doc_type>{allocated_}};
  std::erase_if(results, std::forward<Filter>(f));
  return results;
}

template <typename cs>
auto RTree<cs>::contains(doc_type const& query_doc, geom_type const& query_geom) const
    -> query_results {
  return apply_predicate(bgi::contains(get_rect<cs>(query_doc)), [&](auto const& doc) -> bool {
    auto geom = lookup(doc);
    return geom.has_value() && std::visit(filter_results<cs>, query_geom, *geom);
  });
}

template <typename cs>
auto RTree<cs>::within(doc_type const& query_doc, geom_type const& query_geom) const
    -> query_results {
  return apply_predicate(bgi::within(get_rect<cs>(query_doc)), [&](auto const& doc) -> bool {
    auto geom = lookup(doc);
    return geom.has_value() && std::visit(filter_results<cs>, *geom, query_geom);
  });
}

template <typename cs>
auto RTree<cs>::generate_predicate(doc_type const& query_doc, QueryType query_type,
                                   geom_type const& query_geom) const -> query_results {
  switch (query_type) {
    case QueryType::CONTAINS:
      return contains(query_doc, query_geom);
    case QueryType::WITHIN:
      return within(query_doc, query_geom);
    default:
      throw std::runtime_error{"unknown query"};
  }
}

template <typename cs>
auto RTree<cs>::query(std::string_view wkt, QueryType query_type, RedisModuleString** err_msg) const
    -> IndexIterator* {
  try {
    const auto query_geom = from_wkt<cs>(wkt);
    auto results = generate_predicate(query_type, query_geom);
    auto geometry_query_iterator =
        new (allocated_) QueryIterator{results | std::views::transform(get_id<cs>), allocated_};
    return geometry_query_iterator->base();
  } catch (const std::exception& e) {
    if (err_msg) {
      *err_msg = RedisModule_CreateString(nullptr, e.what(), std::strlen(e.what()));
    }
    return nullptr;
  }
}

#define X(variant) template class RTree<variant>;
GEO_VARIANTS(X)
#undef X
}  // namespace GeoShape
}  // namespace RediSearch
