/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "rtree.hpp"

namespace RediSearch {
namespace GeoShape {

template <typename cs>
RTree<cs>::RTree(size_t& alloc_ref)
    : alloc_{alloc_ref},
      rtree_{bgi::quadratic<16>{}, bgi::indexable<doc_type>{}, bgi::equal_to<doc_type>{},
             Allocator::TrackingAllocator<doc_type>{alloc_ref}},
      docLookup_{0, Allocator::TrackingAllocator<LUT_value_type>{alloc_ref}} {
}
template <typename cs>
constexpr auto RTree<cs>::make_mbr(geom_type const& geom) -> rect_type {
  return std::visit(
      [](auto&& geom) {
        if constexpr (std::is_same_v<point_type, std::decay_t<decltype(geom)>>) {
          constexpr auto EPSILON = 1e-10;
          auto p1 = point_type{bg::get<0>(geom) - EPSILON, bg::get<1>(geom) - EPSILON};
          auto p2 = point_type{bg::get<0>(geom) + EPSILON, bg::get<1>(geom) + EPSILON};
          return rect_type{p1, p2};
        } else {
          return bg::return_envelope<rect_type>(geom);
        }
      },
      geom);
}
template <typename cs>
constexpr auto RTree<cs>::make_doc(geom_type const& geom, t_docId id) -> doc_type {
  return doc_type{make_mbr(geom), id};
}
template <typename cs>
constexpr auto RTree<cs>::get_rect(doc_type const& doc) -> rect_type {
  return doc.first;
}
template <typename cs>
constexpr auto RTree<cs>::get_id(doc_type const& doc) -> t_docId {
  return doc.second;
}

template <typename cs>
auto RTree<cs>::lookup(t_docId id) const -> std::optional<std::reference_wrapper<const geom_type>> {
  if (auto it = docLookup_.find(id); it != docLookup_.end()) {
    return it->second;
  }
  return std::nullopt;
}
template <typename cs>
auto RTree<cs>::lookup(doc_type const& doc) const
    -> std::optional<std::reference_wrapper<const geom_type>> {
  return lookup(get_id(doc));
}

template <typename cs>
auto RTree<cs>::from_wkt(std::string_view wkt) const -> geom_type {
  geom_type geom{};
  if (wkt.starts_with("POI")) {
    geom = bg::from_wkt<point_type>(std::string{wkt});
  } else if (wkt.starts_with("POL")) {
    geom = bg::from_wkt<poly_type>(std::string{wkt});
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

template <typename cs>
constexpr auto geometry_reporter = [](auto&& geom) {
  using point_type = typename RTree<cs>::point_type;
  if constexpr (std::is_same_v<point_type, std::decay_t<decltype(geom)>>) {
    return 0ul;
  } else {
    auto const& inners = geom.inners();
    auto outer_size = geom.outer().get_allocator().report();
    return std::accumulate(
        inners.begin(), inners.end(), outer_size,
        [](size_t acc, auto&& hole) { return acc + hole.get_allocator().report(); });
  }
};

template <typename cs>
void RTree<cs>::insert(geom_type const& geom, t_docId id) {
  rtree_.insert(make_doc(geom, id));
  docLookup_.insert(LUT_value_type{id, geom});
  alloc_.allocated_ += std::visit(geometry_reporter<cs>, geom);
}

template <typename cs>
int RTree<cs>::insertWKT(std::string_view wkt, t_docId id, RedisModuleString** err_msg) {
  try {
    insert(from_wkt(wkt), id);
    return 0;
  } catch (const std::exception& e) {
    if (err_msg) {
      *err_msg = RedisModule_CreateString(nullptr, e.what(), strlen(e.what()));
    }
    return 1;
  }
}

template <typename cs>
bool RTree<cs>::remove(t_docId id) {
  if (auto geom = lookup(id); geom.has_value()) {
    rtree_.remove(make_doc(geom.value(), id));
    alloc_.allocated_ -= std::visit(geometry_reporter<cs>, geom.value().get());
    docLookup_.erase(id);
    return true;
  }
  return false;
}

template <typename T>
auto to_string(T const& t) -> string {
  using sstream = std::basic_stringstream<char, std::char_traits<char>, Allocator::Allocator<char>>;
  auto ss = sstream{};
  ss << t;
  return ss.str();
}
template <typename cs>
auto RTree<cs>::geometry_to_string(geom_type const& geom) -> string {
  return std::visit([&](auto&& geom) { return to_string(bg::wkt(geom)); }, geom);
}
template <typename cs>
auto RTree<cs>::doc_to_string(doc_type const& doc) -> string {
  return to_string(bg::wkt(get_rect(doc)));
}

template <typename cs>
void RTree<cs>::dump(RedisModuleCtx* ctx) const {
  size_t lenTop = 0;
  RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);

  RedisModule_ReplyWithStringBuffer(ctx, "type", strlen("type"));
  RedisModule_ReplyWithStringBuffer(ctx, "boost_rtree", strlen("boost_rtree"));
  lenTop += 2;

  RedisModule_ReplyWithStringBuffer(ctx, "ptr", strlen("ptr"));
  auto addr = to_string(&rtree_);
  RedisModule_ReplyWithStringBuffer(ctx, addr.c_str(), addr.length());
  lenTop += 2;

  RedisModule_ReplyWithStringBuffer(ctx, "num_docs", strlen("num_docs"));
  RedisModule_ReplyWithLongLong(ctx, static_cast<long long>(rtree_.size()));
  lenTop += 2;

  RedisModule_ReplyWithStringBuffer(ctx, "docs", strlen("docs"));
  RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
  lenTop += 2;

  size_t lenDocs = 0;
  std::ranges::for_each(rtree_, [&](doc_type const& doc) {
    lenDocs += 1;
    size_t lenValues = 0;
    RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);

    RedisModule_ReplyWithStringBuffer(ctx, "id", strlen("id"));
    RedisModule_ReplyWithLongLong(ctx, get_id(doc));
    lenValues += 2;

    if (auto geom = lookup(doc); geom.has_value()) {
      RedisModule_ReplyWithStringBuffer(ctx, "geoshape", strlen("geoshape"));
      auto str = geometry_to_string(geom.value());
      RedisModule_ReplyWithStringBuffer(ctx, str.c_str(), str.length());
      lenValues += 2;
    }
    RedisModule_ReplyWithStringBuffer(ctx, "rect", strlen("rect"));
    auto str = doc_to_string(doc);
    RedisModule_ReplyWithStringBuffer(ctx, str.c_str(), str.length());
    lenValues += 2;

    RedisModule_ReplySetArrayLength(ctx, lenValues);
  });
  RedisModule_ReplySetArrayLength(ctx, lenDocs);
  RedisModule_ReplySetArrayLength(ctx, lenTop);
}

template <typename cs>
size_t RTree<cs>::report() const noexcept {
  return alloc_.report();
}

template <typename cs>
auto RTree<cs>::generate_query_iterator(query_results&& results, auto&& alloc) -> IndexIterator* {
  auto p = Allocator::TrackingAllocator<QueryIterator>{alloc.allocated_}.allocate(1);
  auto geometry_query_iterator = std::construct_at(
      p, results | std::views::transform([](auto&& doc) { return get_id(doc); }), alloc);
  return geometry_query_iterator->base();
}

template <typename cs>
template <typename Predicate>
auto RTree<cs>::apply_predicate(Predicate p) const -> query_results {
  query_results results{Allocator::TrackingAllocator<doc_type>{alloc_.allocated_}};
  rtree_.query(p, std::back_inserter(results));
  return results;
}

template <typename cs>
constexpr auto filter_results = [](auto&& geom1, auto&& geom2) {
  using point_type = typename RTree<cs>::point_type;
  if constexpr (std::is_same_v<point_type, std::decay_t<decltype(geom2)>> &&
                !std::is_same_v<point_type, std::decay_t<decltype(geom1)>>) {
    return false;
  } else {
    return !bg::within(geom1, geom2);
  }
};

template <typename cs>
auto RTree<cs>::contains(doc_type const& query_doc, geom_type const& query_geom) const
    -> query_results {
  auto results = apply_predicate(bgi::contains(get_rect(query_doc)));
  std::erase_if(results, [&](auto const& doc) {
    auto geom = lookup(doc);
    return geom.has_value() && std::visit(filter_results<cs>, query_geom, geom.value().get());
  });
  return results;
}

template <typename cs>
auto RTree<cs>::within(doc_type const& query_doc, geom_type const& query_geom) const -> query_results {
  auto results = apply_predicate(bgi::within(get_rect(query_doc)));
  std::erase_if(results, [&](auto const& doc) {
    auto geom = lookup(doc);
    return geom.has_value() && std::visit(filter_results<cs>, geom.value().get(), query_geom);
  });
  return results;
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
    auto query_geom = from_wkt(wkt);
    return generate_query_iterator(generate_predicate(make_doc(query_geom), query_type, query_geom),
                                   alloc_);
  } catch (const std::exception& e) {
    if (err_msg) {
      *err_msg = RedisModule_CreateString(nullptr, e.what(), strlen(e.what()));
    }
    return nullptr;
  }
}

#define X(variant) template class RTree<variant>;
GEO_VARIANTS(X)
#undef X
}  // namespace GeoShape
}  // namespace RediSearch
