/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "rtree.hpp"

namespace RediSearch {
namespace GeoShape {

namespace bg = boost::geometry;
namespace bgm = bg::model;
namespace bgi = bg::index;

template <typename coord_system>
RTree<coord_system>::RTree(std::size_t& alloc_ref)
    : alloc_{alloc_ref},
      rtree_{bgi::quadratic<16>{}, bgi::indexable<doc_type>{}, bgi::equal_to<doc_type>{},
             TrackingAllocator<doc_type>{alloc_ref}},
      docLookup_{0, TrackingAllocator<LUT_value_type>{alloc_ref}} {
}
template <typename coord_system>
constexpr auto RTree<coord_system>::make_mbr(geom_type const& geom) -> rect_type {
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
template <typename coord_system>
constexpr auto RTree<coord_system>::make_doc(geom_type const& geom, t_docId id) -> doc_type {
  return doc_type{make_mbr(geom), id};
}
template <typename coord_system>
constexpr auto RTree<coord_system>::get_rect(doc_type const& doc) -> rect_type {
  return doc.first;
}
template <typename coord_system>
constexpr auto RTree<coord_system>::get_id(doc_type const& doc) -> t_docId {
  return doc.second;
}

template <typename coord_system>
auto RTree<coord_system>::lookup(t_docId id) const
    -> std::optional<std::reference_wrapper<const geom_type>> {
  if (auto it = docLookup_.find(id); it != docLookup_.end()) {
    return it->second;
  }
  return std::nullopt;
}
template <typename coord_system>
auto RTree<coord_system>::lookup(doc_type const& doc) const
    -> std::optional<std::reference_wrapper<const geom_type>> {
  return lookup(get_id(doc));
}

template <typename coord_system>
auto RTree<coord_system>::from_wkt(std::string_view wkt) const -> geom_type {
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

template <typename coord_system>
void RTree<coord_system>::insert(geom_type const& geom, t_docId id) {
  rtree_.insert(make_doc(geom, id));
  docLookup_.insert(LUT_value_type{id, geom});
}

template <typename coord_system>
int RTree<coord_system>::insertWKT(const char* wkt, std::size_t len, t_docId id,
                                   RedisModuleString** err_msg) {
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

template <typename coord_system>
bool RTree<coord_system>::remove(const doc_type& doc) {
  return rtree_.remove(doc);
}

template <typename coord_system>
bool RTree<coord_system>::remove(t_docId id) {
  if (auto geom = lookup(id); geom.has_value()) {
    remove(make_doc(geom.value(), id));
    docLookup_.erase(id);
    return true;
  }
  return false;
}

template <typename T>
auto to_string(T const& t) -> string {
  using sstream = std::basic_stringstream<char, std::char_traits<char>, Allocator<char>>;
  auto ss = sstream{};
  ss << t;
  return ss.str();
}
template <typename coord_system>
auto RTree<coord_system>::geometry_to_string(geom_type const& geom) -> string {
  return std::visit([&](auto&& geom) { return to_string(bg::wkt(geom)); }, geom);
}
template <typename coord_system>
auto RTree<coord_system>::doc_to_string(doc_type const& doc) -> string {
  return to_string(bg::wkt(get_rect(doc)));
}

template <typename coord_system>
void RTree<coord_system>::dump(RedisModuleCtx* ctx) const {
  std::size_t lenTop = 0;
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

  std::size_t lenDocs = 0;
  std::ranges::for_each(rtree_, [&](doc_type const& doc) {
    lenDocs += 1;
    std::size_t lenValues = 0;
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

template <typename coord_system>
std::size_t RTree<coord_system>::report() const {
  auto tracked = alloc_.report();
  return std::accumulate(docLookup_.begin(), docLookup_.end(), tracked, [](std::size_t acc, auto&& value) {
    auto const& geom = value.second;
    return acc + std::visit([](auto&& geom) {
      if constexpr (std::is_same_v<point_type, std::decay_t<decltype(geom)>>) {
        return 0ul;
      } else {
        auto const& inners = geom.inners();
        auto outer_size = geom.outer().get_allocator().report();
        return std::accumulate(inners.begin(), inners.end(), outer_size, [](std::size_t acc, auto&& hole) {
          return acc + hole.get_allocator().report();
        });
      }
    }, geom);
  });
}

template <typename coord_system>
auto RTree<coord_system>::generate_query_iterator(ResultsVec&& results,
                                                  TrackingAllocator<QueryIterator>&& a)
    -> IndexIterator* {
  auto p = a.allocate(1);
  auto geometry_query_iterator =
      std::construct_at(p, results | std::views::transform([](auto&& doc) { return get_id(doc); }),
                        TrackingAllocator<t_docId>{a.allocated_});
  return geometry_query_iterator->base();
}

template <typename coord_system>
template <typename Predicate>
auto RTree<coord_system>::query(Predicate p) const -> ResultsVec {
  ResultsVec results{TrackingAllocator<doc_type>{alloc_.allocated_}};
  rtree_.query(p, std::back_inserter(results));
  return results;
}

template <typename coord_system>
constexpr auto filter_results = [](auto&& g1, auto&& g2) {
  using point_type = typename RTree<coord_system>::point_type;
  if constexpr (std::is_same_v<point_type, std::decay_t<decltype(g2)>> &&
                !std::is_same_v<point_type, std::decay_t<decltype(g1)>>) {
    return false;
  } else {
    return !bg::within(g1, g2);
  }
};

template <typename coord_system>
auto RTree<coord_system>::contains(doc_type const& queryDoc, geom_type const& queryGeom) const
    -> ResultsVec {
  auto results = query(bgi::contains(get_rect(queryDoc)));
  std::erase_if(results, [&](auto const& doc) {
    auto geom = lookup(doc);
    return geom && std::visit(filter_results<coord_system>, queryGeom, geom.value().get());
  });
  return results;
}

template <typename coord_system>
auto RTree<coord_system>::within(doc_type const& queryDoc, geom_type const& queryGeom) const
    -> ResultsVec {
  auto results = query(bgi::within(get_rect(queryDoc)));
  std::erase_if(results, [&](auto const& doc) {
    auto geom = lookup(doc);
    return geom && std::visit(filter_results<coord_system>, geom.value().get(), queryGeom);
  });
  return results;
}

template <typename coord_system>
auto RTree<coord_system>::query(doc_type const& queryDoc, QueryType queryType,
                                geom_type const& queryGeom) const -> ResultsVec {
  switch (queryType) {
    case QueryType::CONTAINS:
      return contains(queryDoc, queryGeom);
    case QueryType::WITHIN:
      return within(queryDoc, queryGeom);
    default:
      throw std::runtime_error{"unknown query"};
  }
}

template <typename coord_system>
auto RTree<coord_system>::query(const char* wkt, std::size_t len, QueryType query_type,
                                RedisModuleString** err_msg) const -> IndexIterator* {
  try {
    auto query_geom = from_wkt(std::string_view{wkt, len});
    return generate_query_iterator(query(make_doc(query_geom), query_type, query_geom),
                                   TrackingAllocator<QueryIterator>{alloc_.allocated_});
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
