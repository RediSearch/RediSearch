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
template <typename... Ts>
struct overload : Ts... {
  using Ts::operator()...;
};
// template deduction guide might be necessary for clang? can't hurt to include it regardless.
template <typename ...Ts>
overload(Ts...) -> overload<Ts...>;

template <typename cs>
constexpr auto make_mbr = overload{
    [](typename RTree<cs>::point_type const& point) -> RTree<cs>::rect_type {
      constexpr auto INF = std::numeric_limits<long double>::infinity();
      const auto x = bg::get<0>(point);
      const auto y = bg::get<1>(point);
      const auto p1 = decltype(point){std::nexttoward(x, -INF), std::nexttoward(y, -INF)};
      const auto p2 = decltype(point){std::nexttoward(x, +INF), std::nexttoward(y, +INF)};
      return {p1, p2};
    },
    [](auto const& geom) { return bg::return_envelope<typename RTree<cs>::rect_type>(geom); }};

template <typename cs>
constexpr auto make_doc(typename RTree<cs>::geom_type const& geom, t_docId id = 0)
    -> RTree<cs>::doc_type {
  return {std::visit(make_mbr<cs>, geom), id};
}
template <typename cs>
constexpr auto get_rect(typename RTree<cs>::doc_type const& doc) -> RTree<cs>::rect_type {
  return doc.first;
}
template <typename cs>
constexpr auto get_id(typename RTree<cs>::doc_type const& doc) -> t_docId {
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
template <typename cs>
auto geometry_to_string(typename RTree<cs>::geom_type const& geom) -> string {
  return std::visit([](auto const& geom) -> string { return to_string(bg::wkt(geom)); }, geom);
}
template <typename cs>
auto doc_to_string(typename RTree<cs>::doc_type const& doc) -> string {
  return to_string(bg::wkt(get_rect<cs>(doc)));
}

// Ironically, the reason I introduced an overload set in the first place requires recursive
// lambdas and therefore does not work without `deducing this`, a C++23 feature.
// template <typename cs>
// constexpr auto to_string =
//     overload{
//         [](this auto&& self, typename RTree<cs>::geom_type const& geom) -> string {
//           return std::visit([](auto const& geom) -> string { return self(bg::wkt(geom)); }, geom);
//         },
//         [](this auto&& self, typename RTree<cs>::doc_type const& doc) -> string {
//           return self(bg::wkt(get_rect<cs>(doc)));
//         },
//         [](auto const& val) -> string {
//           using sstream =
//               std::basic_stringstream<char, std::char_traits<char>, Allocator::Allocator<char>>;
//           auto ss = sstream{};
//           ss << val;
//           return ss.str();
//         }};

template <typename cs>
auto from_wkt(std::string_view wkt) -> RTree<cs>::geom_type {
  auto geom = [&] -> RTree<cs>::geom_type {
    if (wkt.starts_with("POI")) {
      return bg::from_wkt<typename RTree<cs>::point_type>(std::string{wkt});
    } else if (wkt.starts_with("POL")) {
      return bg::from_wkt<typename RTree<cs>::poly_type>(std::string{wkt});
    } else {
      throw std::runtime_error{"unknown geometry type"};
    }
  }();
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
constexpr auto geometry_reporter =
    overload{[](typename RTree<cs>::point_type const& point) -> std::size_t { return 0; },
             [](auto const& geom) -> std::size_t {
               const auto& inners = geom.inners();
               const auto outer_size = geom.outer().get_allocator().report();
               // reduce allows out-of-order execution of associative and commutative binary
               // ops. transform to associative and commutative using a unary predicate.
               return std::transform_reduce(
#ifndef __APPLE__  // apple clang does not implement `std::execution` despite being a C++17 feature
                   std::execution::unseq,
#endif
                   std::begin(inners), std::end(inners), outer_size, std::plus{},
                   [](auto const& hole) { return hole.get_allocator().report(); });
             }};

template <typename cs>
constexpr auto within_filter = overload{
    [](auto const& geom1, typename RTree<cs>::point_type const& geom2) -> bool { return false; },
    [](auto const& geom1, auto const& geom2) -> bool { return bg::within(geom1, geom2); }};
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
  return boost::none;
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
  return lookup(id)
      .map([&](geom_type const& geom) {
        allocated_ -= std::visit(geometry_reporter<cs>, geom);
        rtree_.remove(make_doc<cs>(geom, id));
        docLookup_.erase(id);
        return true;
      })
      .value_or(false);
}

template <typename cs>
void RTree<cs>::dump(RedisModuleCtx* ctx) const {
  RedisModule_ReplyWithArray(ctx, 8);

  RedisModule_ReplyWithStringBuffer(ctx, "type", std::strlen("type"));
  RedisModule_ReplyWithStringBuffer(ctx, "boost_rtree", std::strlen("boost_rtree"));

  RedisModule_ReplyWithStringBuffer(ctx, "ptr", std::strlen("ptr"));
  const auto addr = to_string(&rtree_);
  RedisModule_ReplyWithStringBuffer(ctx, addr.c_str(), addr.length());

  RedisModule_ReplyWithStringBuffer(ctx, "num_docs", std::strlen("num_docs"));
  const auto len = static_cast<long long>(rtree_.size());
  RedisModule_ReplyWithLongLong(ctx, len);

  RedisModule_ReplyWithStringBuffer(ctx, "docs", std::strlen("docs"));
  RedisModule_ReplyWithArray(ctx, len);

  for (doc_type const& doc : rtree_) {
    const auto geom = lookup(doc);
    RedisModule_ReplyWithArray(ctx, 4 + (geom ? 2 : 0));

    RedisModule_ReplyWithStringBuffer(ctx, "id", std::strlen("id"));
    RedisModule_ReplyWithLongLong(ctx, get_id<cs>(doc));

    geom.map([ctx](geom_type const& geom) {
      RedisModule_ReplyWithStringBuffer(ctx, "geoshape", std::strlen("geoshape"));
      const auto str = geometry_to_string<cs>(geom);
      RedisModule_ReplyWithStringBuffer(ctx, str.c_str(), str.length());
      return 0;
    });

    RedisModule_ReplyWithStringBuffer(ctx, "rect", std::strlen("rect"));
    const auto str = doc_to_string<cs>(doc);
    RedisModule_ReplyWithStringBuffer(ctx, str.c_str(), str.length());
  }
}

template <typename cs>
std::size_t RTree<cs>::report() const noexcept {
  return allocated_;
}

template <typename cs>
template <typename Predicate, typename Filter>
auto RTree<cs>::query_begin(Predicate&& predicate, Filter&& filter) const -> query_results {
  return rtree_.qbegin(std::forward<Predicate>(predicate) &&
                       bgi::satisfies([this, f = std::move(filter)](auto const& doc) -> bool {
                         return lookup(doc).map(f).value_or(false);
                       }));
}

template <typename cs>
auto RTree<cs>::generate_predicate(QueryType query_type, geom_type const& query_geom) const
    -> query_results {
  const auto query_mbr = get_rect<cs>(make_doc<cs>(query_geom));
  switch (query_type) {
    case QueryType::CONTAINS:  // contains(g1, g2) == within(g2, g1)
      return query_begin(bgi::contains(query_mbr), [&](auto const& geom) -> bool {
        return std::visit(within_filter<cs>, query_geom, geom);
      });
    case QueryType::WITHIN:
      return query_begin(bgi::within(query_mbr), [&](auto const& geom) -> bool {
        return std::visit(within_filter<cs>, geom, query_geom);
      });
    default:
      throw std::runtime_error{"unknown query"};
  }
}

template <typename cs>
auto RTree<cs>::query(std::string_view wkt, QueryType query_type, RedisModuleString** err_msg) const
    -> IndexIterator* {
  try {
    using alloc_type = Allocator::TrackingAllocator<QueryIterator>;
    auto alloc = alloc_type{allocated_};
    const auto query_geom = from_wkt<cs>(wkt);  // lifetime begins here
    const auto qbegin = query_begin(query_type, query_geom);
    const auto qi = std::allocator_traits<alloc_type>::allocate(alloc, 1);
    std::allocator_traits<alloc_type>::construct(
        alloc, qi, std::ranges::subrange{qbegin, rtree_.qend()} | std::views::transform(get_id<cs>),
        allocated_);  // query_geom used here. is not dangling
    return qi->base();
    // query_geom lifetime ends here.
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
