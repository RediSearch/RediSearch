/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#pragma once

#include "geometry_types.h"
#include "allocator/tracking_allocator.hpp"
#include <boost/geometry.hpp>
#include <boost/geometry/geometries/register/ring.hpp>

namespace RediSearch {
namespace GeoShape {

template <typename point_type>
struct Ring : std::vector<point_type, Allocator::TrackingAllocator<point_type>> {};

template <typename point_type>
struct Polygon {
  using ring_type = Ring<point_type>;
  using interior_type = std::vector<ring_type, Allocator::TrackingAllocator<ring_type>>;

  ring_type boundary;
  interior_type holes;

  Polygon() = delete;
  template <typename T>
  Polygon(Allocator::TrackingAllocator<T> const& a)
      : boundary(Allocator::TrackingAllocator<point_type>{a}),
        holes(Allocator::TrackingAllocator<ring_type>{a}) {
  }
};

template <typename coord_system>
using Point = boost::geometry::model::point<double, 2, coord_system>;
using Cartesian = boost::geometry::cs::cartesian;
using Geographic = boost::geometry::cs::geographic<boost::geometry::degree>;
using CartesianRing = Ring<Point<Cartesian>>;
using GeographicRing = Ring<Point<Geographic>>;

}  // namespace GeoShape
}  // namespace RediSearch

BOOST_GEOMETRY_REGISTER_RING(RediSearch::GeoShape::CartesianRing);
BOOST_GEOMETRY_REGISTER_RING(RediSearch::GeoShape::GeographicRing);

// There is currently no registration macro for polygons. WHY?!?
namespace boost {
namespace geometry {
namespace traits {

namespace rs = RediSearch;
namespace rsg = rs::GeoShape;

template <typename point_type>
struct tag<rsg::Polygon<point_type>> {
  using type = boost::geometry::polygon_tag;
};
template <typename point_type>
struct ring_const_type<rsg::Polygon<point_type>> {
  using type = rsg::Polygon<point_type>::ring_type const&;
};
template <typename point_type>
struct ring_mutable_type<rsg::Polygon<point_type>> {
  using type = rsg::Polygon<point_type>::ring_type&;
};
template <typename point_type>
struct interior_const_type<rsg::Polygon<point_type>> {
  using type = rsg::Polygon<point_type>::interior_type const&;
};
template <typename point_type>
struct interior_mutable_type<rsg::Polygon<point_type>> {
  using type = rsg::Polygon<point_type>::interior_type&;
};
template <typename point_type>
struct exterior_ring<rsg::Polygon<point_type>> {
  using ring_type = rsg::Polygon<point_type>::ring_type;
  static auto get(rsg::Polygon<point_type>& p) -> ring_type& {
    return p.boundary;
  }
  static auto get(rsg::Polygon<point_type> const& p) -> ring_type const& {
    return p.boundary;
  }
};
template <typename point_type>
struct interior_rings<rsg::Polygon<point_type>> {
  using holes_type = rsg::Polygon<point_type>::interior_type;
  static auto get(rsg::Polygon<point_type>& p) -> holes_type& {
    return p.holes;
  }
  static auto get(rsg::Polygon<point_type> const& p) -> holes_type const& {
    return p.holes;
  }
};

}  // namespace traits
}  // namespace geometry
}  // namespace boost
