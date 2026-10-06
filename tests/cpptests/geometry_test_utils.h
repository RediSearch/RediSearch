/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#pragma once

#include "redismock/redismock.h"  // RMCK_GetReplyLog
#include "geometry/geometry_api.h"

#include <ostream>
#include <string>

// Points for even i, 0.5-degree squares for odd i.
inline std::string shapeAt(int i) {
  const int x = 1 + i % 50, y = 1 + i / 50;
  if (i % 2 == 0) return "POINT(" + std::to_string(x) + " " + std::to_string(y) + ")";
  const std::string X = std::to_string(x), Y = std::to_string(y);
  const std::string X2 = std::to_string(x + 0.5), Y2 = std::to_string(y + 0.5);
  return "POLYGON((" + X + " " + Y + ", " + X + " " + Y2 + ", " + X2 + " " + Y2 + ", " + X2 + " " +
         Y + ", " + X + " " + Y + "))";
}

// From dump's reply log: per R-tree entry, array:6 if its id has a geometry, array:4 if not.
struct TreeShape {
  long numDocs = 0, withGeom = 0, withoutGeom = 0;
  bool operator==(const TreeShape &o) const {
    return numDocs == o.numDocs && withGeom == o.withGeom && withoutGeom == o.withoutGeom;
  }
  friend std::ostream &operator<<(std::ostream &os, const TreeShape &s) {
    return os << "{numDocs=" << s.numDocs << ", withGeom=" << s.withGeom
              << ", withoutGeom=" << s.withoutGeom << "}";
  }
};

inline TreeShape treeShapeOf(const GeometryIndex *idx) {
  RedisModuleCtx *ctx = RedisModule_GetThreadSafeContext(nullptr);
  GeometryApi_Get(idx)->dump(idx, ctx);
  const auto &log = RMCK_GetReplyLog(ctx);
  TreeShape s;
  s.numDocs = std::stol(log.at(1).substr(sizeof("array:") - 1));
  for (size_t i = 2; i < log.size(); ++i) {
    if (log[i] == "array:6") ++s.withGeom;
    if (log[i] == "array:4") ++s.withoutGeom;
  }
  RedisModule_FreeThreadSafeContext(ctx);
  return s;
}
