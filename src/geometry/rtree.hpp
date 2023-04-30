/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#pragma once

#include "rtdoc.hpp"
#include "query_iterator.hpp"
#include "rtree.h"
//#include <boost/unordered/unordered_map.hpp> //is faster than std::unordered_map?

namespace bg = boost::geometry;
namespace bgi = bg::index;

struct RTree {
  using parameter_type = bgi::quadratic<16>;
  using rtree_internal =
      bgi::rtree<RTDoc, parameter_type, RTDoc_Indexable, RTDoc_EqualTo, rm_allocator<RTDoc>>;
  using docLookup_internal = 
    std::unordered_map<t_docId, RTDoc::poly_type, std::hash<t_docId>, std::equal_to<t_docId>, rm_allocator<std::pair<const t_docId, RTDoc::poly_type>>>;
  
  
  rtree_internal rtree_;
  docLookup_internal docLookup_;

  explicit RTree() = default;
  explicit RTree(rtree_internal const& rt) noexcept : rtree_{rt} {
  }

  [[nodiscard]] std::optional<std::reference_wrapper<const RTDoc::poly_type>> lookup(t_docId id) const {
    if (auto it = docLookup_.find(id); it != docLookup_.end()) {
      return it->second;
    }
    return {};
  }

  void insert(const RTDoc::poly_type& poly, t_docId id) {
    RTDoc doc{poly, id};
    rtree_.insert(doc);
    docLookup_.insert({id, poly});
  }

  bool remove(t_docId id) {
    auto doc = lookup(id);
    if (doc.has_value()) {
      rtree_.remove(RTDoc{doc.value(), id});
      docLookup_.erase(id);
      return true;
    }
    return false;
  }

  bool removeId(t_docId id) {
    return docLookup_.erase(id) != 0;
  }

  bool remove(const RTDoc& doc) {
    return rtree_.remove(doc);
  }

  [[nodiscard]] static string geometry_to_string(const RTDoc::poly_type& geometry) {
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
    RedisModule_ReplyWithLongLong(ctx, (long long)rtree_.size());
    lenTop += 2;

    RedisModule_ReplyWithStringBuffer(ctx, "docs", strlen("docs"));
    RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
    lenTop += 2;

    size_t lenDocs = 0;
    std::for_each(rtree_.qbegin(bgi::satisfies([](RTDoc const& d) { return true; })),
      rtree_.qend(),
      [this, &lenDocs, ctx](RTDoc const& d) {
          lenDocs += 1;
          size_t lenValues = 0;
          RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
          
          RedisModule_ReplyWithStringBuffer(ctx, "id", strlen("id"));
          RedisModule_ReplyWithLongLong(ctx, d.id());
          lenValues += 2;
          
          auto geometry = lookup(d.id());
          if (geometry.has_value()) {
            RedisModule_ReplyWithStringBuffer(ctx, "geometry", strlen("geometry"));
            auto str = geometry_to_string(geometry.value());
            RedisModule_ReplyWithStringBuffer(ctx, str.data(), str.size());
            lenValues += 2;
          }
          RedisModule_ReplyWithStringBuffer(ctx, "rect", strlen("rect"));
          auto str = d.rect_to_string();
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

  using ResultsVec = std::vector<RTDoc, rm_allocator<RTDoc>>;

	[[nodiscard]] ResultsVec query(RTDoc const& queryDoc, QueryType queryType, const RTDoc::poly_type& queryGeometry) const {
		switch (queryType) {
		 case QueryType::CONTAINS:
			return contains(queryDoc, queryGeometry);
		 case QueryType::WITHIN:
			return within(queryDoc, queryGeometry);
		 default:
			return {};
		}
	}

	[[nodiscard]] ResultsVec contains(RTDoc const& queryDoc, const RTDoc::poly_type& queryGeometry) const {
		auto results = query(bgi::contains(queryDoc.rect_));
		std::erase_if(results, [&](auto const& doc) {
      auto geometry = lookup(doc.id());
      return geometry && !bg::within(queryGeometry, geometry.value().get());
		});
		return results;
	}
	[[nodiscard]] ResultsVec within(RTDoc const& queryDoc, const RTDoc::poly_type& queryGeometry) const {
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
};
