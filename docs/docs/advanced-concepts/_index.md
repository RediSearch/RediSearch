---
title: "Advanced concepts"
linkTitle: "Advanced concepts"
weight: 7
description: Details about query syntax, aggregation, scoring, and other search and query options
aliases: 
    - /docs/stack/search/reference/
---

Redis Stack supports the following search and query features. This article provides you an overview.

## Indexing features

* Secondary indexing
* Vector indexing
* Index on [JSON](/docs/data-types/json/) documents
* Full-text indexing of multiple fields in a document
* Incremental indexing without performance loss
* Document deletion and updating with index garbage collection


## Query features

* Multi-field queries
* Query on [JSON](/docs/data-types/json/) documents
* [Aggregation](/docs/interact/search-and-query/search/aggregations/)
* Boolean queries with AND, OR, and NOT operators between subqueries
* Optional query clauses
* Retrieval of full document contents or only their IDs
* Exact phrase search and slop-based search
* [Numeric filters](/docs/interact/search-and-query/query/#numeric-filters-in-query) and ranges
* [Geo-filtering](/docs/interact/search-and-query/query/#geo-filters-in-query) using Redis [geo commands](/commands/?group=geo)
* [Vector similartiy search](/docs/interact/search-and-query/advanced-concepts/vectors/)


## Full-text search features

* [Prefix-based searches](/docs/interact/search-and-query/query/#prefix-matching)
* Field weights
* [Auto-complete](/docs/interact/search-and-query/administration/overview/#auto-complete) and fuzzy prefix suggestions
* [Stemming](/docs/interact/search-and-query/advanced-concepts/stemming/)-based query expansion for [many languages](/docs/interact/search-and-query/advanced-concepts/stemming//#supported-languages) using [Snowball](http://snowballstem.org/)
* Support for custom functions for query expansion and scoring (see [Extensions](/docs/interact/search-and-query/administration/extensions/))
* Unicode support (UTF-8 input required)
* Document ranking

## Cluster support

The search and query features of Redis Stack are also available for distributed databases that can scale to billions of documents and hundreds of servers.

## Supported platforms
Redis Stack is developed and tested on Linux and macOS on x86_64 CPUs.

Atom CPUs are not supported.

<br/>