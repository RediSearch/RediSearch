---
title: "Basic constructs"
linkTitle: "Basic constructs"
description: Basic constructs for searching and querying Redis data
weight: 2
---



You can use Redis Stack as a powerful search and query engine. It allows you to create indexes and perform efficient queries on structured data, as well as text-based and vector similarity searches on unstructured data.

This section introduces the basic constructs of querying and searching and explains how to use them to build powerful search capabilities into your applications.

## Documents

A document is the basic unit of information. It can be any data object that you want to index and search, stored as a Hash or JSON. Each document is uniquely identifiable by the key name.

## Fields

A document consists of multiple fields, where each field represents a specific attribute or property of the document. Fields can store different types of data, such as strings, numbers, geo-location or even more complex structures like vectors. By indexing these fields, you enable efficient querying and searching based on their values.

Not all documents need to have the same fields. You can include or exclude fields based on the specific requirements of your application or data model.

## Indexing Fields

Not all fields are relevant to perform search operations, and indexing all fields may lead to unnecessary overhead. That's why, you have the flexibility to choose which fields should be indexed for efficient search operations. By indexing a field, you enable Redis Stack to create an index structure that optimizes search performance on that field.

Fields that are not indexed will not contribute to search results. However, they can still be retrieved as part of the document data when fetching search results.

## Schema

The index structure in defined by the schema. The schema defines how fields are stored and indexed within the index. It specifies the type of each field, whether it should be indexed or not, and any additional configuration options.

To create an index, you need to define the schema for your collection. Learn more about how to define the schema on the [Schema definition](/docs/interact/search-and-query/basic-constructs/schema-definition/) page.




## Learn more: