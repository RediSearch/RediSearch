---
title: Index and search JSON documents
linkTitle: Index JSON
weight: 2
description: How to index and search JSON documents
---

In addition to indexing Redis hashes, RediSearch can also index JSON documents. To index JSON, you need to install the RediSearch and [RedisJSON](/docs/stack/json/) modules.

## Prerequisites

Before you can index and search JSON documents, you need a database with either:

- Redis Stack, which automatically includes RediSearch and RedisJSON

- Redis v6.x or later and the following modules installed and enabled:
   - RediSearch v2.2 or later
   - RedisJSON v2.0 or later

## Create index with JSON schema

You can specify `ON JSON` to inform RediSearch that you want to index JSON documents.

For the `SCHEMA`, you can provide [JSONPath](/docs/stack/json/path) expressions.
The result of each JSONPath expression is indexed and associated with a logical name (`attribute`).
This attribute (previously called `field`) is used in queries.

Here's the syntax to create a JSON index:

```sql
FT.CREATE {index_name} ON JSON SCHEMA {json_path} AS {attribute} {type}
```

Create a JSON index:

New index:

```sql
FT.CREATE itemIdx ON JSON PREFIX 1 item: SCHEMA $.name AS name TEXT $.description as description TEXT $.price AS price NUMERIC
```

Note: The `attribute` is optional for `FT.CREATE`.

## Add JSON documents

After you create the index, any existing JSON documents, or any new JSON document added or modified, is
automatically indexed.

You can use any write command from the RedisJSON module `JSON.SET`, `JSON.ARRAPPEND`, etc.).

This example uses the following JSON documents:

Item 1:

```json
{
  "name": "Noise-cancelling Bluetooth headphones",
  "description": "Wireless Bluetooth headphones with noise-cancelling technology",
  "connection": {
    "wireless": true,
    "type": "Bluetooth"
  },
  "price": 99.98,
  "stock": 25,
  "colors": [
    "black",
    "silver"
  ]
}
```

Item 2:

```json
{
  "name": "Wireless earbuds",
  "description": "Wireless Bluetooth in-ear headphones",
  "connection": {
    "wireless": true,
    "type": "Bluetooth"
  },
  "price": 64.99,
  "stock": 17,
  "colors": [
    "black",
    "white"
  ]
}
```

Use `JSON.SET` to store the document in the database:


New example:

```sql
> JSON.SET item:1 $ '{"name":"Noise-cancelling Bluetooth headphones","description":"Wireless Bluetooth headphones with noise-cancelling technology","connection":{"wireless":true,"type":"Bluetooth"},"price":99.98,"stock":25,"colors":["black","silver"]}'
"OK"
> JSON.SET item:2 $ '{"name":"Wireless earbuds","description":"Wireless Bluetooth in-ear headphones","connection":{"wireless":true,"type":"Bluetooth"},"price":64.99,"stock":17,"colors":["black","white"]}'
"OK"
```

Because indexing is synchronous, the document will be visible on the index as soon as the `JSON.SET` command returns.
Any subsequent query that matches the indexed content will return the document.

## Search the index

To search for documents, use the `FT.SEARCH` command.
You can search any attribute defined in the SCHEMA.


Search for items with the word "earbuds" in the name:

```sql
> FT.SEARCH itemIdx '@name:(earbuds)'
1) "1"
2) "item:2"
3) 1) "$"
   2) "{\"name\":\"Wireless earbuds\",\"description\":\"Wireless Bluetooth in-ear headphones\",\"connection\":{\"wireless\":true,\"connection\":\"Bluetooth\"},\"price\":64.99,\"stock\":17,\"colors\":[\"black\",\"white\"]}"
```

Search for all items that include "bluetooth" and "headphones" in the description:

```sql
> FT.SEARCH itemIdx '@description:(bluetooth headphones)'
1) "2"
2) "item:1"
3) 1) "$"
   2) "{\"name\":\"Noise-cancelling Bluetooth headphones\",\"description\":\"Wireless Bluetooth headphones with noise-cancelling technology\",\"connection\":{\"wireless\":true,\"type\":\"Bluetooth\"},\"price\":99.98,\"stock\":25,\"colors\":[\"black\",\"silver\"]}"
4) "item:2"
5) 1) "$"
   2) "{\"name\":\"Wireless earbuds\",\"description\":\"Wireless Bluetooth in-ear headphones\",\"connection\":{\"wireless\":true,\"connection\":\"Bluetooth\"},\"price\":64.99,\"stock\":17,\"colors\":[\"black\",\"white\"]}"
```

Search for items with a price less than 70:

```sql
> FT.SEARCH itemIdx '@description:(bluetooth headphones) @price:[0 70]'
1) "1"
2) "item:2"
3) 1) "$"
   2) "{\"name\":\"Wireless earbuds\",\"description\":\"Wireless Bluetooth in-ear headphones\",\"connection\":{\"wireless\":true,\"connection\":\"Bluetooth\"},\"price\":64.99,\"stock\":17,\"colors\":[\"black\",\"white\"]}"
```

For information about search queries, see [Search query syntax](/docs/stack/search/reference/query_syntax).

Note: `FT.SEARCH` and  `FT.AGGREGATE` queries require `attribute` modifiers. Don't use JSONPath expressions in queries because the query parser doesn't fully support them.

## Index JSON arrays

It is possible to index scalar string and boolean values in JSON arrays by using the wildcard operator in the JSON Path.

You can apply an index to the `colors` field by specifying the JSON Path `$.colors.*` in your schema creation:

New index:

```sql
FT.CREATE itemIdx2 ON JSON PREFIX 1 item: SCHEMA $.colors.* AS colors TAG $.name AS name TEXT $.description as description TEXT
```

Search for headphones available in the color silver:

```sql
127.0.0.1:6379> FT.SEARCH itemIdx2 "@colors:{silver} (@name:(headphones)|@description:(headphones))"
1) "1"
2) "item:1"
3) 1) "$"
   2) "{\"name\":\"Noise-cancelling Bluetooth headphones\",\"description\":\"Wireless Bluetooth headphones with noise-cancelling technology\",\"connection\":{\"wireless\":true,\"type\":\"Bluetooth\"},\"price\":99.98,\"stock\":25,\"colors\":[\"black\",\"silver\"]}"
```

## Index JSON objects

You cannot index JSON objects. If the JSONPath expression returns an object, it will be ignored.

If you want to index the contents of a JSON object, you need to index the individual elements within the object in separate attributes.

To index the `customer-reviews` JSON object, define the `rating` and `review` fields as separate attributes when you create the index:

```sql
> FT.CREATE itemIdx3 ON JSON SCHEMA $.connection.wireless AS wireless TAG $.connection.type AS connectionType TEXT
"OK"
```

Search for items with the wireless tag set to `true`:

```sql
> FT.SEARCH itemIdx3 '@wireless:{true}'
1) "2"
2) "item:2"
3) 1) "$"
   2) "{\"name\":\"Wireless earbuds\",\"description\":\"Wireless Bluetooth in-ear headphones\",\"connection\":{\"wireless\":true,\"connection\":\"Bluetooth\"},\"price\":64.99,\"stock\":17,\"colors\":[\"black\",\"white\"]}"
4) "item:1"
5) 1) "$"
   2) "{\"name\":\"Noise-cancelling Bluetooth headphones\",\"description\":\"Wireless Bluetooth headphones with noise-cancelling technology\",\"connection\":{\"wireless\":true,\"type\":\"Bluetooth\"},\"price\":99.98,\"stock\":25,\"colors\":[\"black\",\"silver\"]}"
```

Search for items with a Bluetooth connection type:

```sql
> FT.SEARCH itemIdx3 '@connectionType:(bluetooth)'
1) "2"
2) "item:1"
3) 1) "$"
   2) "{\"name\":\"Noise-cancelling Bluetooth headphones\",\"description\":\"Wireless Bluetooth headphones with noise-cancelling technology\",\"connection\":{\"wireless\":true,\"type\":\"Bluetooth\"},\"price\":99.98,\"stock\":25,\"colors\":[\"black\",\"silver\"]}"
4) "item:2"
5) 1) "$"
   2) "{\"name\":\"Wireless earbuds\",\"description\":\"Wireless Bluetooth in-ear headphones\",\"connection\":{\"wireless\":true,\"type\":\"Bluetooth\"},\"price\":64.99,\"stock\":17,\"colors\":[\"black\",\"white\"]}"
```

## Field projection

`FT.SEARCH` returns the whole document by default. If you want to limit the returned search results to a specific attribute, you can use field projection.

### Project with attribute name

This example only returns the `name` and `price` of each set of headphones:

```sql
> FT.SEARCH itemIdx '@description:(headphones)' RETURN 2 name price
1) "2"
2) "item:1"
3) 1) "name"
   2) "Noise-cancelling Bluetooth headphones"
   3) "price"
   4) "99.98"
4) "item:2"
5) 1) "name"
   2) "Wireless earbuds"
   3) "price"
   4) "64.99"
```

### Project with JSONPath

The `RETURN` parameter also accepts a JSONPath expression which lets you extract any part of the JSON document.

The following example returns the result of the JSONPath expression `$.stock`.

```sql
> FT.SEARCH itemIdx '@description:(headphones)' RETURN 3 name price $.stock
1) "2"
2) "item:1"
3) 1) "name"
   2) "Noise-cancelling Bluetooth headphones"
   3) "price"
   4) "99.98"
   5) "$.stock"
   6) "25"
4) "item:2"
5) 1) "name"
   2) "Wireless earbuds"
   3) "price"
   4) "64.99"
   5) "$.stock"
   6) "17"
```

Note that the property name is the JSON expression itself: `3) 1) "$.stock"`

Using the `AS` option, you can also alias the returned property.

```sql
> FT.SEARCH itemIdx '@description:(headphones)' RETURN 5 name price $.stock AS stock
1) "2"
2) "item:1"
3) 1) "name"
   2) "Noise-cancelling Bluetooth headphones"
   3) "price"
   4) "99.98"
   5) "stock"
   6) "25"
4) "item:2"
5) 1) "name"
   2) "Wireless earbuds"
   3) "price"
   4) "64.99"
   5) "stock"
   6) "17"
```

### Highlighting

You can [highlight](/docs/stack/search/reference/highlight/) any attribute as soon as it is indexed using the TEXT type.

For `FT.SEARCH`, you have to explicitly set the attribute in the `RETURN` and the `HIGHLIGHT` parameters.

Highlight the word "bluetooth" with bold HTML tags in item names and descriptions:

```sql
> FT.SEARCH itemIdx '(@name:(bluetooth))|(@description:(bluetooth))' RETURN 3 name description price HIGHLIGHT FIELDS 2 name description TAGS '<b>' '</b>'
1) "2"
2) "item:1"
3) 1) "name"
   2) "Noise-cancelling <b>Bluetooth</b> headphones"
   3) "description"
   4) "Wireless <b>Bluetooth</b> headphones with noise-cancelling technology"
   5) "price"
   6) "99.98"
4) "item:2"
5) 1) "name"
   2) "Wireless earbuds"
   3) "description"
   4) "Wireless <b>Bluetooth</b> in-ear headphones"
   5) "price"
   6) "64.99"
```

## Aggregate with JSONPath

You can use [aggregation](/docs/stack/search/reference/aggregations) to generate statistics or build facet queries.

The `LOAD` parameter accepts JSONPath expressions. You can use any value in the pipeline, even if the value is not indexed.

This example calculates a 10% price discount and sorts the items from least expensive to most expensive:

```sql
> FT.AGGREGATE itemIdx '*' LOAD 4 name $.price AS originalPrice APPLY '@originalPrice - (@originalPrice * 0.10)' AS salePrice SORTBY 2 @salePrice ASC
1) "2"
2) 1) "name"
   2) "Wireless earbuds"
   3) "originalPrice"
   4) "64.99"
   5) "salePrice"
   6) "58.491"
3) 1) "name"
   2) "Noise-cancelling Bluetooth headphones"
   3) "originalPrice"
   4) "99.98"
   5) "salePrice"
   6) "89.982"
```

## Index limitations

### Schema mapping

During index creation, you need to map the JSON elements to `SCHEMA` fields as follows:

- Strings as TEXT, TAG, or GEO.
- Numbers as NUMERIC.
- Booleans as TAG.
- JSON array of strings or booleans in a TAG field. Other types (NUMERIC, GEO, NULL) are not supported.
- You cannot index JSON objects. Index the individual elements as separate attributes instead.
- NULL values are ignored.

### TAG not sortable

If you create an index for JSON documents, you cannot sort on a `TAG` field:

```sql
> FT.CREATE itemIdx4 ON JSON PREFIX 1 item: SCHEMA $.colors.* AS colors TAG SORTABLE
"On JSON, cannot set tag field to sortable - colors"
```

With hashes, you can use `SORTABLE` (as a side effect) to improve the performance of `FT.AGGREGATE` on `TAG`fields.
This is possible because the value in the hash is a string, such as "black,white,silver".

However with JSON, you can index an array of strings.
Because there is no valid single textual representation of those values,
RediSearch doesn't know how to sort the result.
