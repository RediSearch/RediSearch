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

```sql
FT.CREATE userIdx ON JSON SCHEMA $.user.name AS name TEXT $.user.tag AS country TAG
```

Note: The `attribute` is optional for `FT.CREATE`.

## Add JSON documents

After you create the index, any existing JSON documents, or any new JSON document added or modified, is
automatically indexed.

You can use any write command from the RedisJSON module `JSON.SET`, `JSON.ARRAPPEND`, etc.).

This example uses the following JSON document:

```JSON
{
  "user": {
    "name": "John Smith",
    "tag": "apprentice,knight",
    "hp": "1000",
    "dmg": "150"
  }
}
```

Use `JSON.SET` to store the document in the database:

```sql
JSON.SET myDoc $ '{"user":{"name":"John Smith","tag":"apprentice,knight","hp":1000, "dmg":150}}'
```

Because indexing is synchronous, the document will be visible on the index as soon as the `JSON.SET` command returns.
Any subsequent query that matches the indexed content will return the document.

## Search the index

To search for documents, use the `FT.SEARCH` command.
You can search any attribute defined in the SCHEMA.

Following our example, find the user called `John`:

```sql
FT.SEARCH userIdx '@name:(John)'
1) (integer) 1
2) "myDoc"
3) 1) "$"
   2) "{\"user\":{\"name\":\"John Smith\",\"tag\":\"apprentice,knight\",\"hp\":1000,\"dmg\":150}}"
```

Note: `FT.SEARCH` and  `FT.AGGREGATE` queries require `attribute` modifiers. Don't use JSONPath expressions in queries because the query parser doesn't fully support them.

## Index JSON arrays

It is possible to index scalar string and boolean values in JSON arrays by using the wildcard operator in the JSON Path. For example if you were indexing blog posts you might have a field called `tags` which is an array of tags that apply to the blog post.

```JSON
{
   "title":"Using RedisJson is Easy and Fun",
   "tags":["redis","json","redisjson"]
}
```

You can apply an index to the `tags` field by specifying the JSON Path `$.tags.*` in your schema creation:

```sql
FT.CREATE blog-idx ON JSON PREFIX 1 Blog: SCHEMA $.tags.* AS tags TAG
```

You would then set a blog post as you would any other JSON document:

```sql
JSON.SET Blog:1 . '{"title":"Using RedisJson is Easy and Fun", "tags":["redis","json","redisjson"]}'
```

And finally you can search using the typical tag searching syntax:

```sql
127.0.0.1:6379> FT.SEARCH blog-idx "@tags:{redis}"
1) (integer) 1
2) "Blog:1"
3) 1) "$"
   2) "{\"title\":\"Using RedisJson is Easy and Fun\",\"tags\":[\"redis\",\"json\",\"redisjson\"]}"
```

## Field projection

`FT.SEARCH` returns the whole document by default. If you want to limit the returned search results to a specific attribute, you can use field projection.

### Project with attribute name

This example only returns the `name` attribute:

```sql
FT.SEARCH userIdx '@name:(John)' RETURN 1 name
1) (integer) 1
2) "myDoc"
3) 1) "name"
   2) "\"John Smith\""
```

### Project with JSONPath

The `RETURN` parameter also accepts a JSONPath expression which lets you extract any part of the JSON document.

The following example returns the result of the JSONPath expression `$.user.hp`.

```sql
FT.SEARCH userIdx '@name:(John)' RETURN 1 $.user.hp
1) (integer) 1
2) "myDoc"
3) 1) "$.user.hp"
   2) "1000"
```

Note that the property name is the JSON expression itself: `3) 1) "$.user.hp"`

Using the `AS` option, you can also alias the returned property.

```sql
FT.SEARCH userIdx '@name:(John)' RETURN 3 $.user.hp AS hitpoints
1) (integer) 1
2) "myDoc"
3) 1) "hitpoints"
   2) "1000"
```

### Highlighting

You can highlight any attribute as soon as it is indexed using the TEXT type.

For `FT.SEARCH`, you have to explicitly set the attribute in the `RETURN` and the `HIGHLIGHT` parameters.

```sql
FT.SEARCH userIdx '@name:(John)' RETURN 1 name HIGHLIGHT FIELDS 1 name TAGS '<b>' '</b>'
1) (integer) 1
2) "myDoc"
3) 1) "name"
   2) "\"<b>John</b> Smith\""
```

## Aggregate with JSONPath

You can use [aggregation](/docs/stack/search/reference/aggregations) to generate statistics or build facet queries.

The `LOAD` parameter accepts JSONPath expressions. You can use any value in the pipeline, even if the value is not indexed.

This example loads two numeric values from the JSON document and applies a simple operation:

```sql
FT.AGGREGATE userIdx '*' LOAD 6 $.user.hp AS hp $.user.dmg AS dmg APPLY '@hp-@dmg' AS points
1) (integer) 1
2) 1) "hp"
   2) "1000"
   3) "dmg"
   4) "150"
   5) "points"
   6) "850"
```

## Index limitations

### JSON scalars

You can only index:

- JSON strings as TEXT, TAG, or GEO (using the correct syntax).
- JSON numbers as NUMERIC.
- JSON booleans as TAG.
- NULL values are ignored.

### JSON arrays

You can only index a JSON array of strings or booleans in a TAG field. Other types (numeric, geo, null) are not supported.

### JSON objects

You cannot index JSON objects. If the JSONPath expression returns an object, it will be ignored.

However it is possible to index the strings in separated attributes.

Given the following JSON document:

```JSON
{
  "name": "Headquarters",
  "address": [
    "Suite 250",
    "Mountain View"
  ],
  "cp": "CA 94040"
}
```

Before you can index the array under the `address` key, you have to create two fields:

```SQL
FT.CREATE orgIdx ON JSON SCHEMA $.address[0] AS a1 TEXT $.address[1] AS a2 TEXT
OK
```

Create and index the document:

```SQL
JSON.SET org:1 $ '{"name": "Headquarters","address": ["Suite 250","Mountain View"],"cp": "CA 94040"}'
OK
```

Search in the address:

```SQL
FT.SEARCH orgIdx "suite 250"
1) (integer) 1
2) "org:1"
3) 1) "$"
   2) "{\"name\":\"Headquarters\",\"address\":[\"Suite 250\",\"Mountain View\"],\"cp\":\"CA 94040\"}"
```

### TAG not sortable

If you create an index for JSON documents, you cannot sort on a `TAG` field:

```SQL
FT.CREATE orgIdx ON JSON SCHEMA $.cp[0] AS cp TAG SORTABLE
(error) On JSON, cannot set tag field to sortable - cp
```

With hashes, you can use `SORTABLE` (as a side effect) to improve the performance of `FT.AGGREGATE` on `TAG`fields.
This is possible because the value in the hash is a string, such as "apprentice,knight".

However with JSON, you can index an array of strings.
Because there is no valid single textual representation of those values,
RediSearch doesn't know how to sort the result.
