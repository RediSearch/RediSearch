---
title: "Indexing JSON documents"
linkTitle: "Indexing JSON"
weight: 8
description: >
    Indexing and searching JSON documents
---

In addition to indexing Redis hashes, RediSearch also indexes JSON. To index JSON, you must use the RedisJSON module.

## Prerequisites

What do you need to start indexing JSON documents?

- Redis 6.x or later
- RediSearch 2.2 or later
- RedisJSON 2.0 or later

## How to index JSON documents

This section shows how to create an index.

You can now specify `ON JSON` to inform RediSearch that you want to index JSON documents.

For the `SCHEMA`, you can provide JSONPath expressions.
The result of each `JSON Path` expression is indexed and associated with a logical name (`attribute`).
This attribute (previously called `field`) is used in the query.

This is the basic syntax to index a JSON document:

    FT.CREATE {index_name} ON JSON SCHEMA {json_path} AS {attribute} {type}

And here's a concrete example:

    FT.CREATE userIdx ON JSON SCHEMA $.user.name AS name TEXT $.user.tag AS country TAG

## Adding a JSON document to the index

As soon as the index is created, any pre-existing JSON document, or any new JSON document added or modified, is
automatically indexed.

You can use any write command from the RedisJSON module (`JSON.SET`, `JSON.ARRAPPEND`, etc.).

This example uses the following JSON document:

```JSON
{
  "user": {
    "name": "John Smith",
    "tag": "foo,bar",
    "hp": "1000",
    "dmg": "150"
  }
}
```

Use `JSON.SET` to store the document in the database:

    JSON.SET myDoc $ '{"user":{"name":"John Smith","tag":"foo,bar","hp":1000, "dmg":150}}'

Because indexing is synchronous, the document will be visible on the index as soon as the `JSON.SET` command returns.
Any subsequent query matching the indexed content will return the document.

## Searching

To search for documents, use the `FT.SEARCH` commands.
You can search any attribute mentioned in the SCHEMA.

Following our example, find the user called `John`:

```
FT.SEARCH userIdx '@name:(John)'
1) (integer) 1
2) "myDoc"
3) 1) "$"
   2) "{\"user\":{\"name\":\"John Smith\",\"tag\":\"foo,bar\",\"hp\":1000,\"dmg\":150}}"
```

## Field projection

`FT.SEARCH` returns the whole document by default.

You can also return only a specific attribute (`name` for example):

```
FT.SEARCH userIdx '@name:(John)' RETURN 1 name
1) (integer) 1
2) "myDoc"
3) 1) "name"
   2) "\"John Smith\""
```

### Projecting using JSON Path expressions

The `RETURN` parameter also accepts a `JSON Path expression` which lets you extract any part of the JSON document.

The following example returns the result of the JSON Path expression `$.user.hp`.

```
FT.SEARCH userIdx '@name:(John)' RETURN 1 $.user.hp
1) (integer) 1
2) "myDoc"
3) 1) "$.user.hp"
   2) "1000"
```

Note that the property name is the JSON expression itself: `3) 1) "$.user.hp"`

Using the `AS` option, you can also alias the returned property.

```
FT.SEARCH userIdx '@name:(John)' RETURN 3 $.user.hp AS hitpoints
1) (integer) 1
2) "myDoc"
3) 1) "hitpoints"
   2) "1000"
```

### Highlighting

You can highlight any attribute as soon as it is indexed using the TEXT type.
For FT.SEARCH, you have to explicitly set the attribute in the `RETURN` and the `HIGHLIGHT` parameters.

```
FT.SEARCH userIdx '@name:(John)' RETURN 1 name HIGHLIGHT FIELDS 1 name TAGS '<b>' '</b>'
1) (integer) 1
2) "myDoc"
3) 1) "name"
   2) "\"<b>John</b> Smith\""
```

## Aggregation with JSON Path expression

[Aggregation](/redisearch/reference/aggregations) is a powerful feature. You can use it to generate statistics or build facet queries.
The LOAD parameter accepts JSON Path expressions. Any value (even not indexed) can be used in the pipeline.

This example loads two numeric values from the JSON document applying a simple operation.

```
FT.AGGREGATE userIdx '*' LOAD 6 $.user.hp AS hp $.user.dmg AS dmg APPLY '@hp-@dmg' AS points
1) (integer) 1
2) 1) "point"
   2) "850"
```

### Indexing Arrays of Objects and Scalars

Any JSON path that resolves to an array of strings or booleans is indexable as a TAG field.
This means that we can index any JSON array of strings or booleans, as well as any objects
we can derive a JSON path from to resolve to an array of strings or booleans.

Take the following example, which has an array of string (`phone_numbers`), and an array of objects `addresses`:

```json
{
   "name":"Bob Fry",
   "phone_numbers":
   [
      "555-555-5555",
      "123-456-7890"
   ],
   "addresses":
   [
      {
         "street_address":"123 Main Street",
         "city":"Orlando",
         "state":"Florida",
         "postal_code":"32789"
      },
      {
         "street_address":"234 Drury Lane",
         "city":"Atlanta",
         "state":"Georgia",
         "postal_code":"30301"
      }
   ]
}
```

You can index any JSON path that will resolve to an array. For example, `$.phone_numbers.*` resolves to the array `['555-555-5555', 123-456-7890]` so it can be indexed. Also, the string fields WITHIN the objects of the `addresses` field can also be indexed. For example, the JSON path `$.addresses[*].city` resolves to an array of strings `[Orlando, Atlanta]`. Consequentially, you can create a corresponding index to index both of these fields:

```
FT.CREATE idx ON JSON SCHEMA $.phone_numbers.* AS numbers TAG $.addresses[*].city AS cities TAG
```

You can then query those documents with the typical `TAG` syntax e.g.

```
FT.SEARCH idx "@cities:{Orlando}"
```

## Current indexing limitations

### Multiple-values cannot be indexed for non-TAG fields

JSON paths that resolve to multiple values are only supported for TAG fields, `TEXT`, `NUMERIC`,
and `GEO` are not supported. The JSON paths for these non-TAG fields must resolve to a single scalar value.
If they resolve to anything but a single scalar value, the index will fail.

### It is not possible to index whole JSON objects.

To be indexed, a JSONPath expression must return a single scalar value (e.g. a string or number),
with the exception of `TAG` fields, which can index arrays of scalar strings and booleans.

If the JSONPath expression returns an object, it will be ignored.

However it is possible to index the scalars within JSON objects and arrays.

Given the following document:

```JSON
{
  "name": "Headquarters",
  "address": {
    "unit": "Suite 250",
    "city":"Mountain View",
    "state":"CA",
    "postal_code":"94040"
   },
   "phone_numbers":[
      "555-555-5555",
      "123-456-7890"
   ]
}
```

The path `$.address` returns the whole object for address, so this cannot be indexed. However, you can index the components of `address` with a JSON path resolving to the scalars within it e.g. `$.address.unit`. Additionally if you wanted to create a rule where you'd consider the first element of the `phone_numbers` the primary phone number, you could use the path $.phone_numbers[0] to resolve to that item:

```SQL
FT.CREATE orgIdx ON JSON SCHEMA $.address.unit AS unit TEXT $.phone_numbers[0] AS primary_phone TAG
OK
```

You can now index the document:

```SQL
JSON.SET org:1 $ '{"name": "Headquarters","address": {"unit": "Suite 250", "city":"Mountain View", "state":"CA", "postal_code":"94040"}, "phone_numbers":["555-555-5555","123-456-7890"]}'
OK
```

You can now search in the address:

```SQL
FT.SEARCH orgIdx "suite 250"
1) (integer) 1
2) "org:1"
3) 1) "$"
   2) "{\"name\":\"Headquarters\",\"address\":{\"unit\":\"Suite 250\",\"city\":\"Mountain View\",\"state\":\"CA\",\"postal_code\":\"94040\"},\"phone_numbers\":[\"555-555-5555\",\"123-456-7890\"]}"
```

Or for the `primary_phone`:

```SQL
FT.SEARCH orgIdx "@primary_phone:{555\\-555\\-5555}"
1) (integer) 1
2) "org:1"
3) 1) "$"
   2) "{\"name\":\"Headquarters\",\"address\":{\"unit\":\"Suite 250\",\"city\":\"Mountain View\",\"state\":\"CA\",\"postal_code\":\"94040\"},\"phone_numbers\":[\"555-555-5555\",\"123-456-7890\"]}"
```

### Index JSON strings and numbers as TEXT and NUMERIC

- You can only index JSON strings as TEXT, TAG, or GEO (using the right syntax).
- You can only index JSON numbers as NUMERIC.
- Boolean and NULL values are ignored.

### SORTABLE not supported on TAG

```SQL
FT.CREATE orgIdx ON JSON SCHEMA $.cp[0] AS cp TAG SORTABLE
(error) On JSON, cannot set tag field to sortable - cp
```

With hashes, you can use SORTABLE (as a side effect) to improve the performance of FT.AGGREGATE on TAGs.
This is possible because the value in the hash is a string, such as "foo,bar".

With JSON, you can index an array of strings.
Because there is no valid single textual representation of those values,
there is no way for RediSearch to know how to sort the result.
