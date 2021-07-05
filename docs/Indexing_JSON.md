# Indexing JSON documents

In addition to indexing Redis hashes, RediSearch also indexes JSON. To index JSON, you must use the RedisJSON module.

## Prerequisite

What do you need to start indexing JSON documents?

- Redis 6.x or later
- RediSearch 2.2 or later
- RediJSON 2.0 or later

## How to index JSON documents

Let's start by creating an index.

We can now specify `ON JSON` to inform RediSearch that we want to index JSON documents.

Then, on the `SCHEMA` part, you can provide JSONPath expressions.
The result of each `JSON Path` expression is indexed and associated with a logical name (`attribute`).
This attribute (previously called `field`) is used in the query part.

This is the basic syntax for indexing a JSON document:

    FT.CREATE {index_name} ON JSON SCHEMA {json_path} AS {attribute} {type}

And here's a concrete example:

    FT.CREATE userIdx ON JSON SCHEMA $.user.name AS name TEXT $.user.tag AS country TAG

## Adding JSON document to the index

As soon as the index is created, any pre-existing JSON document, or any new JSON document added or modified is
automatically indexed.

We are free to use any writing command from the RedisJSON module (`JSON.SET`, `JSON.ARRAPPEND`, etc.).

In our example we are going to use the following JSON document:

```JSON
{
  "user": {
    "name": "John Smith",
    "tag": [
      "foo",
      "bar"
    ],
    "hp": "1000",
    "dmg": "150"
  }
}
```

We can use `JSON.SET` to store in our database:

    JSON.SET myDoc $ '{"user":{"name":"John Smith","tag":["foo","bar"],"hp":1000, "dmg":150}}'

Because indexing is synchronous, the document will be visible on the index as soon as the `JSON.SET` command returns.
Any subsequent query matching the indexed content will return the document.

## Searching

To search for documents, we use the [FT.SEARCH](Commands.md#FT.SEARCH) commands.
We can search any attribute mentioned in the SCHEMA.

Following our example, let's find our user called `John`:

```
FT.SEARCH myIdx '@name:(John)'
1) (integer) 1
2) "myDoc"
3) 1) "$"
   2) "{\"user\":{\"name\":\"John Smith\",\"tag\":[\"foo\",\"bar\"],\"hp\":1000,\"dmg\":150}}"
```

## Field projection

We just saw that, by default, `FT.SEARCH` returns the whole document.

We can also return only specific attribute (here `name`):

```
FT.SEARCH myIdx '@name:(John)' RETURN 1 name
1) (integer) 1
2) "myDoc"
3) 1) "name"
   2) "\"John Smith\""
```

### Projecting using JSON Path expressions

The `RETURN` parameter also accepts a `JSON Path expression` which let us extract any part of the JSON document.

In this example, we return the result of the JSON Path expression `$.user.hp`.

```
FT.SEARCH myIdx '@name:(John)' RETURN 1 $.user.hp
1) (integer) 1
2) "myDoc"
3) 1) "$.user.hp"
   2) "1000"
```

Note that the property name is the JSON expression itself: `3) 1) "$.user.hp"`

Using the `AS` option, you can also alias the returned property.

```
FT.SEARCH myIdx '@name:(John)' RETURN 3 $.user.hp AS hitpoints
1) (integer) 1
2) "myDoc"
3) 1) "hitpoints"
   2) "1000"
```

### Highlighting

We can highlight any attribute as soon as it is indexed using the TEXT type.
In FT.SEARCH we have to explicitly set the attribute in the `RETURN` and the `HIGHLIGHT` parameters.

```
FT.SEARCH myIdx '@name:(John)' RETURN 1 name HIGHLIGHT FIELDS 1 name TAGS '<b>' '</b>'
1) (integer) 1
2) "myDoc"
3) 1) "name"
   2) "\"<b>John</b> Smith\""
```

## Aggregation with JSON Path expression

[Aggregation](Aggregations.md) is a powerful feature. You can use it to generate statistics or build facet queries.
The LOAD parameters accepts JSON Path expressions. Any value (even not indexed) can be used in the pipeline.

In this example we are loading two numeric values from the JSON document applying a simple operation.

```
FT.AGGREGATE myIdx '*' LOAD 6 $.user.hp AS hp $.user.dmg AS dmg APPLY '@hp-@dmg' AS points
1) (integer) 1
2) 1) "point"
   2) "850"
```

## Current indexing limitations

### It is not possible to index JSON object and JSON arrays.

To be indexed, a JSONPath expression must return a single scalar value (string or number).
If the JSONPath expression returns an object or an array, it will be ignored.

Given the following document:

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

If we want to index the array under the `address` key, we have to create two fields:

```SQL
FT.CREATE orgIdx ON JSON SCHEMA $.address[0] AS a1 TEXT $.address[1] AS a2 TEXT
OK
```

We can now index the document:

```SQL
JSON.SET org:1 $ '{"name": "Headquarters","address": ["Suite 250","Mountain View"],"cp": "CA 94040"}'
OK
```

We can now search in the address:

```SQL
FT.SEARCH orgIdx "suite 250"
1) (integer) 1
2) "org:1"
3) 1) "$"
   2) "{\"name\":\"Headquarters\",\"address\":[\"Suite 250\",\"Mountain View\"],\"cp\":\"CA 94040\"}"
```

### JSON strings and numbers as to be indexed as TEXT and NUMERIC

- JSON Strings can only be indexed as TEXT, TAG and GEO (using the right syntax).
- JSON numbers can only be indexes as NUMERIC.
- Boolean and NULL values are ignored.
