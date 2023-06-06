---
title: "Schema definition"
linkTitle: "Schema definition"
weight: 1
description: >
    How to define the schema of an index.
---

An index structure is defined by the schema. The schema specifies the fields, their types, whether they should be indexed or stored, and any additional configuration options. By properly configuring the schema, you can optimize search performance and control the storage requirements of your index.

```
FT.CREATE idx 
    ON HASH 
    PREFIX 1 blog:post: 
SCHEMA 
    title TEXT WEIGHT 5.0
    content TEXT
    author TAG
    created_date NUMERIC SORTABLE
    views NUMERIC
```

In this example, we define the schema for an index named `idx` that will index all hash documents whose keyname starts with `blog:post:`. 
The schema includes the fields `title`, `content`, `author`, `created_date`, and `views`. The `TEXT` type indicates that `title` and `content` fields are text-based, `TAG` type is used for the `author` field, and `NUMERIC` type is used for `created_date` and `views` fields. Additionally, we assign a weight of 5.0 to the `title` field to give it more relevance in search results, and we mark `created_date` as `SORTABLE` to enable sorting based on this field.

You can learn more about the available field types and options on the [FT.CREATE](/commands/ft.create/) page.

## More schema definition examples

##### Index tags with a separator
Index books that have a "categories" attribute where each category is separated by a `;` character.

```
FT.CREATE books-idx 
    ON HASH 
    PREFIX 1 book:details 
SCHEMA 
    title TEXT 
    categories TAG SEPARATOR ";"
```

##### Index a single field in multiple ways

Index the `sku` attribute from a hash as both a `TAG` and as `TEXT`:

```
FT.CREATE idx 
    ON HASH 
    PREFIX 1 blog:post: 
SCHEMA 
    sku AS sku_text TEXT 
    sku AS sku_tag TAG SORTABLE
```

##### Index documents with multiple prefixes

Index two different hashes, one containing author data and one containing books, in the same index:

```
FT.CREATE author-books-idx 
    ON HASH 
    PREFIX 2 author:details: book:details: 
SCHEMA
    author_id TAG SORTABLE 
    author_ids TAG 
    title TEXT name TEXT
```

In this example, keys for author data use the key pattern `author:details:<id>` while keys for book data use the pattern `book:details:<id>`.

##### Only index documents if a field specifies a certain value (`FILTER`)

Index authors whose names start with G.

```
FT.CREATE g-authors-idx 
    ON HASH 
    PREFIX 1 author:details 
    FILTER 'startswith(@name, "G")' 
SCHEMA 
    name TEXT
```

Index only books that have a subtitle.

```
FT.CREATE subtitled-books-idx
    ON HASH 
    PREFIX 1 book:details 
    FILTER '@subtitle != ""' 
SCHEMA 
    title TEXT
```



##### Index a JSON document using a JSON Path expression

Index a JSON document that has a `title` field and a `categories` field. The `title` field is indexed as `TEXT` and the `categories` field is indexed as `TAG`.

```
FT.CREATE idx 
    ON JSON 
SCHEMA 
    $.title AS title TEXT 
    $.categories AS categories TAG
```


You can learn more about the available field types and options on the [FT.CREATE](/commands/ft.create/) page.