---
title: "Tags"
linkTitle: "Tags"
weight: 6
description: Details about tag fields
aliases: 
    - /docs/stack/search/reference/tags/
    - /redisearch/reference/tags
---

# Tag fields

Tag fields are similar to full-text fields but use simpler tokenization and encoding in the index. The values in these fields cannot be accessed by general field-less search and can be used only with a special syntax.

The main differences between tag and full-text fields are:

1. Stemming is not performed on tag indexes.

2. The tokenization is simpler: the user can determine a separator (defaults to a comma) for multiple tags. Whitespace trimming is only done at the end of tags. Thus, tags can contain spaces, punctuation marks, accents, etc.

3. The only two transformations performed are lower-casing (for latin languages only as of now) and whitespace trimming. Lower-case transformation can be disabled by passing `CASESENSITIVE`.

4. Tags cannot be found from a general full-text search. If a document has a field called "tags" with the values "foo" and "bar", searching for foo or bar without a special tag modifier (see below) will not return this document.

5. The index is much simpler and more compressed: frequencies or offset vectors of field flags are not stored. The index contains only document IDs encoded as deltas. This means that an entry in a tag index is usually one or two bytes long. This makes them very memory efficient and fast.

6. It's possible to create up to 1024 tag fields per index.

## Creating a tag field

Tag fields can be added to the schema with the following syntax:

```
FT.CREATE ... SCHEMA ... {field_name} TAG [SEPARATOR {sep}] [CASESENSITIVE]
```

SEPARATOR defaults to a comma (`,`), and can be any printable ASCII character. For example:

CASESENSITIVE can be specified to keep the original case.

```
FT.CREATE idx ON HASH PREFIX 1 test: SCHEMA tags TAG SEPARATOR ";"
```

## Querying tag fields

As mentioned above, just searching for a tag without any modifiers will not retrieve documents
containing it.

The syntax for matching tags in a query is as follows (the curly braces are part of the syntax):

 ```
    @<field_name>:{ <tag> | <tag> | ...}
 ```

For example, this query finds documents with either the tag `hello world` or `foo bar`:

```
    FT.SEARCH idx "@tags:{ hello world | foo bar }"
```

Tag clauses can be combined into any sub-clause, used as negative expressions, optional expressions, etc. For example, given the following index:

```
FT.CREATE idx ON HASH PREFIX 1 test: SCHEMA title TEXT price NUMERIC tags TAG SEPARATOR ";"
```

You can combine a full-text search on the title field, a numerical range on price, and match either the `foo bar` or `hello world` tag like this:

```
FT.SEARCH idx "@title:hello @price:[0 100] @tags:{ foo bar | hello world }
```

Tags support prefix matching with the regular `*` character:

```
FT.SEARCH idx "@tags:{ hell* }"
FT.SEARCH idx "@tags:{ hello\\ w* }"

```

## Multiple tags in a single filter

Notice that including multiple tags in the same clause creates a union of all documents that contain any of the included tags. To create an intersection of documents containing all of the given tags, you should repeat the tag filter several times.

For example, imagine an index of travelers, with a tag field for the cities each traveler has visited:

```
FT.CREATE myIndex ON HASH PREFIX 1 traveler: SCHEMA name TEXT cities TAG

HSET traveler:1 name "John Doe" cities "New York, Barcelona, San Francisco"
```

For this index, the following query will return all the people who visited at least one of the following cities:

```
FT.SEARCH myIndex "@cities:{ New York | Los Angeles | Barcelona }"
```

But the next query will return all people who have visited all three cities:

```
FT.SEARCH myIndex "@cities:{ New York } @cities:{Los Angeles} @cities:{ Barcelona }"
```

## Including punctuation in tags

A tag can include punctuation other than the field's separator. You do not need to escape punctuation when using the `HSET` command to add the value to a Redis Hash.

For example, given the following index:

```
FT.CREATE punctuation ON HASH PREFIX 1 test: SCHEMA tags TAG
```

You can add tags that contain punctuation like this:

```
HSET test:1 tags "Andrew's Top 5,Justin's Top 5"
```

However, when you query for tags that contain punctuation, you must escape that punctuation with a backslash character (`\`). This is the case with most languages and also redis-cli.

For example, querying for the tag `Andrew's Top 5` in the redis-cli looks like this:

```
FT.SEARCH punctuation "@tags:{ Andrew\\'s Top 5 }"
```

## Tags that contain multiple words

As the examples in this document show, a single tag can include multiple words. It is recommended that you escape spaces when querying, though doing so is not required.

You escape spaces the same way that you escape punctuation: by preceding the space with a backslash character (or two backslashes, depending on the programming language and environment).

Thus, you would escape the tag "to be or not to be" like so when querying in the redis-cli:

```
FT.SEARCH idx "@tags:{ to\\ be\\ or\\ not\\ to\\ be }"
```

You should escape spaces because if a tag includes multiple words and some of them are stop words like "to" or "be," a query that includes these words without escaping spaces will create a syntax error.

You can see what that looks like in the following example:

```
127.0.0.1:6379> FT.SEARCH idx "@tags:{ to be or not to be }"
(error) Syntax error at offset 27 near be
```

Note: stop words are words that are so common that a search engine ignores them. To learn more, see [stop words](/docs/interact/search-and-query/advanced-concepts/stopwords/).

Given the potential for syntax errors,it is recommended that you escape all spaces within tag queries.
