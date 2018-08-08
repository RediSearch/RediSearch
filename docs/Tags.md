# Tag Fields 

RediSearch 0.91 adds a new kind of field - the Tag field. They are similar to full-text fields but use simpler tokenization and encoding in the index. The values in these fields cannot be accessed by general field-less search and can be used only with a special syntax.

The main differences between tag and full-text fields are:

1. An entire tag field index resides in a single Redis key and doesn't have a key per term as the full-text one.

2. We do not perform stemming on tag indexes.

3. The tokenization is simpler: The user can determine a separator (defaults to a comma) for multiple tags, and we only do whitespace trimming at the end of tags. Thus, tags can contain spaces, punctuation marks, accents, etc. The only two transformations we perform are lower-casing (for latin languages only as of now), and whitespace trimming.

4. Tags cannot be found from a general full-text search. If a document has a field called "tags" with the values "foo" and "bar", searching for foo or bar without a special tag modifier (see below) will not return this document.

5. The index is much simpler and more compressed: We do not store frequencies, offset vectors of field flags. The index contains only document IDs encoded as deltas. This means that an entry in a tag index is usually one or two bytes long. This makes them very memory efficient and fast.

6. An unlimited number of tag fields can be created per index, as long as the overall number of fields is under 1024.
 
## Creating a tag field
 
Tag fields can be added to the schema in FT.ADD with the following syntax:

```
FT.CREATE ... SCHEMA ... {field_name} TAG [SEPARATOR {sep}]
```

SEPARATOR defaults to a comma (`,`), and can be any printable ASCII character. For example:

```
FT.CREATE idx SCHEMA tags TAG SEPARATOR ";"
```

## Querying tag fields
 
As mentioned above, just searching for a tag without any modifiers will not retrieve documents
containing it.

The syntax for matching tags in a query is as follows (the curly braces are part of the syntax in
this case):
 
 ```
    @<field_name>:{ <tag> | <tag> | ...}
 ```
 
e.g.

```
    @tags:{hello world | foo bar}
```

Tag clauses can be combined into any sub-clause, used as negative expressions, optional expressions, etc. For example:

```
FT.SEARCH idx "@title:hello @price:[0 100] @tags:{ foo bar | hello world }
```
 
## Multiple tags in a single filter

Notice that multiple tags in the same clause create a union of documents containing either tags. To create an intersection of documents containing *all* tags, you should repeat the tag filter several times.

For example, imagine an index of travellers, with a tag field for the cities each traveller has visited:

```
FT.CREATE myIndex SCHEMA name TEXT cities TAG

FT.ADD myIndex user1 1.0 FIELDS name "John Doe" cities "New York, Barcelona, San Francisco"
```

For this index, the following query will return all the people who visited **at least one** of the following cities:

```
FT.SEARCH myIndex "@cities:{ New York | Los Angeles | Barcelona }"
```

But the next query will return all people who have visited **all three cities**:

```
@cities:{ New York } @cities:{Los Angeles} @cities:{ Barcelona }
```

## Multi-word tags And escaping

Tags can be composed multiple words, or include other punctuation marks other than the field's separator (`,` by default). Punctuation marks in tags should be escaped with a backslash (`\`). *NOTE:* in most languages you will need an extra backslash when formatting the document or query, to signify an actual backslash, so the actual text in redis-cli for example, will be entered as hello\\-world.

It is also recommended (but not mandatory) to escape spaces; The reason is that if a multi-word tag includes stopwords, it will create a syntax error. So tags like "to be or not to be" should be escaped as "to\ be\ or\ not\ to\ be". For good measure, you can escape all spaces within tags.

The following are identical:

```
@tags:{foo\ bar\ baz | hello\ world}

@tags:{foo bar baz | hello world }
```
