---
title: "Full-text search"
linkTitle: "Full-text"
description: Perform a full-text search
weight: 3
---

A full-text search finds words or phrases within larger texts. You can search within a specific text field or across all text fields. 

This article provides a good overview of the most relevant full-text search capabilities. Please find further details about all the full-text search features in the [reference documentation](/docs/interact/search-and-query/advanced-concepts/).

The examples in this article use a schema with the following fields:

| Field name | Field type |
| ---------- | ---------- |
| brand      | TEXT       |
| model      | TEXT       |
| description| TEXT       |


## Single word

To search for a word (or word stem) across all text fields, you can construct the following simple query:

```
FT.SEARCH index "word"
```

Instead of searching across all text fields, you might want to limit the search to a specific text field.

```
FT.SEARCH index "@field: word"
```

Words that occur very often in natural language, such as `the` or `a` for the English language, aren't indexed and will not return a search result. You can find further details in the [stop words article](/docs/interact/search-and-query/advanced-concepts/stopwords).

The following example searches for all bicycles that have the word 'kids' in the description:

```
FT.SEARCH idx:bicycle "@description: kids"
```

## Phrase

A phrase is a sentence, sentence fragment, or small group of words. You can find further details about how to find exact phrases in the [exact match article](/docs/interact/search-and-query/query/exact-match).


## Word prefix

You can also search for words that match a given prefix.

```
FT.SEARCH index "prefix*"
```

```
FT.SEARCH index "@field: prefix*"
```

{{% alert title="Important" color="warning" %}}
The prefix needs to be at least two characters long.
{{% /alert  %}}

Here is an example that shows you how to search for bicycles with a brand that starts with 'ka':

```
FT.SEARCH idx:bicycle "@model: ka*"
```

## Word suffix

Similar to the prefix, it is also possible to search for words that share the same suffix.

```
FT.SEARCH index "*suffix"
```

You can also combine prefix- and suffix-based searches within a query expression.

```
FT.SEARCH index "*infix*"
```

Here is an example that finds all brands that end with 'bikes':

```
FT.SEARCH idx:bicycle "@brand:*bikes"
```

## Fuzzy search

A fuzzy search allows you to find documents with words that match your search term approximately.

You can wrap a word by using the `%` operator, whereby the operator can occur up to three times, and it controls the maximum allowed distance. 

Here is the command that searches across all text fields with a distance of one:

```
FT.SEARCH index "%word%"
```

The following example finds all documents that contain a word that has a distance of one to the wrongly spelled word 'optamized'. You can see that this matches the word 'optimized'.

```
FT.SEARCH idx:bicycle "%optamized%"
```

If you want to increase the maximum word distance to two, you can use the following query:

```
FT.SEARCH idx:bicycle "%%optamised%%"
```