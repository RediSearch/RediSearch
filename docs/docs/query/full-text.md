---
title: "Full-text search"
linkTitle: "Full-text"
description: Perform a full-text search
weight: 3
---

A full-text search finds words or phrases within larger texts. You can search within a specific text field or across all text fields. 

This article provides you a good overview about the most relevant full-text search capabilites. Please find further details about all the full-text search features in the [reference documentation](/docs/interact/search-and-query/advanced-concepts/).

The examples in this article use a schema with the following fields:

| Field name | Field type |
| ---------- | ---------- |
| brand      | TEXT       |
| model      | TEXT       |
| description| TEXT       |


## Word search

To search for a word, or a word that has the same stem, across all text fields, you can construct the following simple query:

```
FT.SEARCH index "word"
```

The full-text search can be limited to as specific field.

```
FT.SEARCH index "@field: word"
```

Words that occur very often, such as aticles, aren't indexexed and will not return a search result. You can find further details in the [stop words article](/docs/interact/search-and-query/advanced-concepts/stopwords).

The following example searches for all bicycles that have the word 'kids' in the description:

```
FT.SEARCH idx:bicycle "@description: kids"
```


## Prefix search

You can also search for words that have the same prefix.

```
FT.SEARCH index "prefix*"
```

```
FT.SEARCH index "@field: prefix*"
```

{{% alert title="Important" color="warning" %}}
The prefix needs to be at least two characters long.
{{% /alert  %}}


Here is an example that shows you how to search for bikes with a brand that starts with 'ka':

```
FT.SEARCH idx:bicycle "@model: ka*"
```

## Suffix search

Similar to the prefix, it is also possible to search for words that share the same suffix.

```
FT.SEARCH index "*suffix"
```

Here is an example that finds all brands that end with 'bikes':

```
FT.SEARCH idx:bicycle "@brand:*bikes"
```

## Fuzzy search

Fuzzy search allows you to find words that match your search term approximately.

You can wrap a word by using the `%` operator, whereby the operator can occur up to three times and controls the maximum allowed distance. Here is the command with a distance of one:

```
FT.SEARCH index "%word%"
```

The following example finds all documents that contain a word that has a distance of one to the wrongly spelled word 'optamized'. You can see that this matches on 'bikes'.

```
FT.SEARCH idx:bicycle "%optamized%"
```

If you want to increase the maximum word distance to two, you can use the following query:

```
FT.SEARCH idx:bicycle "%%optamised%%"
```

## Phrase search

A phrase is a sentence, sentence fragment, or small group of words. You can find further details about how to find exact phrases in the [exact match](/docs/interact/search-and-query/query/exact-match) article.



