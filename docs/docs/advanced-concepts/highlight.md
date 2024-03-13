---
title: "Highlighting"
linkTitle: "Highlighting"
weight: 7
description: Highlighting full-text results
aliases: 
    - /docs/stack/search/reference/highlight/
    - /redisearch/reference/highlight
---

# Highlighting API

Redis Stack uses advanced algorithms for highlighting and summarizing, which enable only the relevant portions of a document to appear in response to a search query. This feature allows users to immediately understand the relevance of a document to their search criteria, typically highlighting the matching terms in bold text.

## Command syntax

```
FT.SEARCH ...
    SUMMARIZE [FIELDS {num} {field}] [FRAGS {numFrags}] [LEN {fragLen}] [SEPARATOR {sepstr}]
    HIGHLIGHT [FIELDS {num} {field}] [TAGS {openTag} {closeTag}]
```

There are two sub-commands used for highlighting. The first is `HIGHLIGHT`, which surrounds matching text with an open and/or close tag. The second is `SUMMARIZE`, which splits a field into contextual fragments surrounding the found terms. It is possible to summarize a field, highlight a field, or perform both actions in the same query.

### Summarization

```
FT.SEARCH ...
    SUMMARIZE [FIELDS {num} {field}] [FRAGS {numFrags}] [LEN {fragLen}] [SEPARATOR {sepStr}]
```

Summarization will fragment the text into smaller sized snippets, each of which containing the found term(s) and some additional surrounding context.

Redis Stack can perform summarization using the `SUMMARIZE` keyword. If no additional arguments are passed, all returned fields are summarized using built-in defaults.

The `SUMMARIZE` keyword accepts the following arguments:

* **`FIELDS`**: If present, it must be the first argument. This should be followed
    by the number of fields to summarize, which itself is followed by a list of
    fields. Each field is summarized. If no `FIELDS` directive is passed,
    then all returned fields are summarized.

* **`FRAGS`**: The number of fragments to be returned. If not specified, a default is 3.

* **`LEN`**: The number of context words each fragment should contain. Context
    words surround the found term. A higher value will return a larger block of
    text. If not specified, the default value is 20.

* **`SEPARATOR`**: The string used to divide individual summary snippets.
    The default is `... ` which is common among search engines, but you may
    override this with any other string if you desire to programmatically divide the snippets
    later on. You may also use a newline sequence, as newlines are stripped from the
    result body during processing.

### Highlighting

```
FT.SEARCH ... HIGHLIGHT [FIELDS {num} {field}] [TAGS {openTag} {closeTag}]
```

Highlighting will surround the found term (and its variants) with a user-defined pair of tags. This may be used to display the matched text in a different typeface using a markup language, or to otherwise make the text appear differently.

Redis Stack performs highlighting using the `HIGHLIGHT` keyword. If no additional arguments are passed, all returned fields are highlighted using built-in defaults.

The `HIGHLIGHT` keyword accepts the following arguments:

* **`FIELDS`**: If present, it must be the first argument. This should be followed
    by the number of fields to highlight, which itself is followed by a list of
    fields. Each field present is highlighted. If no `FIELDS` directive is passed,
    then all returned fields are highlighted.
    
* **`TAGS`**: If present, it must be followed by two strings. The first string is prepended
    to each matched term. The second string is appended to each matched term. If no `TAGS` are
    specified, a built-in tag pair is prepended and appended to each matched term.


#### Field selection

If no specific fields are passed to the `RETURN`, `SUMMARIZE`, or `HIGHLIGHT` keywords, then all of a document's fields are returned. However, if any of these keywords contain a `FIELD` directive, then the `SEARCH` command will only return the sum total of all fields enumerated in any of those directives.

The `RETURN` keyword is treated specially, as it overrides any fields specified in `SUMMARIZE` or `HIGHLIGHT`.

In the command `RETURN 1 foo SUMMARIZE FIELDS 1 bar HIGHLIGHT FIELDS 1 baz`, the fields `foo` is returned as-is, while `bar` and `baz` are not returned, because `RETURN` was specified, but did not include those fields.

In the command `SUMMARIZE FIELDS 1 bar HIGHLIGHT FIELDS 1 baz`, `bar` is returned summarized and `baz` is returned highlighted.
