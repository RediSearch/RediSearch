# Highlighting API

The highlighting API allows you to have only the relevant portions of document matching a search query returned as a result. This allows users to quickly see how a document relates to their query, with the search terms highlighted, usually in bold letters.

RediSearch implements high performance highlighting and summarization algorithms, with the following API: 

## Command syntax

```
FT.SEARCH ...
    SUMMARIZE [FIELDS {num} {field}] [FRAGS {numFrags}] [LEN {fragLen}] [SEPARATOR {sepstr}]
    HIGHLIGHT [FIELDS {num} {field}] [TAGS {openTag} {closeTag}]

```

There are two sub-commands commands used for highlighting. One is `HIGHLIGHT` which surrounds matching text with an open and/or close tag, and the other is `SUMMARIZE` which splits a field into contextual fragments surrounding the found terms. It is possible to summarize a field, highlight a field, or perform both actions in the same query.

### Summarization

```
FT.SEARCH ...
    SUMMARIZE [FIELDS {num} {field}] [FRAGS {numFrags}] [LEN {fragLen}] [SEPARATOR {sepStr}]
```

Summarization  will fragment the text into smaller sized snippets; each snippet will contain the found term(s) and some additional surrounding context.

RediSearch can perform summarization using the `SUMMARIZE` keyword. If no additional arguments are passed, all _returned fields_ are summarized using built-in defaults.

The `SUMMARIZE` keyword accepts the following arguments:

* **`FIELDS`**: If present, must be the first argument. This should be followed
    by the number of fields to summarize, which itself is followed by a list of
    fields. Each field present is summarized. If no `FIELDS` directive is passed,
    then *all* fields returned are summarized.

* **`FRAGS`**: How many fragments should be returned. If not specified, a default of 3 is used.

* **`LEN`** The number of context words each fragment should contain. Context
    words surround the found term. A higher value will return a larger block of
    text. If not specified, the default value is 20.

* **`SEPARATOR`** The string used to divide between individual summary snippets.
    The default is `... ` which is common among search engines; but you may
    override this with any other string if you desire to programmatically divide them
    later on. You may use a newline sequence, as newlines are stripped from the
    result body anyway (thus, it will not be conflated with an embedded newline
    in the text)


### Highlighting

```
FT.SEARCH ... HIGHLIGHT [FIELDS {num} {field}] [TAGS {openTag} {closeTag}]
```

Highlighting will highlight the found term (and its variants) with a user-defined tag. This may be used to display the matched text in a different typeface using a markup language, or to otherwise make the text appear differently.

RediSearch can perform highlighting using the `HIGHLIGHT` keyword. If no additional arguments are passed, all _returned fields_ are highlighted using built-in defaults.

The `HIGHLIGHT` keyword accepts the following arguments:

* **`FIELDS`** If present, must be the first argument. This should be followed
    by the number of fields to highlight, which itself is followed by a list of
    fields. Each field present is highlighted. If no `FIELDS` directive is passed,
    then *all* fields returned are highlighted.
    
* **`TAGS`** If present, must be followed by two strings; the first is prepended
    to each term match, and the second is appended to it. If no `TAGS` are
    specified, a built-in tag value is appended and prepended.


#### Field selection

If no specific fields are passed to the `RETURN`, `SUMMARIZE`, or `HIGHLIGHT` keywords, then all of a document's fields are returned. However, if any of these keywords contain a `FIELD` directive, then the `SEARCH` command will only return the sum total of all fields enumerated in any of those directives.

The `RETURN` keyword is treated specially, as it overrides any fields specified in `SUMMARIZE` or `HIGHLIGHT`.

In the command `RETURN 1 foo SUMMARIZE FIELDS 1 bar HIGHLIGHT FIELDS 1 baz`, the fields `foo` is returned as-is, while `bar` and `baz` are not returned, because `RETURN` was specified, but did not include those fields.

In the command `SUMMARIZE FIELDS 1 bar HIGHLIGHT FIELDS 1 baz`, `bar` is returned summarized and `baz` is returned highlighted.
