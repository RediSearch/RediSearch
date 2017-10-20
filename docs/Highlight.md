# Highlighting API

## Entry Points

It would be good to support multiple highlighter engines, but also allow for
sane defaults. As such, two entry points will be available. One is a simplified
syntax, and the other is a more complicated one.

### Command Syntax

```
FT.SEARCH ...
    SUMMARIZE [FIELDS {num} {field}] [FRAGS {numFrags}] [LEN {fragLen}]
    HIGHLIGHT [FIELDS {num} {field}] [TAGS {openTag} {closeTag}]

```

There are two sub-commands commands used for highlighting. One is `HIGHLIGHT`
which surrounds matching text with an open and/or close tag; and the other is
`SUMMARIZE` which splits a field into contextual fragments surrounding the
found terms. It is possible to summarize a field, highlight a field, or perform
both actions in the same query.

#### Summarization


```
FT.SEARCH ... SUMMARIZE [FIELDS {num} {field}] [FRAGS {numFrags}] [LEN {fragLen}]
```

Summarization or snippetization will fragment the text into smaller sized
snippets; each snippet will contain the found term(s) and some additional
surrounding context.

Redis Search can perform summarization using the `SUMMARIZE` keyword. If no
additional arguments are passed, all _returned fields_ are summarized using
built-in defaults.

The `SUMMARIZE` keyword accepts the following arguments:

* **`FIELDS`** If present, must be the first argument. This should be followed
    by the number of fields to summarize, which itself is followed by a list of
    fields. Each field present is summarized. If no `FIELD` directive is passed,
    then *all* fields returned are summarized.
* **`FRAGS`** How many fragments should be returned. If not specified, a sane
    default is used.
* **`LEN`** The number of context words each fragment should contain. Context
    words surround the found term. A higher value will return a larger block of
    text.



#### Highlighting

```
FT.SEARCH ... HIGHLIGHT [FIELDS {num} {field}] [TAGS {openTag} {closeTag}]
```

Highlighting will highlight the found term (and its variants) with a user-defined
tag. This may be used to display the matched text in a different typeface using
a markup language, or to otherwise make the text appear differently.

Redis Search can perform highlighting using the `HIGHLIGHT` keyword. If no
additional arguments are passed, all _returned fields_ are highlighted using
build-in defaults.

The `HIGHLIGHT` keyword accepts the following arguments:

* **`FIELDS`** If present, must be the first argument. This should be followed
    by the number of fields to highlight, which itself is followed by a list of
    fields. Each field present is highlighted. If no `FIELD` directive is passed,
    then *all* fields returned are highlighted.
* **`TAGS`** If present, must be followed by two strings; the first is prepended
    to each term match, and the second is appended to it. If no `TAGS` are
    specified, a built-in tag value is appended and prepended.


#### Field Selection

If no specific fields are passed to the `RETURN`, `SUMMARIZE`, or `HIGHLIGHT`
keywords, then all of a document's fields are returned. However, if any of these
keywords contain a `FIELD` directive, then the `SEARCH` command will only retun
the sum total of all fields enumerated in any of those directives. For example
in the command `RETURN 1 foo SUMMARIZE FIELDS 1 bar HIGHLIGHT FIELDS 1 baz`,
the fields `foo` is returned as-is, `bar` is returned summarized and `baz` is
returned highlighted. If a keyword (e.g. `SUMMARIZE` or `HIGHLIGHT`) is
passed without any additional fields, then it affects all fields returned; thus
`RETURN 2 foo bar HIGHLIGHT FIELDS 1 baz SUMMARIZE` will return `foo` and `bar`
summarized, and `baz` summarized and highlighted.