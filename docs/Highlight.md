# Highlighting API

## Entry Points

It would be good to support multiple highlighter engines, but also allow for
sane defaults. As such, two entry points will be available. One is a simplified
syntax, and the other is a more complicated one.

### Simple Syntax

```
FT.SEARCH ... SUMMARIZE
    [TAGS {open} {close}]
    [FRAGSIZE {size}]
    [NOTRUNCATE]
    {num} {field}
```

* **TAGS** If passed, should specify the open and close tags to surround each
    term with.
    If not specified, then no special text will wrap the terms.
* **FRAGSIZE** return (approximately) {size}-sized summaries. If the raw text is
    less than this size, or within +/- 10 chars, it is not truncated.
    If not specified, the module default will be used.
* **NOTRUNCATE** Rather than returning a synopsis of the field, return the entire
    field's body with highlighting applied.

* **num** number of fields to summarize
* **field** fields to summarize

The return value will be augmented to the fields returned via `RETURN`.
Conversely, if `RETURN` appears after this keyword, then the fields indicated
in `RETURN` follow after those from `SUMMARIZE`.

### Extended Syntax

```
FT.SEARCH ... HIGHLIGHTER {engine} {engine args}
```

This will invoke the highlighter `engine` with the given arguments. The arguments
themselves are engine-specific

### Default Engine

```
FT.SEARCH ... HIGHLIGHTER DEFAULT
    FIELD {name}
        [TAGS {open} {close}]
        [FRAGSIZE size]
        [FORMAT {ORDER|RELEVANCE|RELORDER|SYNOPSIS|FULL}]
        [FRAGLIMIT {number}]
    FIELD {name} ...
```

This format allows finer grained control over each field's summarization/highlighting.
Rather than setting a tag policy for all fields, it is set individually for each
one.

The `FORMAT` argument controls how the fragments are returned. The options are:

* **ORDER**: The fragments are returned in the order they appear in the field
* **RELEVANCE**: The fragments are returned sorted by relevance to the search terms.
* **RELORDER**: The top `FRAGLIMIT` results are returned, but they are sorted
    by order of appearance in the field
* **SYNOPSIS**: The top `FRAGLIMIT` fragments are returned, concatenated by
    ellipses. This replicates the 'simplified' format.
* **FULL**: The entire field is returned as a single document, highlighted.
    This is the `NOTRUNCATE` keyword.

The default format is `SYNOPSIS`.

The return value depends on the `FORMAT` argument. For `SYNOPSIS` and `FULL`,
each returned field is a string. For the other formats, the field is returned
as an array.

The return value for this format is also different, as it will be an array of
arrays of fragments per field, whereas the simplified version will simply return
the best fragment as a field of its own.


### Return Semantics

The `HIGHLIGHTER` and/or `SUMMARIZE` keywords are logical extensions of the `RETURN`
keyword. If they are used in conjunction with that keyword, the fields will be
added to the list. A field can be RETURNed and SUMMARIZEd in the same query.

```
SUMMARIZE 2 foo bar RETURN 1 baz
```


## Implementation

The summarization/highlight subsystem is implemented using an environment-agnostic
highlighter/fragmenter, and a higher level which is integrated with RediSearch and
the query keyword parser.

The summarization process begins by tokenizing the requested field, and splitting
the document into *fragments*.

When a matching token (or its stemmed variant) is found, a distance counter begins.
This counts the number of tokens following the matched token. If another matching
token occurs before the maximum token distance has been exceeded, the counter is
reset to 0 and the fragment is extended.

Each time a token is found in a fragment, the fragment's score increases. The
score increase is dependent on the base token score (this is provided as
input to the fragmenter), and whether this term is being repeated, or if it is
a new occurrence (within the same fragment). New terms get higher scores; which
helps eliminate forms like "I said to Abraham: Abraham, why...".

The input score for each term is calculated based on the term's overall frequency
in the DB (lower frequency means higher score), but this is consider out of bounds
for the fragmenter.

Once all fragments are scored, they are then *contextualized*. The fragment's
context is determined to be X amount of tokens surrounding the given matched
tokens. Words in between the tokens are considered as well, ensuring that every
fragment is more or less the same size.