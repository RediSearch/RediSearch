# Characterising `QN_TAG` from Rust — design

Test-only change. It adds Rust integration tests that drive the **still-C**
`Query_EvalTagNode` through the Rust `query_eval::eval_node` entrypoint, so the
behaviour of `QN_TAG` is pinned *before* it is ported. Nothing in `src/query.c`,
and no production Rust or C, changes.

Two supporting crates *are* modified, both test-facing: the `ffi` bindgen
allowlist gains two symbols the fixture calls, and `rqe_iterators_test_utils`
gains the setters for the two pieces of query state it owns exclusively. Neither
changes behaviour of anything the module ships.

## 1. Architecture

### Problem

`query_eval` has taken over node-by-node dispatch: `eval_node` handles the ported
node types itself and delegates the rest back to the C dispatcher
(`query_eval/src/lib.rs`, `eval_node_c`). `QN_TAG` is one of the two remaining
delegated types — the other, `QN_VECTOR`, is already characterised from Rust by
`query_eval/tests/integration/vector.rs`.

`QN_TAG` is next in line to be ported, and today the only executable statement of
what it does is the C itself plus Python flow tests that reach it through
`FT.SEARCH`. A port would be reviewed against reading, not against a suite. The
tests here close that gap: they exercise the C through the same entrypoint the
port will keep, so the same file passes unchanged after the port and any
behavioural drift shows up as a failing test rather than as a review argument.

The scope is **every reachable path** through the four functions below, not a
representative sample: the expansion helpers are where a port has the most room
to drift silently, and they are also where the C's least obvious behaviour lives
(a timed-out expansion returns partial results *without* a warning; a capped one
returns partial results *with* one; whether the suffix trie is consulted is
decided by two different predicates on the two expansion paths).

```
Query_EvalTagNode(q, qn)
  idx = TagIndex_Open(node->fs)          -> NULL  => yield nothing
  NumChildren == 1                       => query_EvalSingleTagNode(child, qn->opts.weight)
  otherwise                              => union of query_EvalSingleTagNode(child_i, ...)
                                            quickExit = q->inNotSubTree || qn->opts.weight == 0
                                            union weight = qn->opts.weight  (NOT effective_weight)

query_EvalSingleTagNode(q, idx, n, weight, fs)
  caseSensitive   = fs->tagOpts.tagFlags & TagField_CaseSensitive
  effectiveWeight = is_hybrid(q->reqFlags) ? 0.0 : weight
                    is_hybrid = QEXEC_F_IS_HYBRID_SEARCH_SUBQUERY
                             || QEXEC_F_IS_HYBRID_VECTOR_AGGREGATE_SUBQUERY
  QN_TOKEN          -> tag_strtolower(token); TagIndex_OpenReader(token, effectiveWeight)
  QN_PREFIX         -> Query_EvalTagPrefixNode(effectiveWeight, FieldSpec_HasSuffixTrie(fs))
  QN_WILDCARD_QUERY -> Query_EvalTagWildcardNode(effectiveWeight)
  QN_PHRASE         -> sdsjoin(children' tokens, " "); TagIndex_OpenReader(joined, effectiveWeight)
  default           -> RS_ABORT

Query_EvalTagPrefixNode(q, idx, qn, weight, withSuffixTrie, fieldIndex, caseSensitive)
  tag_strtolower(pattern)
  len < config->minTermPrefix                 => NULL
  !pfx.suffix || !withSuffixTrie              => brute force over idx->values
      TagIndex_IterateValuesWithFilter(mode)  // PREFIX / SUFFIX / CONTAINS
      [!skipTimeoutChecks] TrieMapIterator_SetTimeout(sctx->time.timeout)
      per hit: TagIndex_OpenReader(value, weight=1)   -- NULL reader => skipped
      stop at config->maxPrefixExpansions
      hasNext && itsSz == max => SetReachedMaxPrefixExpansionsWarning
  otherwise                                   => TagIndex_GetSuffixMatches(prefix=pfx.prefix)
      NULL                                    => NULL (the whole node yields nothing)
      for each suffix key   while itsSz < max      -- outer guard, exits SILENTLY
        for each term listed under that key
          itsSz >= max => SetReachedMaxPrefixExpansionsWarning; break   -- the only warn site
          TagIndex_OpenReader(strlen(term), weight=1)   -- NULL reader => skipped
  NewUnionIterator(its, itsSz, quickExit=true, weight, QN_PREFIX, ...)

Query_EvalTagWildcardNode(q, idx, qn, weight, fieldIndex, caseSensitive)
  tag_strtolower(pattern); len = Wildcard_RemoveEscape(pattern, len)
  len == 0                                    => reader on the "" value alone, no scan
  TagIndex_HasSuffix(idx)                     => TagIndex_GetSuffixWildcardMatches
      NULL                                    => NULL (the whole node yields nothing)
      BAD_POINTER                             => fall back to brute force
      otherwise: per hit TagIndex_OpenReader(strlen(entry), weight=1), capped as above
  !HasSuffix || fellBack                      => brute force, TAG_WILDCARD_MODE, capped as above
  NewUnionIterator(its, itsSz, quickExit=true, weight, QN_WILDCARD_QUERY, ...)

tag_strtolower(&str, &len, caseSensitive)      // runs on every child type above
  drop each backslash that precedes an ispunct/isspace byte, shortening len
  [!caseSensitive] unicode_tolower(str, &len)
```

Two asymmetries in that tree drive much of §2.4 and are the easiest things for a
port to smooth over by accident:

- **Which predicate decides the suffix trie.** `Query_EvalTagPrefixNode` is told
  `FieldSpec_HasSuffixTrie(fs)` — a *field spec option* — while
  `Query_EvalTagWildcardNode` asks `TagIndex_HasSuffix(idx)` — whether the *index*
  has a suffix `TrieMap`. Production keeps them in step (`spec.c` passes
  `FieldSpec_HasSuffixTrie(fs)` to `TagIndex_Ensure`), so the tests keep them in
  step too and do not pin the disagreeing case.
- **What truncation reports.** The cap sets a warning on `q->status`; the timeout
  sets nothing at all, because the trie iterator simply stops yielding and
  `hasNext` comes back false, which is also the "iterator exhausted" signal. A
  short expansion and a timed-out one are indistinguishable from the status.

### User-visible surface

None. No command, option, field type, config or persisted format changes. The
deliverable is one new test module, its registration, two allowlist entries and
two test-utility setters.

### Subsystems touched

| Subsystem | Role |
| --- | --- |
| `src/query.c` | Under test. **Not modified.** |
| `src/tag_index.c`, `src/suffix.c` | `TagIndex_Open`/`_Ensure`/`_OpenIndex`/`_OpenReader`/`_Free`, `addSuffixTrieMap`, the `GetList_SuffixTrieMap*` walks. Not modified. |
| `query_eval` (tests) | New `tests/integration/tag.rs`, registered in `main.rs`; one added dev-dependency. |
| `query::mock` (`c_wrappers/query/src/mock/`) | One new setter, `MockQueryNode::set_tag_field_spec`. This module is `#[cfg(feature = "unittest")]` — test-only, never compiled into the shipped module. |
| `ffi` (`src/redisearch_rs/ffi/build.rs`) | Two symbols added to the bindgen allowlist: `TagIndex_Free` and `addSuffixTrieMap`. Allowlist-only — no generated binding is removed or changed, so no existing caller is affected. |
| `rqe_iterators_test_utils` | Two setters for state `TestContext` owns exclusively (`QueryEvalCtx.config`, `sctx->time`) plus the lock accessor a fixture needs to build C structures of its own. |

### Data model

The fixture owns the tag field, its tag index, the values in it, and the query
tree above it. Only the search context around them comes from `TestContext`.

- **Field spec.** A fixture-owned, zeroed `Box<ffi::FieldSpec>` in every case,
  built with `set_types(INDEXFLD_T_TAG)` and `index = 0`, exactly as `vector.rs`
  builds its vector field. Three flavours, differing only in what the fixture
  then does to it:
  - *`NoIndex`* — nothing more. `TagIndex_Open` asserts the field type and
    returns the null `tagOpts.tagIndex`, so nothing further is read off it.
  - *`Indexed`* — `TagIndex_Ensure(fs, /*diskSpec=*/null, /*withSuffix=*/false)`,
    which hangs a fresh `TagIndex` off `tagOpts.tagIndex`.
  - *`IndexedWithSuffixTrie`* — `options |= FieldSpec_WithSuffixTrie` **and**
    `TagIndex_Ensure(..., /*withSuffix=*/true)`. Both are needed and neither
    implies the other: the prefix path reads the option, the wildcard path reads
    `idx->suffix`.

  `case_sensitive` sets `tagOpts.tagFlags |= TagField_CaseSensitive` on the same
  owned spec. Only `types`, `options`, `index`, `tagOpts.tagFlags` and
  `tagOpts.tagIndex` are read during evaluation; every other field stays zeroed.

  The fixture frees the index with `TagIndex_Free` on drop, which walks
  `idx->values` freeing each `InvertedIndex` and frees `idx->suffix` with
  `suffixTrieMap_freeCallback` — so the values and the suffix entries are freed
  by the same call that frees what owns them.
- **Tag values.** `ffi::TagIndex_OpenIndex(idx, value, len, CREATE_INDEX, &sz)`
  followed by `inverted_index_ffi::InvertedIndex_WriteEntryGeneric` per document
  — the same two calls `TestContext::tag` makes for its own single value. Values
  are passed with an explicit length, so a value may carry an interior NUL. A
  value may be given *no* documents, which leaves an `InvertedIndex` holding zero
  documents in the trie: `TagIndex_OpenReader` returns NULL for it, which is how
  the `continue` in each expansion loop is reached.
- **Suffix trie.** On an `IndexedWithSuffixTrie` field the fixture additionally
  calls `ffi::addSuffixTrieMap(idx->suffix, value, len)` per value, mirroring
  what `TagIndex_Commit` does in memory mode and what `TestContext::prefix`
  already does for a `WITHSUFFIXTRIE` *text* field with `addSuffixTrie`. It
  mirrors the gate too: `addSuffixTrieMap` asserts on a zero length, so the empty
  value is written to `idx->values` only, exactly as production leaves it. That
  asymmetry is a fixture invariant rather than something a test asserts: no query
  can observe it, because every pattern that reaches a suffix-trie walk carries a
  literal token no empty value can contain, and the `minTermPrefix` gate keeps a
  zero-length prefix off the walk entirely. See §3.
- **Documents** are virtual records (`RSIndexResult::build_virt().doc_id(..)`);
  the tag reader yields doc ids and a weight, which is all the assertions read.
  Documents are not added to the `DocTable`: no test enables field expiration, so
  nothing consults it, and `index = 0` on a spec with no TTL table is never
  looked up.
- **Search context.** `TestContext::tag(std::iter::empty())` supplies the
  `RedisSearchCtx`, the `DocTable` and the `QueryEvalCtx` (with its real
  `status`, `config` and metric-request head). Its own TAG field and its
  placeholder `"test_tag"` value are unused and unreachable — the node points at
  the fixture's spec, not the context's — but a TAG-schema spec keeps
  `sctx->spec` coherent with the node under evaluation, which is what the reader
  the C opens is handed.
- **Query tree.** A `MockQueryNode` of type `Tag` pointed at the fixture's field
  spec, with one `MockQueryNode` child per `Child` (below). Token buffers are
  owned by `MockQueryNode::with_token`, except the one row whose token
  `tag_strtolower` frees, which uses `with_redis_token` — see *Edge cases*.

### Behaviours characterised

Each maps to one or more tests in §2.4.

1. `TagIndex_Open` returning NULL — the field has no tag index, so evaluation
   yields no iterator at all (not an empty one).
2. The single-child shortcut — one child evaluates straight through, with no
   union wrapped around it, and a child that resolves to nothing makes the whole
   node yield nothing.
3. The multi-child union path — `NewUnionIterator` over the children, including
   the reductions it applies: zero surviving children collapse to an empty
   iterator, one surviving child is returned bare.
4. `quickExit` selection — driven by `q->inNotSubTree` or by a zero node weight,
   observable as the number of child records on a document two children share.
5. Weight propagation — the node weight reaches the child readers, and *either*
   hybrid request flag forces the child readers to weight 0 *without* changing
   either `quickExit` or the weight of the union wrapped around them, both of
   which are keyed off `qn->opts.weight`. `is_hybrid` is a disjunction of two
   request flags; both disjuncts are tested, as both of `quickExit`'s are.
6. Every child type `query_EvalSingleTagNode` accepts: `QN_TOKEN`, `QN_PREFIX`
   on each of its three scan modes, `QN_WILDCARD_QUERY` — on a pattern that
   scans and on the empty pattern that short-circuits to the empty tag value —
   and `QN_PHRASE` including its `sdsjoin` of the children's tokens with a
   single space.
7. `tag_strtolower`'s escape removal, which runs ahead of any lowering on every
   one of those child types: a backslash before a punctuation or whitespace byte
   is dropped and the token shortened, so an escaped query token matches the
   unescaped indexed value. And all three of its lowering outcomes on a
   case-insensitive field — ASCII in place, multibyte re-encoded into the
   caller's buffer, and multibyte lengthened into a fresh `rm_malloc`'d buffer
   that replaces the token — each pinned by what the lookup then finds; see the
   ownership note under *Edge cases*.
8. Binary-safe and NUL-carrying tag values, on both a case-sensitive and a
   case-insensitive field — and, on the phrase path, the one place the C is
   *not* binary-safe.
9. Which expansions consult the suffix trie: a prefix-anchored `QN_PREFIX` is
   brute-forced even on a `WITHSUFFIXTRIE` field, a suffix- or contains-anchored
   one is not; and a `QN_WILDCARD_QUERY` consults it whenever the index has one.
10. What the suffix-trie walks return, including their two failure shapes: no
    matches makes the whole node yield **nothing** (not an empty iterator), and a
    wildcard pattern with no usable literal token comes back `BAD_POINTER` and
    falls back to the brute-force scan — a fallback whose only external tell is
    that it finds what the suffix walk would have missed.
11. The `maxPrefixExpansions` cap on all four expansion loops, and the exact
    condition for `QueryError_SetReachedMaxPrefixExpansionsWarning`, which is a
    different condition on each shape of loop. On the two brute-force loops the
    warning needs the trie iterator to have had *more* to give, so a scan whose
    match count lands exactly on the cap is capped silently. On the suffix-trie
    prefix walk — a nested loop — it fires only from the *inner* loop, so it
    needs one suffix key whose term list still has entries after the cap is hit;
    if every key's list is exhausted first, the outer `itsSz < max` guard ends
    the walk silently instead. Both halves of that are pinned.
12. Timeouts during expansion: an expired deadline truncates the scan and the
    node returns the partial expansion with **no** warning and **no** error,
    while `skipTimeoutChecks` disables the deadline entirely.
13. That an expansion skips a value whose inverted index holds no documents,
    rather than counting it against the cap or admitting a null reader.

### Edge cases

- **`tag_strtolower` may free the query token, and which allocator owns the
  token decides how a test may reach it.** `tag_strtolower`'s case-insensitive
  arm has three outcomes, not two, and they differ in *who owns the buffer
  afterwards*:
  1. **ASCII** — `unicode_tolower` lowercases in place, returns NULL, allocates
     nothing. The token buffer is still the caller's.
  2. **Multibyte, not lengthened** (`reencoded_len <= in_len`, the common case:
     `CAFÉ` → `café`, same byte count) — the re-encode is written back into the
     caller's buffer, `unicode_tolower` still returns NULL, and the length is
     updated. The buffer is still the caller's.
  3. **Multibyte, lengthened** (`reencoded_len > in_len`: `İ` U+0130 lowercases
     to the two codepoints `i̇`, 3 bytes against 2) — `unicode_tolower` returns a
     fresh `rm_malloc`'d buffer, `tag_strtolower` `rm_free`s the original and
     rewrites `*pstr` and `*len`. **Ownership moves to the Redis allocator.**

  `MockQueryNode::with_token` backs its token with a **Rust** allocation that the
  mock deallocates on drop, so it may be used for (1) and (2) but not for (3),
  where letting the C free it is a free-with-the-wrong-allocator followed by a
  double free. Rather than leave (3) untested — §1 says every reachable path, and
  a lengthening lower is ordinary production behaviour on a case-insensitive tag
  field — the mock gains a second constructor, `with_redis_token`, whose buffer
  is `RedisModule_Alloc`'d and whose drop frees whatever `tok.str_` points at
  *now* with `RedisModule_Free`. That is the same ownership dance
  `string_utils/tests/integration/tag_strtolower.rs` already performs around the
  same C function; see §2.3. Tests use `with_token` unless they are exercising
  (3), so the allocator swap stays confined to the rows that need it.

  What this suite pins about (2) and (3) is the *node-level* consequence — that
  the lowered, possibly relengthened token is what the tag lookup uses. The
  transformation itself is already characterised byte-for-byte, against the same
  C function and including a proptest over arbitrary UTF-8, in
  `string_utils/tests/integration/tag_strtolower.rs`; these rows do not restate
  it.
- **The phrase path truncates at a NUL whatever the field's case sensitivity.**
  `query_EvalSingleTagNode`'s `QN_PHRASE` arm joins the children with `sdsjoin`,
  which is `sdscat(join, argv[j])` — `strlen`-based, so a child token carrying an
  interior NUL contributes only the bytes before it, and `sdslen` of the join
  reflects the truncation. `tag_strtolower` has already NUL-terminated each child
  at its own scan point, so this is guaranteed rather than incidental. It is
  worth pinning because a Rust port would naturally join by the recorded token
  length and *preserve* the NUL — a silent behaviour change this suite exists to
  catch. Tested on a case-sensitive field, so no lowering step runs and `sdsjoin`
  is unambiguously the cause.
- **A NUL truncates a case-insensitive lookup.** `unicode_tolower`'s ASCII scan
  stops at an embedded NUL and shortens the length to it, so `b"ab\0cd"` is looked
  up as `b"ab"`. On a case-sensitive field the full length survives and the same
  bytes match the value indexed under them. Both halves are tested; they are the
  observable difference between the two field flavours.
- **The escape loop runs before the case-sensitivity check**, so it applies to a
  case-sensitive field too, and it is *not* redundant with the parser: `unescapen`
  in `src/query_parser/v2/parser.y` is applied to the field modifier only, so a
  value parsed out of `@tag:{red\ apple}` reaches the node still escaped. A port
  that lowercases and looks up without unescaping would silently stop matching
  such a query, which is why it is pinned rather than left to §3. The loop scans
  with `while (*p)`, so it stops at an interior NUL — the NUL-carrying tokens
  below carry no backslash, so the two behaviours do not interact.
- **On the wildcard path the escape loop runs *before* `Wildcard_RemoveEscape`,
  and eats its escapes — and neither of them can produce a literal `*`.**
  `Wildcard_RemoveEscape` only *deletes* backslashes; it leaves no marker saying
  the byte behind one was escaped, and the pattern is then matched in
  `TAG_WILDCARD_MODE` where every surviving `*` is a wildcard. `*` and `\` are
  both `ispunct`, so `tag_strtolower` turns `\*` into a bare `*` (and
  `Wildcard_RemoveEscape`, finding no backslash left, returns it unchanged),
  while `\\*` becomes `\*` and `Wildcard_RemoveEscape` then makes that a bare
  `*` too. The two patterns are therefore **byte-identical by the time anything
  matches**, and there is no way to query a literal `*` in a tag value at all —
  a wildcard `b*t` matches the value `b*t` only because `*` matches the literal
  `*` among any other bytes. Both are pinned, on a value set where the wildcard
  reading and a hypothetical literal one give different answers, because a port
  that kept an escape marker would diverge here. A lone `\` survives
  `tag_strtolower` (the byte after it is the NUL terminator, neither punctuation
  nor space) and is then consumed by `Wildcard_RemoveEscape`, leaving length 0 —
  so it takes the empty-pattern short-circuit rather than matching a backslash.
- **The empty wildcard pattern is a branch of its own.** `Query_EvalTagWildcardNode`
  short-circuits `tok->len == 0` (after `Wildcard_RemoveEscape`) past both the
  suffix-trie and the brute-force scans, opening a single reader on the `""` value
  — the one production indexes for an empty tag field. The fixture indexes `""`
  directly, exactly as `tagIndex_Put` does, so the branch is reachable, and the
  no-such-value half of it (`if (ret)` false) is reachable by leaving `""`
  unindexed, which yields an *empty union* rather than nothing.
- **The timeout is only observed every 100 traversal steps.** `trie_rs`'
  `IteratorTimeoutState` probes the clock once per `TIMEOUT_CHECK_GRANULARITY`
  (100) advances, so a deadline in the past does not truncate a small scan at
  all. The timeout tests therefore index a few hundred generated values and
  assert the expansion is *strictly smaller* than the untimed one and non-empty,
  rather than asserting an exact doc set — how many keys the first 100 advances
  yield depends on the trie's internal shape, which is not what is being pinned.
  For the same reason those tests raise `maxPrefixExpansions` above the value
  count, so the cap is not what truncates.
- **A past deadline is expressed as one second after boot.** `sctx->time.timeout`
  is an absolute `CLOCK_MONOTONIC` timespec, and `{0, 0}` is the sentinel
  `TrieMapIterator_SetTimeout` reads as *unlimited*. `{tv_sec: 1, tv_nsec: 0}` is
  therefore both non-sentinel and reliably in the past for any running process,
  with no clock call in the test.
- **`q->config` is not the `Config` threaded into `eval_node`.** `minTermPrefix`
  and `maxPrefixExpansions` are read off `QueryEvalCtx.config`, the FFI
  `IteratorsConfig` that `TestContext` allocates and initialises to the
  production defaults (200 and 2). The Rust `Config` argument reaches the C as an
  opaque `EvalConfig` that the tag path never reads, so overriding
  `Config::max_prefix_expansions` — the knob `prefix.rs` uses for the *ported*
  text prefix path — would have no effect here. The override goes through
  `TestContext` because `TestContext` owns that allocation.
- **The suffix trie is not binary-safe and the fixture does not pretend it is.**
  `addSuffixTrieMap` copies each value with `rm_strndup` and the `GetList_*`
  results are handed to `TagIndex_OpenReader` with `strlen`, so a value carrying
  an interior NUL could never be recovered from it. Every value in a
  suffix-trie fixture is ASCII; the NUL-carrying and binary values are exercised
  on the token and phrase paths, which take the length through unchanged.
- **`tagUniqueId` is a plain non-atomic global** that `NewTagIndex` post-increments,
  which is why `TestContext`'s constructors serialise on `CONTEXT_MUTEX`. The
  fixture creates its `TagIndex` under that same lock, obtained after the
  `TestContext` is fully built — the mutex is not reentrant, so the two cannot be
  nested.
- **A `QN_TAG` node with no children** takes the union path with zero iterators
  and yields an empty iterator. Not tested: the parser cannot produce it, and the
  reduction it exercises is already covered by the two-absent-children test.
- **The `if (!idx) return NULL` guards** at the top of `Query_EvalTagPrefixNode`
  and `Query_EvalTagWildcardNode` are unreachable from `Query_EvalTagNode`, which
  has already returned on a null index. They are left untested; see §3.

### Alternatives rejected

- **Hang the values off the `TagIndex` that `TestContext::tag` already owns**,
  flipping case sensitivity through `TestContext::spec_write()`. This was the
  earlier design, and the widened scope retires it: that index is created with
  `withSuffix = false` from a `SCHEMA tag_field TAG` spec, so no suffix-trie path
  is reachable through it, and its `FieldSpec` carries no `FieldSpec_WithSuffixTrie`
  option to set without reaching through a write guard into a spec the context
  owns. A fixture-owned `FieldSpec` sets all three knobs (option, flag, index)
  directly on something no one else can see, and `TagIndex_Free` puts the
  teardown next to the construction. The cost is one allowlist entry.
- **Extend `TestContext::tag` with a `with_suffix_trie` flag**, mirroring
  `TestContext::prefix(terms, with_suffix_trie)`. Rejected as redundant once the
  fixture owns its spec: the flag would only re-expose what
  `TagIndex_Ensure(fs, null, true)` already gives, and the value set, the
  case-sensitivity flag and the field-spec flavour are all things individual
  tests vary — `vector.rs` already establishes that the fixture for one node type
  lives with that node type's tests. What `TestContext` *does* gain is the two
  knobs it alone owns the allocations for.
- **Reach into `QueryEvalCtx.config` and `sctx->time` from `tag.rs` directly**
  through the raw pointer `TestContext::qctx()` hands out. Legal, and no
  allowlist or utility change needed. Rejected: those allocations are private to
  `QctxAlloc`, so a test writing through them is coupled to a layout the utility
  crate is free to change, and would break silently rather than fail to compile.
- **Populate the suffix trie with `TagIndex_Commit`** instead of allowlisting
  `addSuffixTrieMap`. It is the function production uses and in memory mode it
  does exactly the suffix-trie insert. Rejected: it takes its values as an array
  of NUL-terminated `char*` and an `IndexStats` out-parameter, so it can express
  neither an explicit length nor a value the fixture wants to keep out of the
  suffix trie, and it would drag `IndexStats` into the allowlist for nothing.
  `addSuffixTrieMap` is the same insert with the length passed explicitly, and
  `TestContext::prefix` already sets the precedent of calling the `addSuffix*`
  primitive directly.
- **Reuse `TestContext::tag(doc_ids)` for the values.** Rejected: it indexes
  exactly one hard-coded value, which cannot express a union of two values, a
  prefix that expands to two, a phrase, a binary value or a NUL-carrying one.
- **Write the tag node's `fs` through the payload union from `tag.rs`** (legal —
  `MockQueryNode::as_ptr` is public) instead of adding `set_tag_field_spec`.
  Rejected: it is the type confusion the mock's other setters exist to prevent,
  and the setter is twelve lines in a `unittest`-gated module that mirrors
  `set_missing_field` exactly.
- **Parse the query text and evaluate the resulting AST.** Rejected: it would
  test the parser as much as the evaluator, and could not construct the
  NUL-carrying and binary values at all.
- **Assert an exact doc set on the timed-out expansions.** Rejected: it would
  pin the trie's traversal order and node count, neither of which is the
  behaviour under test, and both of which a `trie_rs` change may legitimately
  move.

## 2. Program design

### 2.1 File tree

```
src/redisearch_rs/
├── ffi/
│   └── build.rs                         MODIFIED  (+2 allowlist entries)
├── rqe_iterators_test_utils/src/
│   └── test_context.rs                  MODIFIED  (+2 setters, +1 lock accessor)
├── query_eval/
│   ├── Cargo.toml                       MODIFIED  (+ inverted_index_ffi dev-dependency)
│   └── tests/integration/
│       ├── main.rs                      MODIFIED  (+ `mod tag;`)
│       └── tag.rs                       NEW       (the whole deliverable)
└── c_wrappers/query/src/mock/
    └── query_node_ref.rs                MODIFIED  (+ MockQueryNode::set_tag_field_spec,
                                                    + MockQueryNode::with_redis_token)
```

`query_eval` depends on `inverted_index` but not on `inverted_index_ffi`, and the
fixture needs the latter's `InvertedIndex_WriteEntryGeneric` to populate a tag
value — the same call `TestContext::tag` makes. The crate is already in the build
graph through `rqe_iterators_test_utils`, so this is one line under
`[dev-dependencies]`:

```diff
 geo.workspace = true
+inverted_index_ffi = { path = "../c_entrypoint/inverted_index_ffi" }
 proptest = { workspace = true, features = ["std"] }
```

It is a path dependency rather than `workspace = true`: `inverted_index_ffi` is
not listed in `[workspace.dependencies]`, and every crate that uses it names the
path, as `rqe_iterators_test_utils` does. No feature is needed — the fixture
calls only `InvertedIndex_WriteEntryGeneric`.

`main.rs`, keeping the list alphabetical:

```diff
 mod qast_iterate;
+mod tag;
 mod token;
```

The allowlist, in `ffi/build.rs`'s `HEADERS` table. Both symbols are declared in
the header they are added under, which is what `verify_symbols` checks:

```diff
     HeaderAllowlist {
         path: "src/suffix.h",
         fns: &[
             "Suffix_IterateContains",
             "Suffix_IterateWildcard",
             "addSuffixTrie",
+            "addSuffixTrieMap",
             "deleteSuffixTrie",
             "suffixTrie_freeCallback",
         ],
         types: &["SuffixCtx", "SuffixType"],
         vars: &["SUFFIX_STARRED_ANCHOR_PENALTY"],
@@
     HeaderAllowlist {
         path: "src/tag_index.h",
-        fns: &["TagIndex_Ensure", "TagIndex_OpenIndex"],
+        fns: &["TagIndex_Ensure", "TagIndex_Free", "TagIndex_OpenIndex"],
         types: &[],
         vars: &[],
     },
```

The `src/suffix.h` entry already carries `types` and `vars`, and the free
callback it exposes is `suffixTrie_freeCallback` — the plain-trie one, *not*
`suffixTrieMap_freeCallback`; only the one new `fns` line is added, and it goes
alphabetically after `addSuffixTrie`. `suffixTrieMap_freeCallback` stays
unexposed: nothing in the fixture calls it, since `TagIndex_Free` frees the
suffix trie itself.

No `types` entry is needed for either symbol: bindgen already emits `struct TagIndex`
with its fields as a dependency of `TagIndex_Ensure`'s signature, which is how
the fixture reaches `idx->suffix`.

### 2.2 Call stacks

What a test drives, `+` marking what is new. Nothing under `eval_node` changes.

```diff
+tag::TagFixture::new(TagOptions { .. })
+  GlobalGuard::default()
+  TestContext::tag(std::iter::empty())        // sctx, docTable, qctx only
+  [opts.max_prefix_expansions | opts.min_term_prefix]
+                        context.set_iterators_config(cfg)   // NEW setter
+  [opts.timeout != Unlimited] context.set_search_time(EXPIRED_DEADLINE, skip)  // NEW setter
+  rqe_iterators_test_utils::with_c_globals_locked(|| {      // NEW accessor
+    fs = Box::new(zeroed FieldSpec)
+      fs.set_types(INDEXFLD_T_TAG); fs.index = 0
+      [opts.field == IndexedWithSuffixTrie] fs.set_options(FieldSpec_WithSuffixTrie)
+      [opts.case_sensitive]  fs.tagOpts.set_tagFlags(TagField_CaseSensitive)
+    [opts.field != NoIndex] idx = ffi::TagIndex_Ensure(&mut *fs, null, with_suffix)
+    for (value, doc_ids) in opts.values:
+      ii = ffi::TagIndex_OpenIndex(idx, value, len, CREATE_INDEX, &sz)
+      for doc_id: inverted_index_ffi::InvertedIndex_WriteEntryGeneric(ii, virt_record(doc_id))
+      [with_suffix && !value.is_empty()] ffi::addSuffixTrieMap((*idx).suffix, value, len)
+  })
+  [opts.hybrid]         (*qctx).reqFlags |= QEFlags::from(QEFlag::IsHybridSearchSubquery).bits()
+                                          or ..IsHybridVectorAggregateSubquery ..bits()
+  QueryEvalContext::new(context.qctx())
+  [opts.in_not_sub_tree] ctx.set_in_not_sub_tree(true)
+  MockQueryNode::new(QueryNodeType::Tag)
+    node.opts_mut().weight = opts.weight
+    node.set_tag_field_spec(&*fs)            // NEW setter
+    node.set_children(&[child.as_ptr(), ..])
+
+tag::TagFixture::eval(&mut self) -> Option<ContractChecker<EvalResult<'_>>>
   eval_node(&mut ctx, QueryNodeMut::new(node), Config::default())
     eval_node_c                               // QN_TAG is not ported
       ffi::Query_EvalNode
         Query_EvalTagNode
           TagIndex_Open(node->fs)
           query_EvalSingleTagNode(child, qn->opts.weight, fs)   x NumChildren
+            Query_EvalTagPrefixNode / Query_EvalTagWildcardNode  // see §1
           NewUnionIterator(iters, n, quickExit, qn->opts.weight, QN_TAG, NULL, config)
+  ContractChecker::new(evaluated.into_boxed())
+
+impl Drop for TagFixture              // runs before *any* field is dropped, so
+                                      // `_context` is still alive here
+  [opts.field != NoIndex] ffi::TagIndex_Free(fs.tagOpts.tagIndex)
```

The children each test builds:

```
Child::Token(value)                 MockQueryNode::with_token(TokenNodeType::Token, value)
Child::RedisToken(value)            MockQueryNode::with_redis_token(TokenNodeType::Token, value)
Child::Prefix { pattern, .. }       MockQueryNode::with_token(TokenNodeType::Prefix, pattern)
                                      .set_prefix_mode(prefix, suffix)
Child::WildcardQuery(pattern)       MockQueryNode::with_token(TokenNodeType::WildcardQuery, pattern)
Child::Phrase(tokens)               MockQueryNode::new(QueryNodeType::Phrase)
                                      .set_children(one Token child per token)
```

Which expansion branch a `Child::Prefix` reaches, as a function of the two
anchoring flags and the field flavour — the table §2.4's suffix-trie rows are
built from:

```
                        Indexed                     IndexedWithSuffixTrie
prefix: true,  suffix: false   brute force, PREFIX_MODE     brute force, PREFIX_MODE
prefix: false, suffix: true    brute force, SUFFIX_MODE     GetSuffixMatches(prefix=false)
prefix: true,  suffix: true    brute force, CONTAINS_MODE   GetSuffixMatches(prefix=true)
```

### 2.3 Types and signatures

New, in `query_eval/tests/integration/tag.rs`:

```rust
/// A tag value and the documents indexed under it. Owned rather than
/// `&'static`, because the cap and timeout fixtures generate their value sets.
type Indexed = (Vec<u8>, Vec<DocId>);

/// The tag values the fixture indexes. Byte strings throughout: a tag value is
/// binary data, and only a value no `str` can hold tests that it is treated so.
const TAG_APPLE:   &[u8] = b"apple";        // docs 1, 2
const TAG_APRICOT: &[u8] = b"apricot";      // docs 2, 3  -- doc 2 is the overlap
const TAG_BANANA:  &[u8] = b"banana";       // docs 3, 4
const TAG_PHRASE:  &[u8] = b"red apple";    // doc 5 -- the `sdsjoin` target
const TAG_BINARY:  &[u8] = b"caf\xff";      // doc 6 -- 0xff appears in no UTF-8 sequence
const TAG_NUL:     &[u8] = b"ab\0cd";       // doc 7
const TAG_NUL_HEAD:&[u8] = b"ab";           // doc 8 -- what a truncated lookup of TAG_NUL hits
const TAG_NUL_PHRASE_JOINED: &[u8] = b"ab x";       // doc 9 -- what `sdsjoin` builds from TAG_NUL
const TAG_NUL_PHRASE_WHOLE:  &[u8] = b"ab\0cd x";   // doc 10 -- what a binary-safe join would build
const TAG_EMPTY:   &[u8] = b"";             // doc 11 -- the value production indexes for an
                                            //           empty tag field, and the only one the
                                            //           empty wildcard pattern can reach
const TAG_STAR:    &[u8] = b"b*t";          // doc 12 -- a literal `*` in a value; matched by the
                                            //           wildcard `b*t`, since `*` matches a `*`
const TAG_BAT:     &[u8] = b"bat";          // doc 13 -- matched by the wildcard `b*t` and by
                                            //           nothing that read the `*` as a literal
const TAG_SULTANA: &[u8] = b"sultana";      // doc 14 -- a second value under the suffix key `na`,
                                            //           so one key's term list outlives the cap
const TAG_CAFE:    &[u8] = "café".as_bytes();  // doc 15 -- lowering `CAFÉ` re-encodes to the same
                                            //            5 bytes, so the token buffer is rewritten
                                            //            in place rather than replaced
const TAG_DOTTED:  &[u8] = "i\u{307}stanbul".as_bytes();  // doc 16 -- what `İSTANBUL` lowers to:
                                            //            `İ` (U+0130, 2 bytes) becomes `i` + U+0307
                                            //            (3 bytes), so the result outgrows its
                                            //            buffer and `tag_strtolower` replaces it
const TAG_NO_DOCS: &[u8] = b"apogee";       // no documents -- shares the `ap` prefix, and is
                                            //           skipped by every expansion that finds it

/// The default value set: `TAG_APPLE` (docs 1, 2), `TAG_APRICOT` (docs 2, 3),
/// `TAG_BANANA` (docs 3, 4) and `TAG_PHRASE` (doc 5) — and nothing else, so a
/// pattern matching everything yields exactly `[1, 2, 3, 4, 5]`. Every other
/// constant above belongs to the tests that name it, which say so in their
/// value set. Covers every test that does not need the binary, NUL-carrying,
/// literal-`*` or generated values.
fn fruit() -> Vec<Indexed>;

/// `count` values `az0000`, `az0001`, … each holding one document, ids starting
/// at `first_doc_id`. Used where a scan has to be long enough to be capped or to
/// reach a clock probe; see the timeout granularity note in §1.
fn generated(count: u32, first_doc_id: DocId) -> Vec<Indexed>;

/// Adapt byte literals to owned [`Indexed`] pairs, so a test can keep writing
/// its value set inline.
fn values(pairs: &[(&[u8], &[DocId])]) -> Vec<Indexed>;

/// How many generated values the timeout tests index. Comfortably more than the
/// 100 traversal steps between two clock probes, so an expired deadline is
/// certain to cut the scan short and equally certain not to cut it to nothing.
const TIMEOUT_VALUES: u32 = 500;

/// A node weight that is neither the unit weight a per-expansion reader carries
/// nor the zero that selects `quickExit`, so a test reading a weight back can
/// tell all three apart.
const NODE_WEIGHT: f64 = 5.0;

/// A `CLOCK_MONOTONIC` deadline one second after boot: in the past for any
/// process that can run this test, and not the `{0, 0}` that
/// `TrieMapIterator_SetTimeout` reads as *unlimited*.
const EXPIRED_DEADLINE: ffi::timespec = ffi::timespec { tv_sec: 1, tv_nsec: 0 };

/// What kind of tag field the node names — which decides whether evaluation gets
/// past its first line, and which expansions consult a suffix trie.
enum TagField {
    /// A tag field with a populated index and no suffix trie, as a field created
    /// without `WITHSUFFIXTRIE` has.
    Indexed,
    /// A tag field carrying both halves of `WITHSUFFIXTRIE`: the
    /// `FieldSpec_WithSuffixTrie` option that `Query_EvalTagPrefixNode` reads,
    /// and the `TagIndex.suffix` trie that `Query_EvalTagWildcardNode` reads.
    /// Production always sets them together, so the fixture does too.
    IndexedWithSuffixTrie,
    /// A tag field whose index was never created, as a field no document has
    /// ever been written to leaves it.
    NoIndex,
}

/// A child of the tag node, one variant per node type `query_EvalSingleTagNode`
/// accepts.
enum Child {
    /// A plain `@tag:{value}` term.
    Token(&'static [u8]),
    /// The same, with the token buffer owned by the Redis allocator rather than
    /// the Rust one, so `tag_strtolower` may free and replace it. Used only by
    /// the lengthening-multibyte row; see the ownership note under *Edge cases*.
    RedisToken(&'static [u8]),
    /// A `@tag:{pat*}` expansion. `prefix`/`suffix` are the anchoring flags the
    /// parser sets from where the `*`s sit, and they pick the scan mode:
    /// `suffix` off is `TAG_PREFIX_MODE` (`pat*`), `suffix` alone is
    /// `TAG_SUFFIX_MODE` (`*pat`), and both is `TAG_CONTAINS_MODE` (`*pat*`).
    /// A node with both off is not a state the parser can produce, so no test
    /// builds one. On a [`TagField::IndexedWithSuffixTrie`] field the two
    /// `suffix`-anchored modes take the suffix-trie walk instead.
    Prefix { pattern: &'static [u8], prefix: bool, suffix: bool },
    /// A `@tag:{w'pat'}` wildcard expansion.
    WildcardQuery(&'static [u8]),
    /// A `@tag:{multi word value}` phrase, whose children are always tokens.
    Phrase(&'static [&'static [u8]]),
}

/// What deadline the search context carries while the node is evaluated.
enum Timeout {
    /// `sctx->time.timeout` left at `{0, 0}`, which the trie iterator reads as
    /// no deadline at all — the production default in these fixtures.
    Unlimited,
    /// [`EXPIRED_DEADLINE`], with timeout checks enabled: the scan stops at the
    /// first clock probe past it.
    Expired,
    /// [`EXPIRED_DEADLINE`] with `skipTimeoutChecks` set, which stops the
    /// evaluator installing it on the trie iterator at all.
    ExpiredButSkipped,
}

/// The two request flags `query_EvalSingleTagNode` reads as "this is a hybrid
/// subquery". They are alternatives, never both set at once.
enum Hybrid {
    /// `QEXEC_F_IS_HYBRID_SEARCH_SUBQUERY`.
    SearchSubquery,
    /// `QEXEC_F_IS_HYBRID_VECTOR_AGGREGATE_SUBQUERY`.
    VectorAggregateSubquery,
}

/// How to build a [`TagFixture`]. Each test sets only the knob it exercises and
/// leaves the rest at [`TagOptions::default`].
struct TagOptions {
    values: Vec<Indexed>,
    field: TagField,
    /// Sets `TagField_CaseSensitive` on the field, which decides whether the
    /// query token is lowercased — and, for a value carrying a NUL, whether the
    /// lookup keeps its full length.
    case_sensitive: bool,
    children: Vec<Child>,
    /// The node's weight, which reaches the child readers, the union above them,
    /// and the `quickExit` decision.
    weight: f64,
    /// Drives `quickExit` through `q->inNotSubTree` rather than a zero weight.
    in_not_sub_tree: bool,
    /// Which hybrid subquery flag to set on the request, if any. Either one
    /// zeroes the weight the child readers are opened with while leaving the
    /// node's own weight — and so the union's weight and `quickExit` — alone;
    /// `None` leaves the request non-hybrid.
    hybrid: Option<Hybrid>,
    /// Overrides `QueryEvalCtx.config->maxPrefixExpansions`, the cap on how many
    /// readers one expansion opens. `None` leaves the production default.
    /// This is *not* `Config::max_prefix_expansions`; see §1.
    max_prefix_expansions: Option<u32>,
    /// Overrides `QueryEvalCtx.config->minTermPrefix`. `None` leaves the
    /// production default of 2.
    min_term_prefix: Option<u32>,
    /// What deadline `sctx->time` carries.
    timeout: Timeout,
}

// Hand-written, *not* derived: a derived `Default` would give `weight: 0.0`,
// which is one of `quickExit`'s two disjuncts, so every test that left the
// weight alone would silently take the quick-exit path — and an empty value set,
// leaving nothing for a lookup to find.
impl Default for TagOptions {
    fn default() -> Self {
        Self {
            values: fruit(),                 // NOT `Vec::new()`
            field: TagField::Indexed,
            case_sensitive: false,
            children: Vec::new(),
            weight: 1.0,                     // NOT `0.0` -- see above
            in_not_sub_tree: false,
            hybrid: None,
            max_prefix_expansions: None,
            min_term_prefix: None,
            timeout: Timeout::Unlimited,
        }
    }
}

/// Owns everything a `QN_TAG` evaluation borrows or mutates.
struct TagFixture {
    _guard: GlobalGuard,
    /// Declared before the [`TestContext`] it borrows from, so it drops first.
    ctx: QueryEvalContext,
    /// Owns the `sctx`, the `DocTable` and the `QueryEvalCtx` that `ctx` wraps.
    /// Its own tag field is unused: the node names `field_spec` instead.
    _context: TestContext,
    /// The `QN_TAG` node under evaluation.
    node: MockQueryNode,
    /// The node's children, kept alive because the node holds raw pointers to
    /// them, and the phrase children's own token nodes with them.
    _children: Vec<MockQueryNode>,
    /// The field spec the node names. Boxed for a stable address, since the node
    /// points at it. For every [`TagField`] but [`TagField::NoIndex`] its
    /// `tagOpts.tagIndex` is a `TagIndex` this fixture created and frees — see
    /// the [`Drop`] impl, which must run before `_context` releases the
    /// allocator the index was built with.
    field_spec: Box<ffi::FieldSpec>,
}

impl TagFixture {
    fn new(opts: TagOptions) -> Self;

    /// Evaluate the node, wrapping whatever it yields in a contract checker.
    ///
    /// Returns `None` when evaluation yields no iterator. The checker owns the
    /// iterator and frees it on drop, and borrows the fixture for as long as it
    /// lives — so a test reading the query status does so after dropping it.
    fn eval(&mut self) -> Option<ContractChecker<EvalResult<'_>>>;

    /// Evaluate the node, asserting it yielded no iterator at all — which is not
    /// the same as yielding an empty one.
    fn eval_yielding_nothing(&mut self);

    /// Whether `QueryError_SetReachedMaxPrefixExpansionsWarning` fired during
    /// the last evaluation.
    fn reached_max_prefix_expansions(&mut self) -> bool;
}

impl Drop for TagFixture {
    /// Frees the fixture's `TagIndex`, which takes the per-value inverted
    /// indexes and the suffix trie with it.
    fn drop(&mut self);
}

/// One document an iterator yielded, with what the result says about it.
#[derive(Debug, PartialEq)]
struct Match {
    doc_id: DocId,
    /// The weight of the result itself. What that means depends on what the
    /// node built, which is exactly the distinction the hybrid tests turn on:
    ///
    /// - a multi-child node's union carries `qn->opts.weight` verbatim, so this
    ///   stays the node weight even under a hybrid request;
    /// - a single-child node returns its child's own iterator, so this is the
    ///   *effective* weight — a hybrid request zeroes it, and an expansion's
    ///   sub-union carries it while its per-expansion readers carry 1.
    weight: f64,
    /// The weights of the result's child records, in order, if it is an
    /// aggregate — a union result's are the child iterators' current results.
    ///
    /// **Empty when the result is not an aggregate** (`as_aggregate()` is
    /// `None`), which is every bare `InvIdxTag` reader: a leaf carries no child
    /// records, and its own weight is in [`weight`](Match::weight). A leaf is
    /// therefore distinguishable from a one-record aggregate, which matters
    /// because the union reduction that returns a lone surviving child *bare* is
    /// one of the behaviours pinned here.
    ///
    /// The length subsumes the arity `quickExit` is observed by: a quick-exit
    /// union stops at the first child holding the document, so this holds one
    /// entry however many children match.
    records: Vec<f64>,
}

/// Check `it` against the iterator contract, then rewind it and replay it into
/// the documents it yields.
///
/// The contract check comes first because it is what holds the C iterator to the
/// same rules a ported Rust one will have to meet. It rewinds only *between* its
/// two passes and returns with the iterator at EOF, so `drain` rewinds again
/// itself before the replay — without that the replay reads an exhausted
/// iterator and every result is empty.
fn drain(it: &mut ContractChecker<EvalResult<'_>>) -> Vec<Match>;

/// The doc ids of `drain(it)`, for the many tests that assert nothing else.
fn drain_doc_ids(it: &mut ContractChecker<EvalResult<'_>>) -> Vec<DocId>;
```

New, in `c_wrappers/query/src/mock/query_node_ref.rs`, mirroring
`set_missing_field`:

```rust
impl MockQueryNode {
    /// Set the `fs` field of the tag-node union variant.
    ///
    /// `fs` must outlive this `MockQueryNode`: evaluation opens the field's tag
    /// index and reads its case-sensitivity flag out of it.
    pub fn set_tag_field_spec(&mut self, fs: *const ffi::FieldSpec);

    /// Like [`with_token`](MockQueryNode::with_token), but the token buffer is
    /// allocated with `RedisModule_Alloc` instead of the Rust global allocator,
    /// so C may legitimately `rm_free` it and hand back a replacement.
    ///
    /// This exists for `tag_strtolower`, whose lengthening multibyte lower frees
    /// the token it was given and rewrites `tok.str_` and `tok.len`. Drop
    /// therefore frees whatever `tok.str_` points at *at drop time* with
    /// `RedisModule_Free` — not the pointer this returned — which is correct
    /// whether or not C replaced it, since both buffers come from the same
    /// allocator. The buffer is not tracked in `_aux`, which frees with the Rust
    /// allocator.
    ///
    /// Prefer `with_token`: this constructor is only sound for callees
    /// documented to own the buffer, and it cannot run under `miri`, which does
    /// not support the `RedisModule_Alloc` extern static.
    pub fn with_redis_token(type_: TokenNodeType, content: impl AsRef<[u8]>) -> Self;
}
```

New, in `rqe_iterators_test_utils/src/test_context.rs`. Both setters mutate
allocations `TestContext` owns and nothing else can reach, which is why they live
here rather than in `tag.rs`:

```rust
impl TestContext {
    /// Replace the [`IteratorsConfig`] that [`qctx`](TestContext::qctx) exposes
    /// as `QueryEvalCtx.config`.
    ///
    /// This is the config the **C** evaluator reads for `minTermPrefix` and
    /// `maxPrefixExpansions`. It is not the `Config` value threaded into
    /// `query_eval::eval_node`, which reaches the C as an opaque `EvalConfig`.
    ///
    /// Allocates the `QueryEvalCtx` if it does not exist yet, so a later
    /// `qctx()` returns one already carrying the override.
    pub fn set_iterators_config(&mut self, config: IteratorsConfig);

    /// Set the deadline and the skip flag in `sctx->time`.
    ///
    /// `timeout` is an absolute `CLOCK_MONOTONIC` deadline; `{0, 0}` is the
    /// sentinel every consumer reads as *no deadline*. `skip_checks` sets
    /// `skipTimeoutChecks`, which stops consumers installing the deadline at
    /// all — it wins over a deadline that has already passed.
    pub fn set_search_time(&mut self, timeout: ffi::timespec, skip_checks: bool);
}

/// Run `f` holding the lock the [`TestContext`] constructors take around C
/// global state, for a test that builds C structures of its own — a `TagIndex`
/// whose id comes from a plain non-atomic global, say.
///
/// The lock is not reentrant: call this *after* the constructor has returned,
/// never around one.
pub fn with_c_globals_locked<T>(f: impl FnOnce() -> T) -> T;
```

### 2.4 Tests

Rows are grouped by what they pin. Unless a row says otherwise the field is
`TagField::Indexed`, the value set is `fruit()`, and no cap, deadline or hybrid
flag is set.

**Index, children and union shape**

| Test | Shape | Pins |
| --- | --- | --- |
| `eval_tag_without_a_tag_index_yields_nothing` | `NoIndex`, one token child | `TagIndex_Open` NULL ⇒ `None`, not an empty iterator |
| `eval_tag_with_one_child_returns_its_reader_unwrapped` | one `Token(TAG_APPLE)`, `NODE_WEIGHT` | type `InvIdxTag` (no union), docs `[1, 2]`, `weight == NODE_WEIGHT`, `records == []` — a leaf, not a one-record aggregate |
| `eval_tag_with_one_absent_child_yields_nothing` | one `Token(b"durian")` | the shortcut returns the reader verbatim, so a NULL reader is the node's answer |
| `eval_tag_unions_its_children` | `Token(TAG_APPLE)`, `Token(TAG_BANANA)` | type `Union`, docs `[1, 2, 3, 4]` |
| `eval_tag_union_drops_a_child_value_that_is_not_indexed` | `Token(TAG_APPLE)`, `Token(b"durian")` | NULL child filtered, union reduced to the bare reader (`InvIdxTag`), so `records == []` |
| `eval_tag_union_of_absent_values_is_empty` | two absent tokens | reduced to `Empty`, yields nothing — and is *not* `None` |

**`quickExit` and weight**

| Test | Shape | Pins |
| --- | --- | --- |
| `eval_tag_full_union_reports_every_matching_child` | `Token(TAG_APPLE)`, `Token(TAG_APRICOT)`, weight 1.0 | `quickExit` off: doc 2 has `records.len() == 2` |
| `eval_tag_zero_weight_takes_quick_exit` | same, `weight: 0.0` | doc 2 has `records.len() == 1` |
| `eval_tag_in_a_not_subtree_takes_quick_exit` | same, non-zero weight, `in_not_sub_tree` | the other disjunct, same effect |
| `eval_tag_union_carries_the_node_weight` | two tokens, `NODE_WEIGHT` | doc 2: `weight == NODE_WEIGHT` (the union's own) **and** `records == [NODE_WEIGHT, NODE_WEIGHT]` (each child reader's) |
| `eval_tag_hybrid_search_subquery_opens_its_readers_with_zero_weight` | two tokens, `NODE_WEIGHT`, `Hybrid::SearchSubquery` | doc 2: `records == [0.0, 0.0]` — the zeroing, readable nowhere else — while `weight == NODE_WEIGHT`, because `NewUnionIterator` is passed `qn->opts.weight`, and `records.len() == 2`, because `quickExit` reads it too |
| `eval_tag_hybrid_vector_aggregate_subquery_opens_its_readers_with_zero_weight` | same, `Hybrid::VectorAggregateSubquery` | the other disjunct of `is_hybrid`, same three assertions |
| `eval_tag_hybrid_single_child_reader_carries_zero_weight` | one `Token(TAG_APPLE)`, `NODE_WEIGHT`, `Hybrid::SearchSubquery` | with no union above it the effective weight *is* the result's: `weight == 0.0`, `records == []` |

**Child node types**

| Test | Shape | Pins |
| --- | --- | --- |
| `eval_tag_prefix_child_expands_to_every_matching_value` | `Prefix { b"ap", prefix: true, suffix: false }`, `NODE_WEIGHT` | `TAG_PREFIX_MODE`: docs `[1, 2, 3]`; the expansion sub-union carries `NODE_WEIGHT` and its per-expansion readers unit weight, so doc 2 has `weight == NODE_WEIGHT`, `records == [1.0]` |
| `eval_tag_prefix_child_expansions_quick_exit` | `Prefix { b"ap", … }` with `apple` and `apricot` both holding doc 2 | the expansion union is built with `quickExit` hard-coded true: doc 2 has `records.len() == 1` even though two expansions hold it |
| `eval_tag_prefix_child_in_suffix_mode_matches_the_value_ending` | `Prefix { b"na", prefix: false, suffix: true }` | `TAG_SUFFIX_MODE`: docs `[3, 4]` — `banana` ends in `na` and no value starts with it, so a prefix scan of the same pattern would find nothing |
| `eval_tag_prefix_child_in_contains_mode_matches_anywhere` | `Prefix { b"an", prefix: true, suffix: true }` | `TAG_CONTAINS_MODE`: docs `[3, 4]` — `banana` matches on an interior `an`, which neither a prefix nor a suffix scan would reach |
| `eval_tag_prefix_child_shorter_than_the_minimum_yields_nothing` | `Prefix { b"a", prefix: true, suffix: false }` | `min_term_prefix` rejects it before the index is touched |
| `eval_tag_prefix_child_honours_a_raised_minimum` | `Prefix { b"ap", … }`, `min_term_prefix: Some(3)` | the bound is read from `QueryEvalCtx.config`, not baked in |
| `eval_tag_prefix_child_skips_a_value_with_no_documents` | `Prefix { b"ap", … }`, values include `TAG_NO_DOCS` | docs `[1, 2, 3]` and `records.len() == 1` for a doc in one expansion: the NULL reader is skipped, not admitted as an empty child |
| `eval_tag_wildcard_child_matches_by_pattern` | `WildcardQuery(b"ba*na")` | docs `[3, 4]` — `banana` only |
| `eval_tag_wildcard_child_skips_a_value_with_no_documents` | `WildcardQuery(b"a*")`, values include `TAG_NO_DOCS` | the `continue` in the wildcard *brute-force* loop, the counterpart of the prefix-path row above: `apogee` matches the pattern but opens a NULL reader, so it is skipped rather than admitted, leaving docs `[1, 2, 3]` |
| `eval_tag_wildcard_child_with_an_empty_pattern_matches_the_empty_value` | `WildcardQuery(b"")`, values include `(TAG_EMPTY, [11])` | doc `[11]` alone: the `tok->len == 0` short-circuit reads the `""` value and skips the scan, so no other value can match |
| `eval_tag_wildcard_child_with_an_empty_pattern_and_no_empty_value_is_empty` | `WildcardQuery(b"")`, `fruit()` | the short-circuit's reader is NULL, so the node yields an *empty union*, not `None` |
| `eval_tag_phrase_child_joins_its_tokens_with_a_space` | `Phrase(&[b"red", b"apple"])` | doc `[5]`: the `sdsjoin` looked up `"red apple"`, not either token |
| `eval_tag_phrase_child_truncates_a_token_at_a_nul` | `case_sensitive`, `Phrase(&[TAG_NUL, b"x"])`, values include both phrase values | doc `[9]`, not 10: `sdsjoin` is `strlen`-based, so the join is `"ab x"` even though no lowering step ran |

**Escapes, case and binary values**

| Test | Shape | Pins |
| --- | --- | --- |
| `eval_tag_token_child_drops_an_escaping_backslash` | `Token(b"red\\ apple")` | doc `[5]`: the escape loop drops the `\` before the space, so the 10-byte token looks up the 9-byte `TAG_PHRASE` value |
| `eval_tag_case_sensitive_field_still_drops_an_escaping_backslash` | `case_sensitive`, same token | doc `[5]`: the escape loop runs ahead of the case-sensitivity check |
| `eval_tag_prefix_child_drops_an_escaping_backslash` | `Prefix { b"red\\ ap", prefix: true, suffix: false }` | doc `[5]`: the same loop runs on the prefix pattern, so the scan is anchored on `red ap` |
| `eval_tag_wildcard_child_escape_is_eaten_before_remove_escape` | `WildcardQuery(b"b\\*t")`, values include `TAG_STAR` and `TAG_BAT` | docs `[12, 13]`: `tag_strtolower` drops the `\` first, so `Wildcard_RemoveEscape` sees no backslash and the pattern matches as the wildcard `b*t` — which finds `bat`, unreachable to any literal reading, as well as the `b*t` value a literal reading would have found alone |
| `eval_tag_wildcard_child_double_escape_is_the_same_wildcard` | `WildcardQuery(b"b\\\\*t")`, same values | docs `[12, 13]` again: `tag_strtolower` leaves `\*` and `Wildcard_RemoveEscape` strips that backslash too, so the doubly-escaped pattern is byte-identical to the singly-escaped one by match time. There is no literal `*` query; a port that kept an escape marker through the unescape would answer `[12]` here |
| `eval_tag_wildcard_child_of_a_lone_backslash_matches_the_empty_value` | `WildcardQuery(b"\\")`, values include `(TAG_EMPTY, [11])` | doc `[11]`: the backslash survives `tag_strtolower` (the next byte is the terminator) and is consumed by `Wildcard_RemoveEscape`, leaving length 0 — so the length check *after* the unescape is what decides |
| `eval_tag_lowercases_the_query_on_a_case_insensitive_field` | `Token(b"APPLE")` | docs `[1, 2]` |
| `eval_tag_multibyte_query_lowered_in_place` | `Token("CAFÉ".as_bytes())`, values include `(TAG_CAFE, [15])` | doc `[15]`: the lowered form re-encodes to the same 5 bytes, so `unicode_tolower` writes it back into the token's own buffer and returns NULL — the token pointer the node holds is unchanged and the lookup uses the rewritten bytes |
| `eval_tag_multibyte_query_lowered_into_a_longer_buffer` | `RedisToken("İSTANBUL".as_bytes())`, values include `(TAG_DOTTED, [16])` | doc `[16]`: the lower outgrows the buffer, so `tag_strtolower` `rm_free`s the token and rewrites `tok.str_`/`tok.len` — the lookup uses the *replacement*, at its new longer length. The one row needing `with_redis_token`; a port that kept the original pointer or the original length finds nothing |
| `eval_tag_case_sensitive_field_keeps_the_query_case` | `case_sensitive`, `Token(b"APPLE")`, then `Token(TAG_APPLE)` | the first yields nothing, the second yields `[1, 2]` |
| `eval_tag_case_sensitive_field_matches_a_binary_value` | `case_sensitive`, `Token(TAG_BINARY)` | doc `[6]`: no lowering step, so the `0xff` reaches the lookup verbatim |
| `eval_tag_lookup_stops_at_a_nul_on_a_case_insensitive_field` | `Token(TAG_NUL)`, values include both `TAG_NUL` and `TAG_NUL_HEAD` | doc `[8]`, not 7: the length was cut at the NUL |
| `eval_tag_case_sensitive_field_matches_past_a_nul` | `case_sensitive`, same | doc `[7]`: the full length survives |

**Suffix trie**

All on `IndexedWithSuffixTrie`. The negative controls in the first two rows are
what make the rest of the group meaningful: the same query on `Indexed` reaches a
different branch with, deliberately, the same answer.

| Test | Shape | Pins |
| --- | --- | --- |
| `eval_tag_prefix_child_ignores_the_suffix_trie_when_prefix_anchored` | `Prefix { b"ap", prefix: true, suffix: false }` | docs `[1, 2, 3]`: `!pfx.suffix` short-circuits the `withSuffixTrie` test, so a `WITHSUFFIXTRIE` field still brute-forces a `pat*` query |
| `eval_tag_prefix_child_in_suffix_mode_uses_the_suffix_trie` | `Prefix { b"na", prefix: false, suffix: true }` | docs `[3, 4]`: `TagIndex_GetSuffixMatches(prefix=false)` is an exact `TrieMap_Find` on the suffix, so it agrees with the brute-force scan it replaced |
| `eval_tag_prefix_child_in_contains_mode_uses_the_suffix_trie` | `Prefix { b"an", prefix: true, suffix: true }` | docs `[3, 4]`: `prefix=true` walks every suffix *starting* with `an` and unions their term lists |
| `eval_tag_suffix_trie_miss_yields_nothing` | `Prefix { b"zz", prefix: false, suffix: true }` | `GetSuffixMatches` returns NULL and the node returns NULL — an *absent* iterator, where the brute-force path in the same situation returns an empty one. The clearest divergence between the two branches |
| `eval_tag_suffix_trie_contains_miss_yields_nothing` | `Prefix { b"zz", prefix: true, suffix: true }` | the same NULL, reached through the empty-array arm of the `prefix=true` walk rather than the `TRIEMAP_NOTFOUND` arm |
| `eval_tag_prefix_child_via_suffix_trie_skips_a_value_with_no_documents` | `Prefix { b"ee", prefix: false, suffix: true }`, values include `TAG_NO_DOCS` | the `continue` on the suffix-trie side: the value is in the trie but its reader is NULL |
| `eval_tag_wildcard_child_uses_the_suffix_trie` | `WildcardQuery(b"ba*na")` | docs `[3, 4]`: `TagIndex_HasSuffix` is enough — no field option is consulted — and the answer matches the brute-force row above |
| `eval_tag_wildcard_child_with_no_usable_token_falls_back_to_brute_force` | `WildcardQuery(b"*")` | `Suffix_ChooseToken` finds no literal token in an all-`*` pattern, so `GetSuffixWildcardMatches` returns `BAD_POINTER` and the scan runs anyway: every value in `fruit()` matches, docs `[1, 2, 3, 4, 5]` — where a port that treated `BAD_POINTER` as "no matches" would yield nothing |
| `eval_tag_wildcard_child_via_suffix_trie_skips_a_value_with_no_documents` | `WildcardQuery(b"*ogee")`, values include `TAG_NO_DOCS` | the `continue` in the `GetSuffixWildcardMatches` result loop: `ogee` is a usable literal token and the trie hits `apogee`, whose reader is NULL, so the node yields an **empty** iterator — which is what separates this from the `!arr` row below, where it yields none at all |
| `eval_tag_wildcard_child_suffix_trie_miss_yields_nothing` | `WildcardQuery(b"zz*qq")` | the pattern has a usable token that hits nothing: NULL, so the node yields nothing rather than an empty iterator |

**Expansion cap**

| Test | Shape | Pins |
| --- | --- | --- |
| `eval_tag_prefix_expansion_stops_at_the_cap` | `Prefix { b"ap", prefix: true, suffix: false }`, `max_prefix_expansions: Some(1)` | one reader opened, docs `[1, 2]`, and `reached_max_prefix_expansions()` — `hasNext` was still true |
| `eval_tag_prefix_expansion_at_the_cap_warns_only_if_more_remained` | same, `max_prefix_expansions: Some(2)` (exactly the number of matches) | the full docs `[1, 2, 3]` and **no** warning: the loop's next `TrieMapIterator_Next` came back empty, so `hasNext` is false and the `itsSz == max` test does not fire |
| `eval_tag_prefix_expansion_cap_grows_the_iterator_array` | `Prefix { b"az", … }`, `values: generated(20, 100)`, `max_prefix_expansions: Some(20)` | 20 readers, past the initial capacity of 8, so the `rm_realloc` growth path runs; all 20 docs present, no warning |
| `eval_tag_suffix_trie_expansion_stops_at_the_cap` | `IndexedWithSuffixTrie`, `Prefix { b"na", prefix: true, suffix: true }`, `max_prefix_expansions: Some(1)`, values include `TAG_SULTANA` | the cap inside the nested walk, which only warns from the *inner* loop: the suffix key `na` lists both `banana` and `sultana`, so the term list still has an entry when the cap is hit. One reader — the bare `InvIdxTag` the union reduces to, holding one of those two values' documents, since `GetList_SuffixTrieMap` does not pin the order within a key's term list — and `reached_max_prefix_expansions()` |
| `eval_tag_suffix_trie_expansion_at_the_cap_exits_silently` | same, default `fruit()` values (so `na` lists `banana` alone) | the other half of behaviour 11's nested-loop condition: the inner list is exhausted before the cap can be re-tested, so the outer `itsSz < max` guard ends the walk with docs `[3, 4]` and **no** warning |
| `eval_tag_wildcard_expansion_stops_at_the_cap` | `WildcardQuery(b"*a*")`, `max_prefix_expansions: Some(1)` | the brute-force wildcard loop's own cap and warning |
| `eval_tag_wildcard_suffix_trie_expansion_stops_at_the_cap` | `IndexedWithSuffixTrie`, `WildcardQuery(b"*an*")`, `max_prefix_expansions: Some(1)` | the fourth and last cap site |
| `eval_tag_expansion_under_the_cap_sets_no_warning` | `Prefix { b"ap", … }`, default cap | the control: the warning is not set by an ordinary expansion |

**Timeouts**

`values: generated(TIMEOUT_VALUES, 100)` and a cap raised past
`TIMEOUT_VALUES` throughout, so the cap cannot be what truncates.

| Test | Shape | Pins |
| --- | --- | --- |
| `eval_tag_prefix_expansion_stops_at_the_deadline` | `Prefix { b"az", prefix: true, suffix: false }`, `Timeout::Expired` | fewer docs than `TIMEOUT_VALUES` and more than none |
| `eval_tag_prefix_expansion_that_times_out_reports_nothing` | same | the behaviour a port is most likely to "fix": **no** max-expansions warning and `status.code() == Ok`. A truncated expansion is indistinguishable from a complete one from the status alone |
| `eval_tag_prefix_expansion_ignores_the_deadline_when_checks_are_skipped` | same, `Timeout::ExpiredButSkipped` | all `TIMEOUT_VALUES` docs: `skipTimeoutChecks` stops the deadline being installed at all |
| `eval_tag_wildcard_expansion_stops_at_the_deadline` | `WildcardQuery(b"az*")`, `Timeout::Expired` | the wildcard brute-force scan takes the same deadline |
| `eval_tag_suffix_trie_expansion_stops_at_the_deadline` | `IndexedWithSuffixTrie`, `Prefix { b"az", prefix: true, suffix: true }`, `Timeout::Expired` | the deadline reaches the suffix-trie walk too, through `GetList_SuffixTrieMap`'s own iterator |
| `eval_tag_expansion_without_a_deadline_is_complete` | `Prefix { b"az", … }`, `Timeout::Unlimited` | the control the three truncation rows are compared against, and the `{0, 0}`-means-unlimited sentinel |

All tests are `#![cfg(not(miri))]` at module level, as `vector.rs` is:
`TestContext` calls into the C library, which Miri cannot execute.

## 3. Out of scope

Stated so the port is not read as having tested them:

- The **disk mode** of `TagIndex_OpenReader` (`idx->diskSpec`). Every fixture
  passes a null `diskSpec` to `TagIndex_Ensure`, so the tag index is always in
  memory mode.
- `RS_ABORT` on an **invalid child type**, which is `LCOV_EXCL`-marked
  unreachable.
- The `if (!idx) return NULL` guards at the top of `Query_EvalTagPrefixNode` and
  `Query_EvalTagWildcardNode`. `Query_EvalTagNode` returns before either is
  called when the index is null, so no query can reach them; they are defensive,
  not behaviour.
- **The empty value's absence from the suffix trie.** The fixture keeps it out
  because `addSuffixTrieMap` asserts on a zero length, matching production's own
  gate — but nothing here pins that absence, because nothing can: a suffix-trie
  walk is only reached by a pattern carrying a literal token, which no empty
  value contains, so both branches answer the same. The one query that *does*
  read the empty value, `WildcardQuery(b"")`, is short-circuited before any scan.
- **Values carrying an interior NUL under a suffix trie.** The suffix trie stores
  NUL-terminated copies and its results are re-read with `strlen`, so such a
  value cannot round-trip through it. That is a property of the suffix trie
  rather than of `QN_TAG` evaluation, and the NUL-carrying values are exercised
  on the paths that do take a length.
