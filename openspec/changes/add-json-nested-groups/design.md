# Design: NESTED groups

## Schema

- `IndexSpec.nestedGroups` holds the groups (`NestedGroup`: name, path, compiled
  path). It is NULL when the schema has no group. The code of the groups is in
  `src/nested_group.c`.
- A member is a `FieldSpec` with the option `FieldSpec_NestedMember`.
  `FieldSpec.nestedGroup` is the position of its group in
  `IndexSpec.nestedGroups`.
- `FT.CREATE` rejects: a group on HASH or on disk, a group in `FT.ALTER`, a
  group inside a group, a name that a field or a group already has, a group
  path that selects one value, a member path that selects more than one value
  in an element, a member path that does not start with `$`, a member that is
  not TAG or NUMERIC, a SORTABLE TAG member, `NOINDEX`, `INDEXEMPTY` or
  `INDEXMISSING` on a member, and fewer members than `NESTED n` declares.
- A member also has `FieldSpec.nestedValuesPathText`: the group path joined
  with the member path. It selects the values of the member across all
  elements, which is what `RETURN` and `LOAD` show. Only a NUMERIC member has
  its compiled form, `FieldSpec.nestedValuesPath`.

## Indexing

The group path is evaluated once per document and group. For each member,
`JSON_LoadNestedMember` iterates the elements and gives each value its element
number. A NUMERIC member also evaluates `nestedValuesPath` once per document.

- **TAG member.** The tag index has `storesElements` set. Each index entry
  holds the element numbers where the tag occurs in the document, in the place
  of the offsets of a term record. One entry per tag and document.
- **NUMERIC member.** The numeric index is unchanged. The values are also
  stored per element with the document, in the sorting vector
  (`ElementNumbers`). Thus a NUMERIC member is SORTABLE, also when the schema
  does not say so. Reason: a range such as `[(100 +inf]` is true for most
  elements. To read the numeric index for it is slow (see Alternatives). To
  read the stored value of the few candidate elements is fast.
- **Document update.** The document is indexed again in full, as today.

## Query

- **Grammar.** The rule is `expr ::= modifier COLON LP expr RP`, for every
  name. The `modifier` rule, which already finds the field of a name in the
  schema, also finds the group of a name that is not a field. The action
  accepts the rule only for a group. Reason: the lexer stays free of the
  schema; schema lookups are already done in the grammar actions.
  - Text between the parentheses of a TEXT field still takes
    `modifier COLON text_expr`. lemon reports no conflict and no precedence
    resolution that master does not have.
  - A name that is not a group is a syntax error at the name. Before, the
    error was at the first token that is not text:
    `@title:(@k:{x} @k:{x})` gave `Syntax error at offset 8 near k`, and now
    gives `Syntax error at offset 0 near title`.
  - A group name where a rule expects a field of some type
    (`@group:hello`, `@group:{a}`, `@group:[1 2]`, `ismissing(@group)`) is
    the error of the group syntax.
  - Measured on 4974 queries from the parser tests and a generated grid, on
    three schemas and DIALECT 2 to 4: each of the 14661 valid replies of
    `FT.EXPLAIN` has the same tree as with a lexer that makes a token for a
    group name.
- **Parser.** For a group, the action calls `scopeToElement`:
  - one TAG or NUMERIC condition on a member stays as it is;
  - an AND becomes a `QN_SAME_ELEMENT` node. Each child must be a TAG
    condition on a member, a NUMERIC condition on a member, or an OR of TAG
    conditions on members. `QueryNode_SameElementChild` is the one statement
    of this rule; the parser and the evaluator both call it;
  - an OR, a NOT or an optional at the top of the group: each child is scoped
    on its own;
  - all other shapes are a syntax error: text, an exact phrase, a field
    outside the group, and a NOT, a NUMERIC OR or another AND inside an AND.
- **DIALECT 1.** The v1 rule `modifier COLON expr` returns an error for a
  group name. The v1 tag and numeric rules are unchanged: a group name there is
  an unknown field, which matches nothing, as before.
- **Evaluator.** A `QN_SAME_ELEMENT` node becomes an `ElementFilter` on the
  intersection of its children.
  - With a TAG child: the TAG iterators report element numbers as positions.
    The candidate elements are the positions that all TAG children share.
  - With NUMERIC children only: the numeric indexes find the documents. All
    elements are candidates.
  - `ElementFilter` then tests each NUMERIC condition on the stored value of
    each candidate element. One element that passes keeps the document.
  - Element numbers stop at the `ElementFilter`. They are not word positions
    for SLOP, INORDER or HIGHLIGHT.
- **Score.** A group contributes nothing to the score.
- **Sort.** A NUMERIC member sorts by its first number. It loads and shows as
  a plain multi-value SORTABLE field does.

## RDB version 30

- The saved index flags have the bit `Index_HasNestedGroups` when the schema
  has a group. The bit is only in the RDB, not in `IndexSpec.flags`.
- A member saves `nestedGroup` after its other field data.
- After the fields: the number of groups, then the name and the path of each
  group.
- A schema without a group saves the same bytes as version 29, apart from the
  version number.
- Versions before 30 load as before. A version before 30 that has the bit or
  a member is refused.
- A load validates the groups with the rules of `FT.CREATE`
  (`IndexSpec_PrepareLoadedNestedGroups`). A load on disk refuses a group.
- No downgrade: an older build does not load version 30.

## Alternatives considered

- **Hidden sub-document per element.** One indexed document per element does
  not scale to arrays of thousands of elements.
- **Element numbers in the numeric index.** Correct, but 82 ms for 10,000
  documents × 13 elements. The goal is 7 ms.
- **Heap merge of positions.** No gain measured.
- **Fast path only for members that the schema makes SORTABLE.** A range on a
  member without SORTABLE reads the numeric index, which is the slow path of
  the alternative above.

## Measured results

Generated data set, one dev machine. Not a benchmark.

| Case | Result |
| --- | --- |
| Two conditions, 10,000 documents × 13 elements | 4.2–5.6 ms (goal 7 ms) |
| Two conditions, 1,000 documents × 3,848 elements | 0.7–0.9 ms |
| NUMERIC conditions only, same two data sets | ~14 ms and 16–17 ms |
| Update of one document of 3,848 elements | ~21 ms, about 2× a plain multi-value index |
| Stored per-element values, 3.85 million values | ≈ +210 MB, not counted by `FT.INFO` |

## Testing strategy

- **C++ unit** (`tests/cpptests/test_cpp_nested_group.cpp`): schema, RDB and
  query parser.
- **Rust integration**: `element_numbers` (`value`), `element_filter` and
  `element_tag` (`rqe_iterators`), `same_element` (`query_eval`).
- **Python flow** (`tests/pytests/test_json_nested.py`): the behavior that a
  user sees, which the spec delta names scenario by scenario.
- **Benchmark**: a manual check of the measured results above. It is not in
  the test suite.

## Open questions for maintainers

1. An invalid `@field:( conditions )` query on a field that is not a group
   now reports its syntax error at the name of the field, not at the first
   condition (example in Query, Grammar). Is the new position acceptable?
2. Memory accounting: should `FT.INFO` count the stored per-element values?
3. Is the sorting vector the correct place for the per-element values?
4. Scoring: should a group contribute to the score?
5. `FT.AGGREGATE LOAD @v SORTBY @v` sorts by the loaded JSON value, and
   `FT.SEARCH SORTBY v` by the first number: should the two agree?
