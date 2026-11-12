# Tasks: INDEXNULL

## 1. Schema

- [x] 1.1 Add `FieldSpec_IndexNull` (C and Rust) and `IndexSpec.nulls`
- [x] 1.2 Parse `INDEXNULL` for every field type, alongside `INDEXMISSING`
- [x] 1.3 Reject `INDEXNULL` with `NOINDEX`, and on disk-backed specs
- [x] 1.4 List `INDEXNULL` in `FT.INFO`, `commands.json` and the generated command info

## 2. Indexing

- [x] 2.1 Record `null` fields in `nulls.indexes` (`writeNullFieldDocs`), sharing a helper with `writeMissingFieldDocs`

## 3. Querying

- [x] 3.1 `isnull` in the lexer and parser, producing `QN_ISNULL`; error for fields without `INDEXNULL`
- [x] 3.2 `QN_ISNULL` at every node switch site (free, params, children, explain, optimizer, obfuscation)
- [x] 3.3 Rust: `QueryNode::IsNull`, its evaluation, and the Missing iterator generalized over `FieldDocsKind`

## 4. Garbage collection

- [x] 4.1 Generalize the missing-docs scanner over `FieldDocsKind`
- [x] 4.2 Run a null-docs pass in the fork and in-process drivers

## 5. Tests

- [x] 5.1 Python: validations, `FT.INFO`, every field type, negation, updates
- [x] 5.2 Python: `FT.EXPLAIN` / `FT.PROFILE`, fork GC, `FT.ALTER` with RDB reload
- [x] 5.3 Python: multi-value JSONPath matrix
- [x] 5.4 Python: Flex rejects `INDEXNULL` on every field type
- [x] 5.5 Rust: in-process GC null-docs pass, with live and deleted docs, and once empty
- [x] 5.6 Update the existing Rust iterator and GC tests for the generalization

## 6. Docs & spec delta

- [x] 6.1 Delta spec under `specs/index-null/spec.md`
- [ ] 6.2 User-facing docs for `INDEXNULL` and `isnull` (redis.io docs)
- [x] 6.3 Release-notes checkbox set on the PR
