# Tasks: NESTED groups

Tasks 1 to 3 are one PR: a group that `FT.CREATE` accepts must be saved and
indexed per element in the same change. Tasks 4 and 5 are one PR each. A
checked item has code and tests in the commit of its PR.

## 1. Schema and FT.INFO

- [x] 1.1 `NestedGroup`, `IndexSpec.nestedGroups`, `FieldSpec_NestedMember`, `FieldSpec.nestedGroup`
- [x] 1.2 `FT.CREATE` parses `NESTED n` and rejects unsupported schemas
- [x] 1.3 `FT.INFO` shows `NESTED` and `NESTED_PATH` on members
- [x] 1.4 Tests: `createKeepsGroupsApartFromFields`, `createFindsGroupAfterMemberWithTrailingOption`, `createRejectsNameSharedByGroupAndField`, `createRejectsMemberPathThatDoesNotStartAtTheElement`, `testNestedGroupRejectsUnsupportedSchemas`, `testFieldNamedNestedAfterTypeOption`, `testInfoOfGroupAndItsFields`, `testObfuscatedInfoHidesTheGroup`
- [x] 1.5 Tests: `createRejectsGroupOnDisk`, and HASH in `testNestedGroupRejectsUnsupportedSchemas`

## 2. RDB version 30

- [x] 2.1 Save and load the group section, `Index_HasNestedGroups`, `nestedGroup`
- [x] 2.2 Validate loaded groups (`IndexSpec_PrepareLoadedNestedGroups`)
- [x] 2.3 Tests: `rdbRoundTripKeepsGroupsAndTheGroupOfEachField`, `rdbOfEarlierVersionLoadsWithoutGroups`, `rdbOfEarlierVersionCannotHoldAGroup`, `rdbWithGroupIsRefusedOnDisk`, `NestedGroupLegacyRdbTest.*`, `NestedGroupInvalidRdbTest.*` without `pathThatDoesNotCompile`

## 3. Indexing

- [x] 3.1 `JSON_LoadNestedMember` loads member values with element numbers
- [x] 3.2 TAG index entries store element numbers (`TagIndex.storesElements`)
- [x] 3.3 NUMERIC values stored per element in the sorting vector (`ElementNumbers`), found by `FieldSpec.nestedValuesPath`
- [x] 3.4 TAG iterator that reads these entries and reports element numbers on request (`ElementTag`, `TagElements`)
- [x] 3.5 Tests: `testGroupFieldSortsByItsFirstNumber`, `testGroupFieldsDisplayLikeMultiValueFields`, `testPathOfGroupFieldAtDocumentRootIsNotTheField`, `testConditionsOutsideGroupMatchWholeDocument`, `testGroupFieldValueOfWrongTypeFailsTheDocument`, `testGroupFieldSkipsNullAndIndexesFraction`, `NestedGroupInvalidRdbTest.pathThatDoesNotCompile`, `TagIndexTest.testElement*`, Rust `element_numbers`, `element_tag`, `repair_keeps_the_element_numbers_of_surviving_entries`

## 4. Iterators

- [x] 4.1 `ElementFilter` and its iterator type
- [x] 4.2 Walk of the positions that the children of a result share (`visit_common_positions`)
- [x] 4.3 Tests: Rust `element_filter`, `profile_print`

## 5. Query syntax and evaluator

- [x] 5.1 Grammar rule `modifier COLON LP expr RP`; the `modifier` rule finds the
  group; `scopeToElement`
- [x] 5.2 `QN_SAME_ELEMENT`, `QueryNode_SameElementChild`, `FT.EXPLAIN`
- [x] 5.3 DIALECT 1 error
- [x] 5.4 Evaluator for `QN_SAME_ELEMENT`
- [x] 5.5 Tests: the group queries of `test_json_nested.py`, `test_cpp_nested_group.cpp` (`parser*`), Rust `same_element`
- [x] 5.6 Tests that cover tasks 2 and 3 end to end, which need the query syntax: `testGroupOnEveryShardBeforeAndAfterReload`, `testGroupOnReplica`, `testGroupIsCreatedAgainFromAof`, `testGroupFollowsDocumentUpdate`, `testGroupAfterDeleteAndGC`, `testGroupCountsElementThatIsNotAnObject`

## 6. Before final review

- [x] 6.1 Split the change into the three PRs above
- [ ] 6.2 Resolve the open questions in `design.md`
- [ ] 6.3 User documentation for `NESTED`, and the release-notes checkbox
- [ ] 6.4 Update the spec delta to what shipped
