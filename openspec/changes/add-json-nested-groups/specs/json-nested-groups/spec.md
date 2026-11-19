# json-nested-groups (delta)

Each scenario names the test that covers it. Python tests are in
`tests/pytests/test_json_nested.py`. C++ tests are in
`tests/cpptests/test_cpp_nested_group.cpp`.

## ADDED Requirements

### Requirement: A group matches on one element
A query `@group:( ... )` SHALL match a document only when one element of the
group's array satisfies all the conditions.

#### Scenario: Two conditions on one element
- **WHEN** a document has headcount 90 and revenue 500 in two elements
- **AND** the query is `@metrics:(@k:{headcount} @v:[(100 +inf])`
- **THEN** the document SHALL NOT match (`testGroupMatchesOneElement`)

#### Scenario: Conditions outside a group
- **WHEN** the query is `@k:{headcount} @v:[(100 +inf]`
- **THEN** the same document SHALL match, as today (`testConditionsOutsideGroupMatchWholeDocument`)

#### Scenario: NUMERIC conditions only
- **WHEN** a group holds only NUMERIC conditions
- **THEN** one element SHALL satisfy all of them (`testGroupOfNumericConditionsOnly`)

#### Scenario: OR, NOT and optional
- **WHEN** an OR, a NOT or an optional is at the top of the group
- **THEN** each child SHALL be tested on one element on its own (`testOrNotAndOptionalAtTheTopOfGroup`)

### Requirement: A group accepts only conditions on its members
A group SHALL accept TAG and NUMERIC conditions on its members and an OR of TAG
conditions. All other content SHALL be a syntax error.

#### Scenario: Unsupported content
- **WHEN** a group holds text, a field of another group, or a NOT inside an AND
- **THEN** the error SHALL contain `A condition on one array element must be` (`testGroupRejectsConditionsItCannotMatchToAnElement`, `testGroupNameMustBeTheGroupOfItsFields`)

#### Scenario: DIALECT 1
- **WHEN** a DIALECT 1 query has `@group:` before an expression that is not a tag or a numeric condition
- **THEN** the error SHALL contain `Conditions on a NESTED group require DIALECT 2 or higher` (`testGroupSyntaxRequiresDialect2`)
- **AND** `@group:{x}` and `@group:[1 2]` SHALL match nothing, as for a name that is not a field (`dialect1RejectsConditionsOnAGroup`)

### Requirement: Valid queries without a group do not change
A valid query that names no group SHALL return the same result as before,
with or without a group in the schema. A query that has conditions between the
parentheses of a field that is not a group SHALL be a syntax error at the name
of the field.

#### Scenario: Conditions in a field that is not a group
- **WHEN** the query is `@title:(@k:{x} @k:{x})`
- **THEN** the error SHALL be `SEARCH_SYNTAX Syntax error at offset 0 near title` (`parserReportsConditionsInAFieldAtTheFieldName`, and end to end `testGroupNameMustBeTheGroupOfItsFields`)

#### Scenario: Text in a field
- **WHEN** the query is `@title:(hello world)`
- **THEN** the query SHALL parse to the same phrase as before (`parserReportsConditionsInAFieldAtTheFieldName`)

#### Scenario: A group name where a field is expected
- **WHEN** the query is `@group:hello`, `@group:{a}`, `@group:[1 2]` or `ismissing(@group)`
- **THEN** the error SHALL contain `A condition on one array element must be` (`parserRejectsGroupNameWhereAFieldIsExpected`, `testGroupNameMustBeTheGroupOfItsFields`)

### Requirement: FT.CREATE validates a group
`FT.CREATE` SHALL accept `<path> AS <name> NESTED <n>` followed by `n` TAG or
NUMERIC members on an in-memory JSON index, and SHALL reject all other schemas.

#### Scenario: Unsupported schemas
- **WHEN** a member is TEXT, a SORTABLE TAG, or has `NOINDEX`, `INDEXEMPTY` or `INDEXMISSING`
- **OR** the group has fewer members than declared, a missing or invalid count, or a path that does not select array elements
- **OR** a group is inside a group, or a member path selects several values in an element
- **OR** the index is on HASH, or `FT.ALTER` adds a group
- **THEN** the command SHALL return an error (`testNestedGroupRejectsUnsupportedSchemas`)

#### Scenario: Name of a group
- **WHEN** a group has the name of a field or of another group
- **THEN** `FT.CREATE` SHALL return an error (`createRejectsNameSharedByGroupAndField`)

#### Scenario: Member path that does not start at an element
- **WHEN** the path of a member does not start at an element, as `$.name` does
- **THEN** `FT.CREATE` SHALL return an error (`createRejectsMemberPathThatDoesNotStartAtTheElement`)

#### Scenario: Member path that does not follow the path of the group
- **WHEN** the path of the group followed by the path of a TAG or NUMERIC member is not a path, as with `$v`
- **THEN** `FT.CREATE` SHALL return an error (`testNestedGroupRejectsUnsupportedSchemas`)

#### Scenario: FT.CREATE on disk
- **WHEN** `FT.CREATE` declares a group on disk
- **THEN** the command SHALL return an error (`createRejectsGroupOnDisk`)

#### Scenario: RDB on disk
- **WHEN** an RDB with a group is loaded on disk
- **THEN** the load SHALL fail (`rdbWithGroupIsRefusedOnDisk`)

### Requirement: Member values
A TAG member SHALL accept only strings and nulls in an element, as a
multi-value TAG field does. A NUMERIC member SHALL accept only numbers and
nulls. An element without a value, or with a null, SHALL be skipped.

#### Scenario: Value of the wrong type
- **WHEN** the NUMERIC member of an element holds a string, an array, an object or a boolean
- **OR** the TAG member of an element holds a number or an array
- **THEN** the document SHALL fail to index, with the error of a plain field of that type: `NUMERIC fields can only contain numeric or nulls` or `TEXT/TAG fields can only contain strings or nulls` (`testGroupFieldValueOfWrongTypeFailsTheDocument`)

#### Scenario: Null and fraction
- **WHEN** the TAG member of an element holds a null, or the NUMERIC member a number that is not an integer
- **THEN** the null SHALL be skipped and the number SHALL be indexed (`testGroupFieldSkipsNullAndIndexesFraction`)

### Requirement: FT.INFO and FT.EXPLAIN show the group
`FT.INFO` SHALL show the group of each member, and SHALL NOT list the group as
an attribute. With obfuscation, a member SHALL show `NESTED` with the
obfuscated name of the group (`NestedGroup@<position>`) and SHALL NOT show
`NESTED_PATH`. `FT.EXPLAIN` SHALL show the conditions of a group below the name
of the group.

#### Scenario: FT.INFO
- **WHEN** the index has a group
- **THEN** each member SHALL show `NESTED <group>` and `NESTED_PATH <path>`, and a NUMERIC member SHALL show `SORTABLE` (`testInfoOfGroupAndItsFields`)

#### Scenario: FT.INFO with obfuscation
- **WHEN** the debug command shows the obfuscated info of an index that has a group
- **THEN** each member SHALL show `NESTED NestedGroup@<position>` and SHALL NOT show `NESTED_PATH` (`testObfuscatedInfoHidesTheGroup`)

#### Scenario: FT.EXPLAIN
- **WHEN** the query has a group
- **THEN** the output SHALL contain `SAME-ELEMENT:@<group> {` (`testExplainAndProfileShowTheGroup`)

### Requirement: Score and sort
A group SHALL NOT change the score of a document. A NUMERIC member SHALL sort
by its first number where the sort does not load the member (`FT.SEARCH
SORTBY`), and SHALL load and show as a multi-value SORTABLE field does. A sort
on a loaded member (`FT.AGGREGATE LOAD`) SHALL sort by the loaded value, as it
does for a multi-value SORTABLE field.

#### Scenario: Score
- **WHEN** a group is added to a query, with any weight
- **THEN** the score SHALL be that of the rest of the query (`testGroupDoesNotChangeTheScore`)

#### Scenario: Sort
- **WHEN** `FT.SEARCH` sorts results by a NUMERIC member
- **THEN** a document SHALL sort by its first number (`testGroupFieldSortsByItsFirstNumber`)

#### Scenario: Sort on a loaded member
- **WHEN** `FT.AGGREGATE` loads a NUMERIC member and sorts by it
- **THEN** a document whose first element holds a null SHALL sort by that null (`testGroupFieldSortsByItsFirstNumber`)

### Requirement: Persistence and replication
RDB encoding version 30 SHALL hold the groups. Earlier versions SHALL load.

#### Scenario: Reload
- **WHEN** an index with a group is saved and loaded
- **THEN** group queries SHALL return the same results (`testGroupOnEveryShardBeforeAndAfterReload`, `rdbRoundTripKeepsGroupsAndTheGroupOfEachField`)

#### Scenario: Schema without a group
- **WHEN** a schema without a group is saved
- **THEN** the saved bytes SHALL load as version 29 and as version 30 (`rdbOfEarlierVersionLoadsWithoutGroups`)

#### Scenario: Earlier version with a group
- **WHEN** an RDB of a version before 30 holds a group or a member
- **THEN** the load SHALL fail (`rdbOfEarlierVersionCannotHoldAGroup`)

#### Scenario: Invalid group in an RDB
- **WHEN** a loaded group breaks a rule of `FT.CREATE`
- **THEN** the load SHALL fail (`NestedGroupInvalidRdbTest.*`)

#### Scenario: Replica
- **WHEN** a master creates an index with a group
- **THEN** a replica SHALL get it from the replicated command and, after a full sync, from the RDB (`testGroupOnReplica`)

#### Scenario: AOF
- **WHEN** the append-only file is loaded
- **THEN** the group SHALL exist and match (`testGroupIsCreatedAgainFromAof`)

#### Scenario: Document update
- **WHEN** a document changes
- **THEN** group queries SHALL follow the new elements (`testGroupFollowsDocumentUpdate`)
