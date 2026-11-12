# index-null (delta)

Behavior delta for indexing and querying JSON `null` values. On merge, fold into
`openspec/specs/index-null/spec.md`.

## ADDED Requirements

### Requirement: INDEXNULL is accepted on every field type
`FT.CREATE` and `FT.ALTER` SHALL accept the `INDEXNULL` option on TEXT, TAG,
NUMERIC, GEO, GEOSHAPE and VECTOR fields, wherever and in whatever combination
they accept `INDEXMISSING`, and `FT.INFO` SHALL list `INDEXNULL` on the fields
that have it.

#### Scenario: Field types
- **WHEN** the user creates a JSON index with a field of any of the types above carrying `INDEXNULL`
- **THEN** the index SHALL be created

#### Scenario: Together with INDEXMISSING
- **WHEN** a NUMERIC, GEO, GEOSHAPE or VECTOR field carries both `INDEXNULL` and `INDEXMISSING`, in either order
- **THEN** the index SHALL be created with both options on the field

#### Scenario: FT.INFO
- **WHEN** a field was created with `INDEXNULL`
- **THEN** the field's attributes in `FT.INFO` SHALL include `INDEXNULL`

### Requirement: Invalid INDEXNULL definitions are rejected
A field definition SHALL be rejected when `INDEXNULL` can't take effect.

#### Scenario: With NOINDEX
- **WHEN** a field carries both `INDEXNULL` and `NOINDEX`
- **THEN** the command SHALL fail with `Field cannot be defined with both `NOINDEX` and `INDEXNULL``

#### Scenario: On a disk-backed (Flex) index
- **WHEN** a field of any type carries `INDEXNULL` on a disk-backed index
- **THEN** the command SHALL fail with `INDEXNULL is not supported in Redis Flex`

### Requirement: isnull matches documents whose field is null
The query `isnull(@field)` SHALL match exactly the documents in which the field's
JSONPath yields a single `null` result, and SHALL NOT match documents in which
the field holds a value or is absent.

#### Scenario: Null, value and absent
- **WHEN** documents `null`, `value` and `missing` respectively hold `null` in field `f`, hold a value in it, and lack it
- **AND** `f` carries `INDEXNULL` and `INDEXMISSING`
- **THEN** `isnull(@f)` SHALL match only `null`
- **AND** `ismissing(@f)` SHALL match only `missing`
- **AND** `-isnull(@f)` SHALL match `value` and `missing`

#### Scenario: Multi-value results
- **WHEN** the field's JSONPath yields several results, some or all of which are `null`, or yields no result at all
- **THEN** `isnull(@f)` SHALL NOT match the document

#### Scenario: Updates
- **WHEN** a document's field changes from `null` to a value
- **THEN** `isnull(@f)` SHALL stop matching it
- **AND WHEN** the field changes back to `null`
- **THEN** `isnull(@f)` SHALL match it again

#### Scenario: Field without INDEXNULL
- **WHEN** the user queries `isnull(@f)` and `f` doesn't carry `INDEXNULL`
- **THEN** the query SHALL fail with `'isnull' requires defining the field with 'INDEXNULL'`

#### Scenario: Persistence
- **WHEN** a field gains `INDEXNULL` through `FT.ALTER`
- **THEN** documents already holding `null` in it, and documents indexed later, SHALL match `isnull(@f)`
- **AND** they SHALL keep matching after an RDB save and reload

### Requirement: Null-value bookkeeping is garbage collected
The garbage collector SHALL remove deleted documents from a field's null-value
index, and remove the index once it holds no documents.

#### Scenario: Deleted document
- **WHEN** a document holding `null` in `f` is deleted and the GC runs
- **THEN** `isnull(@f)` SHALL no longer match it
