# Port `QAST_CheckIsValid` to `query_eval`

## Scope and pull-request fit

This change fits one pull request. It moves one validation entry point and its five private helpers
from `src/query.c` into the existing `query_eval` and `query_eval_ffi` crates. The parsed AST,
index schema, search options, error representation, callers, and public C ABI remain in place. No
preparatory C reshaping, crate split, or public API migration is required.

## Architecture

### Problem

Query evaluation is moving to Rust, but its immediately preceding validation pass still lives in
`src/query.c`. `QAST_CheckIsValid` recursively inspects the same typed query nodes and schema
metadata already exposed to `query_eval`. Keeping this pass in C leaves validation coupled to the
old evaluator module and duplicates the C-to-Rust boundary immediately before `QAST_Iterate`.

The port must preserve behavior, including two non-obvious contracts:

- An invalid empty token sets `QueryError` but does **not** change the function's
  `REDISMODULE_OK` return value.
- Traversing a TAG node sets `QueryNode_IsTag` and, when applicable,
  `QueryNode_IndexesEmpty` in the caller-owned `RSSearchOptions.flags`; descendants observe that
  state and it is not restored after recursion.

### User-visible surface

There is no new or changed user-visible surface. `QAST_CheckIsValid` keeps its existing C name,
parameters, status codes, error codes and messages. The generated `query_eval_ffi.h` declaration is
compatible with the declaration retained in `query.h`, so existing C and C++ callers do not move.

Validation continues to cover:

- slop/in-order restrictions for JSON fields with undefined ordering;
- empty TEXT and TAG tokens versus `INDEXEMPTY`;
- invalid numeric ranges and their field-specific private diagnostics;
- vector and explicit-weight restrictions, including the hybrid main-vector exemption; and
- unsupported TAG prefix/suffix/infix and wildcard queries under Flex validation.

### Subsystems touched

- **C query module:** remove the C implementation and its local helpers, but retain the public
  declaration and callers.
- **Rust query wrappers:** use the existing `QueryNodeRef` typed payload dispatch and child
  traversal, changing the TAG payload's field reference to `Option<&ffi::FieldSpec>` because v1
  parser TAG nodes may have no field.
- **Index/field wrappers:** expose the schema predicates currently encoded as C macros and direct
  flag checks, rather than repeating raw bit arithmetic in query validation.
- **FFI bindings/search-disk wrapper:** add `SearchDisk_IsEnabledForValidation` to the
  `src/search_disk.h` function allowlist in `ffi/build.rs`, then expose that generated binding as a
  safe process-wide “enabled for validation” query.
- **`query_eval`:** own the recursive validation algorithm and error construction.
- **`query_eval_ffi`:** reconstruct safe borrowed views from the four established C pointers and
  map the Rust boolean result to `REDISMODULE_OK`/`REDISMODULE_ERR`.
- **Generated headers:** publish the unchanged `QAST_CheckIsValid` ABI from Rust.

### Data model and ownership

No data is re-owned or copied.

- `QueryAST` remains C-owned. Rust borrows its root through `QueryNodeRef` for the duration of the
  call; validation does not mutate nodes.
- `QueryNodeRef::as_enum()` represents the nullable `QueryTagNode.fs` pointer as
  `Option<&ffi::FieldSpec>`. This preserves `NewTagNode(NULL)` nodes without dereferencing null;
  non-null field specs remain borrowed from the C-owned schema.
- `IndexSpec` and its `FieldSpec` array remain C-owned and already locked by the request path. Rust
  uses shared wrapper references only.
- `RSSearchOptions` remains caller-owned and is mutably borrowed because TAG traversal deliberately
  accumulates context bits in `flags`.
- `QueryError` remains the opaque Rust value stored by C. The FFI layer obtains one mutable
  `query_error::QueryError` reference and the validator uses its existing first-error-wins setters.
- `QASTValidationFlagsSet` and `QueryNodeFlagsSet` remain the typed Rust bitflag definitions already
  shared with generated C headers.

The C helper `IndexSpec_GetFieldsByMask` allocates a temporary array. The Rust port instead filters
`IndexSpec::field_specs()` in place by “indexable TEXT and selected by mask.” This is allocation-free
and produces the same empty/non-empty decision without changing schema ownership.

### Edge cases and invariants

1. The ordinary-index fast path remains before root traversal. Flex validation always traverses;
   otherwise validation is skipped only when the schema has neither a non-`INDEXEMPTY` TEXT/TAG
   field nor JSON undefined ordering.
2. FFI pointers are required to be non-null and valid, matching the existing C assertion and caller
   contract. The FFI entry point checks them before constructing wrappers; a parsed AST must have a
   non-null root.
3. The hybrid exemption removes `NoWeight` and `NoVector` only for a vector node marked
   `HybridVectorSubqueryNode`. Children are still checked with the AST's original flags.
4. Explicit-weight rejection precedes node-specific checks and child traversal, preserving which
   error wins.
5. Null and missing nodes stop recursion. Every other node recurses only while the current result
   remains successful.
6. Phrase slop/in-order checks run only for JSON schemas carrying the cached undefined-order flag,
   and only when top-level options or local overrides require them. Field filtering uses the node's
   field mask exactly as C does.
7. A field mask selecting no indexable TEXT field is accepted for empty-token validation for
   backward compatibility. `RS_FIELDMASK_ALL` is treated as accepting empty TEXT, as today.
8. TAG Flex checks inspect immediate TAG children before recursive validation and return on the
   first unsupported prefix or wildcard child.
9. Numeric private diagnostics retain `: @<field>:[<min> <max>]` with C-compatible floating-point
   display and keep the field name out of the public/obfuscated message.
10. The C switch currently routes `QN_UNION` through the token helper, but a well-formed union has
    no token payload and the check is inert. The typed Rust match validates only actual token
    payloads; this is the same behavior for all valid ASTs and avoids reading an inactive union
    member.
11. Empty-token validation intentionally discards its boolean failure after setting the syntax
    error, then continues into child traversal and returns success unless another validation fails.
12. Existing tests that simulate Flex validation through global configuration remain serialized as
    they are today; the port adds no new mutable global state.
13. TAG nodes may have a null field pointer (`NewTagNode(NULL)` is used by the v1 parser). Such a
    node still sets `QueryNode_IsTag`, skips the `FieldSpec_IndexesEmpty` check, performs Flex child
    checks, and recurses exactly as the C validator does.

### Alternatives rejected

- **Keep a C orchestration function and port helpers one at a time.** Rejected because every helper
  is private, small, and shares one recursive state machine; an intermediate callback boundary adds
  unsafe crossings without creating a reviewable independent unit.
- **Call C schema helpers from Rust.** Rejected because their logic is straightforward over already
  wrapped immutable schema data, and `IndexSpec_GetFieldsByMask` allocates solely for iteration.
- **Read `ffi::IndexSpec` and `ffi::FieldSpec` flags directly in `query_eval`.** Rejected because it
  duplicates C macro semantics in a second consumer. Small predicates belong on the existing safe
  wrappers and are independently testable.
- **Replace `RSSearchOptions.flags` mutation with a local TAG context.** Rejected because it would
  silently change the observable caller-owned options contract and could alter later sibling
  validation.
- **Return an error for an invalid empty token.** Rejected despite being more conventional because
  this is a behavior-preserving port; the pre-port C++ contract test explicitly pins the existing
  status/error mismatch.
- **Rename the ABI to `QAST_CheckIsValid_Rs` and add a C adapter.** Rejected because the symbol can
  move directly to `query_eval_ffi`, as `QAST_Iterate` already has, with no adapter or caller churn.
- **Split this into prerequisite wrapper and port pull requests.** Rejected because the wrapper
  additions are narrow predicates required by one bounded port and do not independently deliver
  user value.

## Program design

### File-tree diff

```diff
 src/
 ├── query.c                                      MODIFIED  remove validator and five helpers
 └── redisearch_rs/
     ├── ffi/build.rs                             MODIFIED  allowlist disk-validation predicate
     ├── c_wrappers/
     │   ├── field_spec/Cargo.toml                MODIFIED  add rqe_core dependency for FieldMask
     │   ├── field_spec/src/lib.rs                MODIFIED  field-mask/option predicates
     │   ├── field_spec/tests/tests.rs            MODIFIED  predicate tests
     │   ├── index_spec/Cargo.toml                MODIFIED  promote document dependency
     │   ├── index_spec/src/lib.rs                MODIFIED  cached-flag and JSON predicates
     │   ├── index_spec/tests/tests.rs            MODIFIED  schema predicate tests
     │   ├── query/src/query_node_ref.rs           MODIFIED  nullable TAG field payload
     │   ├── query/tests/integration/
     │   │   └── query_node_ref/mod.rs             MODIFIED  nullable TAG payload test
     │   ├── search_disk/src/lib.rs               MODIFIED  validation-mode accessor
     │   └── search_disk/tests/handle.rs           MODIFIED  validation-mode accessor test
     ├── c_entrypoint/query_eval_ffi/Cargo.toml    MODIFIED  wrapper/error dependencies
     ├── c_entrypoint/query_eval_ffi/src/lib.rs   MODIFIED  export QAST_CheckIsValid
     ├── headers/query_eval_ffi.h                 MODIFIED  regenerated ABI declaration
     └── query_eval/
         ├── Cargo.toml                           MODIFIED  index_spec/field_spec dependencies
         ├── src/lib.rs                           MODIFIED  module and API export
         ├── src/validation.rs                    NEW       validation traversal
         ├── tests/integration/main.rs            MODIFIED  register validation tests
         └── tests/integration/validation.rs      NEW       Rust branch/contract tests
```

`query.h`, `src/aggregate/aggregate_request.c`, and the previously added C++ contract tests remain
unchanged.

### Orchestration change

```diff
 aggregate request / C++ test helper
-└── QAST_CheckIsValid                         [src/query.c]
-    ├── ordinary-index fast path
-    └── QueryNode_CheckIsValid                [recursive C walk]
-        ├── QueryNode_CheckAllowSlopAndInorder
-        ├── QueryNode_ValidateToken
-        │   └── QueryNode_DoesIndexEmpty
-        ├── validateQueryNotDisk
-        └── QueryNode_CheckIsValid            [each child]
+└── QAST_CheckIsValid                         [query_eval_ffi]
+    ├── wrap QueryAST root, IndexSpec and QueryError
+    └── query_eval::check_is_valid
+        ├── search_disk::is_enabled_for_validation
+        ├── ordinary-index fast path
+        └── validation::check_node            [recursive Rust walk]
+            ├── check_allow_slop_and_inorder
+            ├── validate_token
+            │   └── does_index_empty
+            ├── validate_query_not_disk
+            └── check_node                    [each child]
```

### Key types and signatures

```rust
// c_wrappers/field_spec
impl FieldSpec {
    pub fn options(&self) -> FieldSpecOptions;
    pub fn is_indexable_text(&self) -> bool;
    pub fn indexes_empty(&self) -> bool;
    pub fn has_undefined_order(&self) -> bool;
    pub fn field_mask(&self) -> FieldMask;
}

// c_wrappers/index_spec
impl IndexSpec {
    pub fn is_json(&self) -> bool;
    pub fn has_non_empty_fields(&self) -> bool;
    pub fn has_undefined_order(&self) -> bool;
}

// c_wrappers/search_disk
pub fn is_enabled_for_validation() -> bool;

// c_wrappers/query (revised existing variant)
pub enum QueryNode<'a> {
    Tag { fs: Option<&'a ffi::FieldSpec> },
    // existing variants unchanged
}

// query_eval public Rust boundary
pub fn check_is_valid(
    root: QueryNodeRef,
    spec: &IndexSpec,
    opts: &mut ffi::RSSearchOptions,
    status: &mut QueryError,
    validation_flags: QASTValidationFlagsSet,
) -> bool;

// query_eval private traversal
fn check_node(
    node: QueryNodeRef,
    spec: &IndexSpec,
    opts: &mut ffi::RSSearchOptions,
    status: &mut QueryError,
    validation_flags: QASTValidationFlagsSet,
) -> bool;

fn check_allow_slop_and_inorder(
    node: &QueryNodeRef,
    spec: &IndexSpec,
    at_top_level: bool,
    status: &mut QueryError,
) -> bool;

fn does_index_empty(
    node: &QueryNodeRef,
    spec: &IndexSpec,
    opts: &ffi::RSSearchOptions,
) -> bool;

fn validate_token(
    token: RSTokenRef<'_>,
    node: &QueryNodeRef,
    spec: &IndexSpec,
    opts: &ffi::RSSearchOptions,
    status: &mut QueryError,
) -> bool;

fn validate_query_not_disk(query_type: &str, status: &mut QueryError) -> bool;

// QueryNodeRef maps the nullable TAG field pointer to Option before validation. At the numeric
// diagnostic use site, call FieldSpec::from_raw directly and constrain its caller-selected
// lifetime to the QueryNodeRef borrow. The numeric-node contract guarantees that this non-null
// pointer names a matching live schema field for that entire borrow.

// query_eval_ffi: exact replacement for the C symbol
#[unsafe(no_mangle)]
pub unsafe extern "C" fn QAST_CheckIsValid(
    qast: *mut ffi::QueryAST,
    spec: *mut ffi::IndexSpec,
    opts: *mut ffi::RSSearchOptions,
    status: *mut ffi::QueryError,
) -> i32;
```

### Validation pseudocode

```rust
fn check_is_valid(root, spec, opts, status, flags) -> bool {
    let disk_validation = search_disk::is_enabled_for_validation();
    if !disk_validation
        && !spec.has_non_empty_fields()
        && (!spec.is_json() || !spec.has_undefined_order())
    {
        return true;
    }
    check_node(root, spec, opts, status, flags)
}

fn check_node(node, spec, opts, status, flags) -> bool {
    let effective = if node is Vector
        && node.opts().flags contains HybridVectorSubqueryNode
    {
        flags without (NoWeight | NoVector)
    } else {
        flags
    };

    if effective contains NoWeight && node.opts().explicit_weight {
        status.set_code(WeightNotAllowed);
        return false;
    }

    let recurse = match node.as_enum() {
        Phrase { .. } => {
            if spec.is_json() && spec.has_undefined_order() {
                let top = opts.slop >= 0 || opts.flags contains Search_InOrder;
                if !check_allow_slop_and_inorder(node, spec, top, status) {
                    return false;
                }
            }
            true
        }
        Null | Missing { .. } => false,
        Tag { fs } => {
            opts.flags |= QueryNode_IsTag;
            if fs.is_some_and(indexes_empty) { opts.flags |= QueryNode_IndexesEmpty; }
            for child in node.children() {
                if child is Prefix
                    && !validate_query_not_disk("TAG prefix/suffix/infix", status)
                {
                    return false;
                }
                if child is WildcardQuery
                    && !validate_query_not_disk("TAG wildcard", status)
                {
                    return false;
                }
            }
            true
        }
        Token { tok } => {
            if spec.has_non_empty_fields() {
                let _ = validate_token(tok, node, spec, opts, status); // deliberately ignored
            }
            true
        }
        Numeric { nf } if nf.min > nf.max => {
            status.set_with_user_data(
                Syntax,
                "Invalid numeric range (min > max)",
                format!(": @{}:[{:.6} {:.6}]", field_name(nf), nf.min, nf.max),
            );
            return false;
        }
        Vector { .. } if effective contains NoVector => {
            status.set_code(VectorNotAllowed);
            return false;
        }
        _ => true,
    };

    if recurse {
        for child in node.children() {
            // Pass the original, not effective, flags.
            if !check_node(child, spec, opts, status, flags) {
                return false;
            }
        }
    }
    true
}
```

### FFI pseudocode

```rust
unsafe extern "C" fn QAST_CheckIsValid(qast, spec, opts, status) -> i32 {
    require_non_null(qast, spec, opts, status);
    require_non_null((*qast).root);

    let root = QueryNodeRef::new(NonNull::new_unchecked((*qast).root));
    let spec = IndexSpec::from_raw(spec);
    let opts = &mut *opts;
    let status = QueryError::from_opaque_mut_ptr(
        status.cast::<query_error::opaque::OpaqueQueryError>(),
    )
    .expect("status is null");

    if query_eval::check_is_valid(root, spec, opts, status, (*qast).validationFlags) {
        REDISMODULE_OK
    } else {
        REDISMODULE_ERR
    }
}
```

### Test design

```diff
+ Rust wrapper tests
+ ├── FieldSpec option/type predicates and field-mask bit
+ ├── IndexSpec JSON/cached validation flags
+ ├── QueryNodeRef exposes both null and non-null TAG fields without dereferencing null
+ └── search-disk accessor delegates to the existing global validation state

+ Rust query_eval validation tests
+ ├── ordinary-index fast-path bypass
+ ├── explicit weight and nested/main hybrid vector restrictions
+ ├── numeric min > max public/private error split
+ ├── undefined-order slop/in-order field-mask selection
+ ├── TEXT empty-token INDEXEMPTY and no-indexable-TEXT compatibility cases
+ ├── empty token sets Syntax while validation returns success
+ ├── TAG context mutates options and controls empty-token acceptance
+ ├── null-field TAG sets TAG context, skips INDEXEMPTY, and still validates children
+ ├── Flex TAG prefix/wildcard rejection and error text
+ └── null/missing recursion stop and first-failure traversal order

  Existing C++/flow coverage retained
  ├── tests/cpptests/test_cpp_query_validation.cpp (all QueryValidationTest cases)
  ├── tests/cpptests/test_cpp_parse_hybrid.cpp hybrid validation cases
  ├── tests/pytests/test_empty.py and test_highlight.py empty-token behavior
  ├── tests/pytests/test_issues.py and test_json_multi_text.py ordering behavior
  ├── tests/pytests/test.py and test_dialect.py numeric-range behavior
  └── tests/pytests/test_flex_validation.py Flex restrictions
```

Implementation verification runs, in order:

```text
make generate-rust-headers
make fmt CHECK=1
cargo nextest run --manifest-path src/redisearch_rs/Cargo.toml -p field_spec -p index_spec -p query -p search_disk -p query_eval
./build.sh RUN_UNIT_TESTS TEST=QueryValidationTest
./build.sh RUN_UNIT_TESTS TEST=test_cpp_parse_hybrid
./build.sh RUN_PYTEST TEST=tests/pytests/test_empty.py
./build.sh RUN_PYTEST TEST=tests/pytests/test_flex_validation.py
make lint
```
