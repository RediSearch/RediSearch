# Porting plan: QueryNode parameter evaluation

## Architecture

### Problem and scope

`QueryNode_EvalParams` and `QueryNode_EvalParamsCommon` are the remaining query-AST parameter
evaluation routines in `src/query.c`. They dispatch by node type, invoke the existing C parameter
resolvers, mutate resolved targets in the AST, and recursively visit children. This orchestration
belongs in the existing `query_eval` Rust crate, which already owns mutable query-node traversal.

This port preserves behavior and the two C ABI symbols. It does **not** port `QueryParam_Resolve`,
`VectorQuery_EvalParams`, parameter-dictionary storage, vector-query parameter storage,
`QAST_EvalParams`, or query parsing. Those remain C dependencies called through `ffi`.

The work fits one pull request. It is a local C-to-Rust port with no prerequisite C reshaping: the
existing `RSQueryNode` layout and an extended `QueryNodeMut` exclusivity contract provide the
required boundary. No command, option, persisted data, or user-visible behavior changes, so the
repository's large-change spec workflow does not apply.

### User-visible surface

There is no new surface. `FT.SEARCH`, `FT.AGGREGATE`, vector queries, and the module's special-case
query parser continue to accept and reject parameters exactly as before. C callers continue to call:

```c
int QueryNode_EvalParams(dict *, QueryNode *, unsigned int, QueryError *);
int QueryNode_EvalParamsCommon(dict *, QueryNode *, unsigned int, QueryError *);
```

Both still return `REDISMODULE_OK` or `REDISMODULE_ERR`, report details through `QueryError`, and may
leave earlier parameters/nodes resolved when a later resolution fails.

### Subsystems touched

- **`query_eval`** owns node-type dispatch, depth-first traversal, short-circuiting, and the common
  resolver loop.
- **`c_wrappers/query`** exposes mutable access to a node's parameter array and options flags without
  exposing the full `RSQueryNode` layout to `query_eval`.
- **`ffi`** binds the retained C resolvers `QueryParam_Resolve` and `VectorQuery_EvalParams`.
- **`ffi`** also binds `Param_DictCreate`, `Param_DictAdd`, and `Param_DictFree` so evaluator tests
  can construct the real dictionary consumed by those resolvers.
- **`query_eval_ffi`** preserves the two existing C symbols and maps Rust success/failure to Redis
  module status codes.
- **C query code and headers** lose the two implementations and their handwritten declarations;
  callers use the generated `query_eval_ffi.h` declarations.
- **Tests** retain the direct C++ characterization coverage added before this plan and add focused
  Rust wrapper/evaluator coverage where it can exercise the new Rust-owned decisions.

### Data and ownership model

No persistent or independently owned data is introduced.

- `dict`, `QueryError`, `RSQueryNode`, `Param`, and vector-query storage remain C-owned.
- The FFI entrypoint establishes one exclusive `QueryNodeMut` for the supplied subtree. Recursive
  child views are reborrows, so parent and sibling mutable views cannot coexist.
- `QueryNodeMut::params_mut` returns a mutable borrowed slice over the C `array_*` allocation, with
  length obtained from `array_len_func`; a null pointer is represented as an empty slice. Its safe
  signature is justified by extending `QueryNodeMut::new`'s contract: exclusivity covers each
  node's separately allocated parameter array, so no other pointer, reference, or handle may access
  that array while the wrapper or a reborrow from it is live.
- `QueryNodeMut::set_verbatim` mutates only `opts.flags`, using the canonical
  `QueryNodeFlags::Verbatim` value from `query_types`.
- `MockQueryNode::set_params` builds and owns an `array.h` allocation containing supplied `Param`
  values, replacing and freeing any previous mock parameter array; production node ownership is
  unchanged.
- Calls to the retained C resolvers occur while Rust exclusively owns the affected subtree. They may
  mutate a `Param`, its target elsewhere in the node payload, vector-query storage, and `QueryError`.
  Every resolver target and vector payload allocation that may be written must be valid and
  writable, and may not be accessed through any other live reference, pointer, or handle during the
  call. Rust keeps no references into those targets across a resolver call.
- Resolver return `2` remains the only signal that common evaluation marks a node verbatim. Any
  negative resolver result becomes the Rust error outcome; other non-negative results succeed.
- `VectorQuery_EvalParams` has a different status contract: `REDISMODULE_OK` is success and every
  other result, including `REDISMODULE_ERR` (`1`), becomes the Rust error outcome.
- The Rust evaluator functions are unsafe because the type system cannot validate the C-owned
  dictionary, error, parameter-target, and vector payload pointers dereferenced by the retained C
  resolvers. Their safety contracts carry those preconditions through every recursive call.

### Behavior and edge cases

1. Vector nodes call `VectorQuery_EvalParams`; all ordinary parameter-bearing node types call the
   common Rust resolver; union nodes resolve nothing locally.
2. Union's `params == NULL` condition remains a debug invariant rather than new runtime validation.
3. Null and missing nodes neither resolve local parameters nor traverse children, preserving the
   current handling even for a malformed node that has children.
4. Every other valid node traverses children in array order after successful local resolution.
5. The first local or descendant failure returns immediately. Later parameters and siblings remain
   untouched; earlier mutations are not rolled back.
6. A common parameter that resolves as numeric marks the node verbatim immediately. The flag remains
   set if a later parameter fails.
7. Vector evaluation resolves both the node's common parameters and vector-query parameters inside
   the retained C helper before child traversal.
8. Empty parameter and child arrays succeed without resolver calls.
9. `QueryNodeType::Max` remains an unreachable invalid discriminant. The Rust boundary does not add
   recovery for malformed ASTs, null required pointers, cycles, or excessive recursion depth.
10. The generated FFI functions do not unwind into C; their documented safety contract requires
    valid non-null node and status pointers and a parameter dictionary valid for every unresolved
    parameter in the subtree, matching the current C preconditions.

### Validation

- Keep the C++ characterization tests as ABI-level regression tests:
  `testEvalParamsCommonMarksNumericTermVerbatim`, `testEvalParamsTraversesAllUnionChildren`, and
  `testEvalParamsStopsTraversalAfterChildError`.
- Add wrapper tests for null/non-null parameter arrays, mutable parameter access, and setting the
  verbatim flag.
- Add Rust evaluator tests for node dispatch and traversal boundaries that do not duplicate C
  resolver internals; use the real retained C resolver for success, numeric, and missing-parameter
  outcomes. Include a vector-resolution failure that returns `REDISMODULE_ERR` (`1`) whose child has
  an independently resolvable parameter, and assert that the child remains untouched. Bind
  `Param_DictCreate`, `Param_DictAdd`, and `Param_DictFree` to manage the real test dictionary, and
  use a mock-node parameter-array setter to attach `Param` fixtures to the AST.
- Run the focused C++ query tests, `cargo nextest` for `query` and `query_eval`, header generation,
  formatting/lint, then the full build and relevant parameter Python tests named in the analysis.

## Alternatives rejected

### Port `QueryParam_Resolve` and vector parameter resolution in the same change

Rejected because those routines own parsing, allocation, dictionaries, and vector-specific data,
not traversal. Porting them widens the ownership boundary and review surface without being required
to move the two requested functions. They can be ported independently after this orchestration move.

### Keep a C traversal that calls a Rust per-node common resolver

Rejected because traversal and node-type dispatch are most of `QueryNode_EvalParams`, and
`QueryNodeMut` already models exclusive recursive traversal. Leaving orchestration in C would not
complete the requested port and would add an unnecessary C↔Rust call per node.

### Expose `RSQueryNode` fields directly throughout `query_eval`

Rejected because raw field access would duplicate array-length and aliasing safety at every call
site. Two narrow `QueryNodeMut` accessors keep the unsafe C-layout knowledge in the wrapper crate.

### Add a new parameter-dictionary or resolver abstraction

Rejected because there is one production implementation of each retained resolver. A trait or
callback layer would exist only to mock this port and would obscure the actual FFI behavior.

### Change callers to new Rust-only names

Rejected because `QueryNode_EvalParamsCommon` has a direct caller in `src/module.c`, tests call both
symbols, and preserving names lets this remain an implementation-only port. Generated declarations
replace handwritten declarations without changing consumers.

## Program design

### File-tree diff

```diff
 MODIFIED src/query.c
           remove QueryNode_EvalParams and QueryNode_EvalParamsCommon implementations
 MODIFIED src/query.h
           remove handwritten QueryNode_EvalParams declaration
 MODIFIED src/query_node.h
           remove handwritten QueryNode_EvalParamsCommon declaration
 MODIFIED src/module.c
           include query_eval_ffi.h for the common-resolver call
 MODIFIED src/redisearch_rs/ffi/build.rs
           bind retained QueryParam_Resolve and VectorQuery_EvalParams C functions plus
           Param_DictCreate, Param_DictAdd, and Param_DictFree test-fixture helpers
 MODIFIED src/redisearch_rs/c_wrappers/query/src/query_node_ref.rs
           expose mutable parameters and verbatim flag mutation
 MODIFIED src/redisearch_rs/c_wrappers/query/src/mock/query_node_ref.rs
           add an owning parameter-array setter for evaluator fixtures
 MODIFIED src/redisearch_rs/c_wrappers/query/tests/integration/query_node_ref/mod.rs
           cover the new wrapper accessors
 NEW      src/redisearch_rs/query_eval/src/params.rs
           own parameter dispatch, common resolution, and recursive traversal
 MODIFIED src/redisearch_rs/query_eval/src/lib.rs
           register and export parameter evaluation
 NEW      src/redisearch_rs/query_eval/tests/integration/params.rs
           cover Rust-owned dispatch, traversal, short-circuit, and numeric behavior
 MODIFIED src/redisearch_rs/query_eval/tests/integration/main.rs
           register parameter-evaluation integration tests
 MODIFIED src/redisearch_rs/c_entrypoint/query_eval_ffi/Cargo.toml
           add the direct redis-module dependency used by the FFI status mapping
 MODIFIED src/redisearch_rs/c_entrypoint/query_eval_ffi/src/lib.rs
           export the two compatibility entrypoints
 MODIFIED src/redisearch_rs/headers/query_eval_ffi.h
           regenerate declarations from query_eval_ffi
```

`query_eval` already has the dependencies needed for the evaluator. Add
`redis-module.workspace = true` to `query_eval_ffi`, because Rust crates cannot use the
`REDISMODULE_OK` and `REDISMODULE_ERR` constants through the transitive `query_eval` dependency.

### Key types and signatures

```rust
// c_wrappers/query/src/query_node_ref.rs
impl QueryNodeMut<'_> {
    /// # Safety (extension to the existing `new` contract)
    /// Exclusive access covers every node's separately allocated parameter array. No other live
    /// reference, pointer, or handle may access those arrays while this view or a reborrow from it
    /// is live.
    pub const unsafe fn new(ptr: NonNull<ffi::RSQueryNode>) -> Self;

    pub fn params_mut(&mut self) -> &mut [ffi::Param];
    pub fn set_verbatim(&mut self);
}

// query_eval/src/params.rs
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ParamEvaluationError;

/// # Safety
/// `params` must be a valid parameter dictionary for every unresolved parameter reachable from
/// `node`; `status` must be valid for writes by the retained C resolvers; and all parameter targets
/// and vector payload pointers reachable from `node` must remain valid and writable for the call.
/// The subtree represented by `node`, its parameter arrays, and every resolver target or vector
/// payload allocation that may be written must be exclusively borrowed, with no other live
/// reference, pointer, or handle accessing them.
pub unsafe fn eval_params(
    params: *mut ffi::dict,
    node: QueryNodeMut<'_>,
    dialect_version: u32,
    status: *mut ffi::QueryError,
) -> Result<(), ParamEvaluationError>;

/// # Safety
/// `params` must be a valid parameter dictionary for every unresolved parameter on `node`;
/// `status` must be valid for writes by `QueryParam_Resolve`; and every target referenced by the
/// node's `Param` array must remain valid and writable for the call. `node`, its parameter array,
/// and every resolver target that may be written must be exclusively borrowed, with no other live
/// reference, pointer, or handle accessing them.
pub unsafe fn eval_params_common(
    params: *mut ffi::dict,
    node: &mut QueryNodeMut<'_>,
    dialect_version: u32,
    status: *mut ffi::QueryError,
) -> Result<(), ParamEvaluationError>;

// c_entrypoint/query_eval_ffi/src/lib.rs
/// # Safety
/// In addition to valid required pointers, the caller grants exclusive access for the duration of
/// the call to the node subtree, every node parameter array, and every resolver target or vector
/// payload allocation that may be written; no other live reference, pointer, or handle may access
/// them.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn QueryNode_EvalParams(
    params: *mut ffi::dict,
    node: *mut ffi::RSQueryNode,
    dialect_version: u32,
    status: *mut ffi::QueryError,
) -> i32;

/// # Safety
/// In addition to valid required pointers, the caller grants exclusive access for the duration of
/// the call to the node, its parameter array, and every resolver target that may be written; no
/// other live reference, pointer, or handle may access them.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn QueryNode_EvalParamsCommon(
    params: *mut ffi::dict,
    node: *mut ffi::RSQueryNode,
    dialect_version: u32,
    status: *mut ffi::QueryError,
) -> i32;
```

`ParamEvaluationError` carries no duplicate message: the retained C resolver has already populated
`QueryError`. Its purpose is to prevent Redis module integer status codes from leaking into the
Rust orchestration API.

### Orchestration diff

```diff
 QAST_EvalParams [C, unchanged]
-└─ QueryNode_EvalParams [C]
-   ├─ switch node->type
-   │  ├─ VectorQuery_EvalParams [C]
-   │  └─ QueryNode_EvalParamsCommon [C]
-   │     └─ QueryParam_Resolve [C] × params
-   └─ QueryNode_EvalParams [C] × children, stop on error
+└─ QueryNode_EvalParams [Rust FFI]
+   └─ query_eval::eval_params [Rust]
+      ├─ dispatch QueryNodeType
+      │  ├─ VectorQuery_EvalParams [retained C FFI]
+      │  ├─ query_eval::eval_params_common [Rust]
+      │  │  └─ QueryParam_Resolve [retained C FFI] × params
+      │  ├─ Union: debug-check no local params
+      │  └─ Null | Missing: return success without children
+      └─ query_eval::eval_params [Rust] × children, stop on first error

 module special-case parser [C]
-└─ QueryNode_EvalParamsCommon [C]
+└─ QueryNode_EvalParamsCommon [Rust FFI]
+   └─ query_eval::eval_params_common [Rust]
+      └─ QueryParam_Resolve [retained C FFI] × params
```

### Core pseudocode

```rust
unsafe fn eval_params(params, mut node, dialect, status) -> Result<(), ParamEvaluationError> {
    let traverse_children = match node.node_type() {
        Vector => {
            let result = ffi::VectorQuery_EvalParams(
                params,
                node.as_non_null().as_ptr(),
                dialect,
                status,
            );
            if result != REDISMODULE_OK {
                return Err(ParamEvaluationError);
            }
            true
        }
        Geo | Token | Numeric | Tag | Phrase | Not | Prefix | Fuzzy | Optional
        | Ids | Wildcard | WildcardQuery | Geometry => {
            eval_params_common(params, &mut node, dialect, status)?;
            true
        }
        Union => {
            debug_assert!(node.params_mut().is_empty());
            true
        }
        Null | Missing => false,
        Max => unreachable!(),
    };

    if traverse_children {
        for index in 0..node.num_children() {
            eval_params(params, node.child_mut(index), dialect, status)?;
        }
    }
    Ok(())
}

unsafe fn eval_params_common(params, node, dialect, status) -> Result<(), ParamEvaluationError> {
    for param in node.params_mut() {
        match unsafe { ffi::QueryParam_Resolve(param, params, dialect, status) } {
            result if result < 0 => return Err(ParamEvaluationError),
            2 => node.set_verbatim(),
            _ => {}
        }
    }
    Ok(())
}

unsafe extern "C" fn QueryNode_EvalParams(...) -> i32 {
    validate required pointers according to the documented unsafe contract;
    let node = QueryNodeMut::new(non_null_node);
    match eval_params(params, node, dialect, status) {
        Ok(()) => REDISMODULE_OK,
        Err(_) => REDISMODULE_ERR,
    }
}
```

The implementation must structure the common loop so the mutable parameter borrow ends before
`set_verbatim` reborrows the node (for example, index one parameter at a time). It must not retain a
slice/reference into the node while calling `VectorQuery_EvalParams`, which receives the whole node.
