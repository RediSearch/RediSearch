/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#![cfg(not(miri))]

use std::{ffi::c_char, ptr::NonNull};

use query::{QueryNodeMut, mock::MockQueryNode};
use query_error::QueryError;
use query_eval::{eval_params, eval_params_common};
use query_types::{QueryNodeFlags, QueryNodeType};

struct ParamDict(NonNull<ffi::dict>);

impl ParamDict {
    fn new() -> Self {
        redis_mock::init_redis_module_mock();
        // SAFETY: creates a new owned parameter dictionary.
        let ptr = unsafe { ffi::Param_DictCreate() };
        Self(NonNull::new(ptr).expect("parameter dictionary allocation failed"))
    }

    fn add(&mut self, name: &std::ffi::CStr, value: &std::ffi::CStr) {
        let mut status = QueryError::default();
        // SAFETY: the dictionary is valid, both strings are valid for the call,
        // and `status` has the same C layout as `ffi::QueryError`.
        let result = unsafe {
            ffi::Param_DictAdd(
                self.0.as_ptr(),
                name.as_ptr(),
                value.as_ptr(),
                value.to_bytes().len(),
                status_ptr(&mut status),
            )
        };
        assert_eq!(result, 0);
    }

    fn as_ptr(&self) -> *mut ffi::dict {
        self.0.as_ptr()
    }
}

impl Drop for ParamDict {
    fn drop(&mut self) {
        // SAFETY: this is the owned dictionary created in `new`.
        unsafe { ffi::Param_DictFree(self.0.as_ptr()) };
    }
}

fn status_ptr(status: &mut QueryError) -> *mut ffi::QueryError {
    std::ptr::from_mut(status).cast()
}

fn numeric_param(name: &'static std::ffi::CStr, target: &mut f64) -> ffi::Param {
    ffi::Param {
        name: name.as_ptr(),
        len: name.to_bytes().len(),
        type_: ffi::ParamType_PARAM_NUMERIC,
        target: std::ptr::from_mut(target).cast(),
        target_len: std::ptr::null_mut(),
        sign: 1,
    }
}

#[test]
fn common_resolution_marks_a_numeric_term_verbatim() {
    let mut dict = ParamDict::new();
    dict.add(c"value", c"1.25");
    let mut target: *mut c_char = std::ptr::null_mut();
    let param = ffi::Param {
        name: c"value".as_ptr(),
        len: c"value".to_bytes().len(),
        type_: ffi::ParamType_PARAM_TERM,
        target: std::ptr::from_mut(&mut target).cast(),
        target_len: std::ptr::null_mut(),
        sign: 1,
    };
    let mut mock = MockQueryNode::new(QueryNodeType::Token);
    mock.set_params(&[param]);
    let mut status = QueryError::default();
    // SAFETY: the mock exclusively owns the node, parameter array, and target;
    // the dictionary and status are valid for the retained resolver.
    let mut node = unsafe { QueryNodeMut::new(mock.as_non_null()) };

    // SAFETY: the fixtures above satisfy the evaluator's contract.
    unsafe { eval_params_common(dict.as_ptr(), &mut node, 2, status_ptr(&mut status)) }
        .expect("numeric term should resolve");

    assert!(node.opts().flags.contains(QueryNodeFlags::Verbatim));
    assert!(!target.is_null());
    redis_mock::allocator::free_shim(target.cast());
}

#[test]
fn evaluation_traverses_union_children_in_order() {
    let mut dict = ParamDict::new();
    dict.add(c"first", c"1.5");
    dict.add(c"second", c"2.5");
    let (mut first, mut second) = (0.0, 0.0);
    let mut first_node = MockQueryNode::new(QueryNodeType::Numeric);
    first_node.set_params(&[numeric_param(c"first", &mut first)]);
    let mut second_node = MockQueryNode::new(QueryNodeType::Numeric);
    second_node.set_params(&[numeric_param(c"second", &mut second)]);
    let mut root = MockQueryNode::new(QueryNodeType::Union);
    root.set_children(&[first_node.as_ptr(), second_node.as_ptr()]);
    let mut status = QueryError::default();
    // SAFETY: all fixtures are valid, writable, exclusively owned, and outlive
    // evaluation.
    let node = unsafe { QueryNodeMut::new(root.as_non_null()) };

    // SAFETY: the fixtures above satisfy the evaluator's contract.
    unsafe { eval_params(dict.as_ptr(), node, 2, status_ptr(&mut status)) }
        .expect("both children should resolve");

    assert_eq!((first, second), (1.5, 2.5));
}

#[test]
fn evaluation_stops_after_the_first_child_error() {
    let mut dict = ParamDict::new();
    dict.add(c"later", c"9.0");
    let (mut missing, mut later) = (0.0, 7.0);
    let mut first_node = MockQueryNode::new(QueryNodeType::Numeric);
    first_node.set_params(&[numeric_param(c"missing", &mut missing)]);
    let mut second_node = MockQueryNode::new(QueryNodeType::Numeric);
    second_node.set_params(&[numeric_param(c"later", &mut later)]);
    let mut root = MockQueryNode::new(QueryNodeType::Union);
    root.set_children(&[first_node.as_ptr(), second_node.as_ptr()]);
    let mut status = QueryError::default();
    // SAFETY: all fixtures are valid, writable, exclusively owned, and outlive
    // evaluation.
    let node = unsafe { QueryNodeMut::new(root.as_non_null()) };

    // SAFETY: the fixtures above satisfy the evaluator's contract.
    assert!(unsafe { eval_params(dict.as_ptr(), node, 2, status_ptr(&mut status)) }.is_err());

    assert_eq!(later, 7.0);
}

#[test]
fn null_nodes_suppress_child_traversal() {
    let mut dict = ParamDict::new();
    dict.add(c"child", c"3.0");
    let mut target = 0.0;
    let mut child = MockQueryNode::new(QueryNodeType::Numeric);
    child.set_params(&[numeric_param(c"child", &mut target)]);
    let mut root = MockQueryNode::new(QueryNodeType::Null);
    root.set_children(&[child.as_ptr()]);
    let mut status = QueryError::default();
    // SAFETY: all fixtures are valid, writable, exclusively owned, and outlive
    // evaluation.
    let node = unsafe { QueryNodeMut::new(root.as_non_null()) };

    // SAFETY: the fixtures above satisfy the evaluator's contract.
    unsafe { eval_params(dict.as_ptr(), node, 2, status_ptr(&mut status)) }
        .expect("null nodes should succeed without traversing children");

    assert_eq!(target, 0.0);
}

#[test]
fn vector_failure_does_not_traverse_its_child() {
    let mut dict = ParamDict::new();
    dict.add(c"child", c"4.0");
    let (mut missing, mut child_target) = (0.0, 0.0);
    let mut child = MockQueryNode::new(QueryNodeType::Numeric);
    child.set_params(&[numeric_param(c"child", &mut child_target)]);
    // SAFETY: all-zero is a valid empty vector-query fixture.
    let mut vector_query = Box::new(unsafe { std::mem::zeroed::<ffi::VectorQuery>() });
    let mut root = MockQueryNode::new(QueryNodeType::Vector);
    root.set_vector_query(&raw mut *vector_query);
    root.set_params(&[numeric_param(c"missing", &mut missing)]);
    root.set_children(&[child.as_ptr()]);
    let mut status = QueryError::default();
    // SAFETY: all fixtures are valid, writable, exclusively owned, and outlive
    // evaluation.
    let node = unsafe { QueryNodeMut::new(root.as_non_null()) };

    // SAFETY: the fixtures above satisfy the evaluator's contract.
    assert!(unsafe { eval_params(dict.as_ptr(), node, 2, status_ptr(&mut status)) }.is_err());

    assert_eq!(child_target, 0.0);
}
