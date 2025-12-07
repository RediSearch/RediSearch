/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use crate::RSValueTrait;
use std::sync::Arc;

/// Mock implementation of `RSValue` for testing purposes.
#[derive(Clone, Debug, PartialEq)]
pub struct RSValueMock(Arc<RSValueMockInner>);

#[derive(Debug, PartialEq)]
enum RSValueMockInner {
    Null,
    Number(f64),
    String(String),
    Reference(Box<RSValueMock>),
}

impl RSValueMock {
    pub fn strong_count(&self) -> usize {
        Arc::strong_count(&self.0)
    }
}

impl RSValueTrait for RSValueMock {
    fn create_null() -> Self {
        Self(Arc::new(RSValueMockInner::Null))
    }

    fn create_string(s: String) -> Self {
        Self(Arc::new(RSValueMockInner::String(s)))
    }

    fn create_num(num: f64) -> Self {
        Self(Arc::new(RSValueMockInner::Number(num)))
    }

    fn create_ref(value: Self) -> Self {
        Self(Arc::new(RSValueMockInner::Reference(Box::new(value))))
    }

    fn is_null(&self) -> bool {
        matches!(self.0.as_ref(), RSValueMockInner::Null)
    }

    fn get_ref(&self) -> Option<&Self> {
        if let RSValueMockInner::Reference(boxed) = self.0.as_ref() {
            Some(boxed)
        } else {
            None
        }
    }

    fn as_str(&self) -> Option<&str> {
        if let RSValueMockInner::String(s) = self.0.as_ref() {
            Some(s)
        } else {
            None
        }
    }

    fn as_num(&self) -> Option<f64> {
        if let RSValueMockInner::Number(num) = self.0.as_ref() {
            Some(*num)
        } else {
            None
        }
    }

    fn get_type(&self) -> ffi::RSValueType {
        // Mock implementation, return a dummy type
        match self.0.as_ref() {
            RSValueMockInner::Null => ffi::RSValueType_RSValueType_Null,
            RSValueMockInner::Number(_) => ffi::RSValueType_RSValueType_Number,
            RSValueMockInner::String(_) => ffi::RSValueType_RSValueType_String,
            RSValueMockInner::Reference(reference) => reference.get_type(),
        }
    }

    fn is_ptr_type() -> bool {
        // Mock implementation, return false
        false
    }

    fn refcount(&self) -> Option<usize> {
        Some(Arc::strong_count(&self.0))
    }
}
