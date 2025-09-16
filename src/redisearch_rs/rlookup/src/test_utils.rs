/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::ffi::CString;

use crate::{RLookupKey, RLookupKeyFlags};
use sorting_vector::RSSortingVector;
use value::RSValueTrait;

#[derive(Clone, Debug, PartialEq)]
pub struct MockRSValue {
    data: String,
}

impl MockRSValue {
    pub fn new(data: &str) -> Self {
        Self {
            data: data.to_string(),
        }
    }
}

impl RSValueTrait for MockRSValue {
    fn create_null() -> Self {
        Self {
            data: String::new(),
        }
    }

    fn create_string(s: String) -> Self {
        Self { data: s }
    }

    fn create_num(num: f64) -> Self {
        Self {
            data: num.to_string(),
        }
    }

    fn create_ref(value: Self) -> Self {
        value
    }

    fn is_null(&self) -> bool {
        self.data.is_empty()
    }

    fn get_ref(&self) -> Option<&Self> {
        None
    }

    fn get_ref_mut(&mut self) -> Option<&mut Self> {
        None
    }

    fn as_str(&self) -> Option<&str> {
        Some(&self.data)
    }

    fn as_num(&self) -> Option<f64> {
        self.data.parse().ok()
    }

    fn get_type(&self) -> ffi::RSValueType {
        ffi::RSValueType_RSValue_String
    }

    fn is_ptr_type() -> bool {
        false
    }

    fn increment(&mut self) {
        // Mock implementation - no-op
    }

    fn decrement(&mut self) {
        // Mock implementation - no-op
    }
}

pub fn create_sorting_vector(values: Vec<MockRSValue>) -> RSSortingVector<MockRSValue> {
    let mut sv = RSSortingVector::new(values.len());
    for (i, value) in values.into_iter().enumerate() {
        sv.try_insert_val(i, value).unwrap();
    }
    sv
}

pub fn create_mock_key(dstidx: u16, svidx: u16, flags: RLookupKeyFlags) -> RLookupKey<'static> {
    let str = format!("mock_key_{}_{}", dstidx, svidx);
    let cstring = CString::new(str).unwrap();
    let mut key = RLookupKey::new_owned(cstring, flags);
    key.dstidx = dstidx;
    key.svidx = svidx;

    key
}
