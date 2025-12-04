/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use ffi::{NewQueryTerm, RSQueryTerm, RSToken};

/// A builder for creating `QueryTerm` instances.
///
/// Use [`QueryTermBuilder::allocate`] to create a new instance
/// on the heap.
pub(crate) struct QueryTermBuilder<'a> {
    pub(crate) token: &'a str,
    pub(crate) idf: f64,
    pub(crate) id: i32,
    pub(crate) flags: u32,
    pub(crate) bm25_idf: f64,
}

impl<'a> QueryTermBuilder<'a> {
    /// Creates a new instance of `RSQueryTerm` on the heap.
    /// It returns a raw pointer to the allocated `RSQueryTerm`.
    ///
    /// The caller is responsible for freeing the allocated memory
    /// using [`Term_Free`](ffi::Term_Free).
    pub(crate) fn allocate(self) -> *mut RSQueryTerm {
        let Self {
            token,
            idf,
            id,
            flags,
            bm25_idf,
        } = self;
        let token = RSToken {
            str_: token.as_ptr() as *mut _,
            len: token.len(),
            _bitfield_align_1: Default::default(),
            _bitfield_1: Default::default(),
            __bindgen_padding_0: Default::default(),
        };
        let token_ptr = Box::into_raw(Box::new(token));
        let query_term = unsafe { NewQueryTerm(token_ptr as *mut _, id) };

        // Now that NewQueryTerm copied tok->str into ret->str,
        // the temporary token struct is no longer needed.
        unsafe {
            drop(Box::from_raw(token_ptr));
        }

        // Patch the fields we can't set via the constructor
        unsafe { (*query_term).idf = idf };
        unsafe { (*query_term).bm25_idf = bm25_idf };
        unsafe { (*query_term).flags = flags };

        query_term
    }
}
