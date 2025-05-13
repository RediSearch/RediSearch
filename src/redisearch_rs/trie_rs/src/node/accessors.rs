/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use super::Node;
use super::metadata::{NodeHeader, PtrMetadata};

/// Accessor methods.
impl<Data> Node<Data> {
    /// Returns a reference to the header for this node.
    #[inline]
    pub(super) fn header(&self) -> &NodeHeader {
        // SAFETY:
        // - The header field is dereferenceable thanks to invariant 2. in [`Self::ptr`]'s documentation.
        unsafe { self.ptr.as_ref() }
    }

    /// Returns the layout and field offsets for the allocated buffer backing this node.
    #[inline]
    pub(super) fn metadata(&self) -> PtrMetadata<Data> {
        self.header().metadata()
    }

    /// Returns the length of the label associated with this node.
    #[inline]
    pub fn label_len(&self) -> u16 {
        self.header().label_len
    }

    /// Returns the number of children for this node.
    #[inline]
    pub fn n_children(&self) -> u8 {
        self.header().n_children
    }

    /// Returns a reference to the label associated with this node.
    #[inline]
    pub fn label(&self) -> &[u8] {
        // SAFETY:
        // - The layout satisfies the requirements thanks to invariant 1. in [`Self::ptr`]'s documentation.
        let label_ptr = unsafe { PtrMetadata::<Data>::label_ptr(self.ptr) };
        // SAFETY:
        // - The label field is dereferenceable thanks to invariant 2. in [`Self::ptr`]'s documentation.
        // - The length is correct thanks to invariant 1. in [`Self::ptr`]'s documentation.
        unsafe { std::slice::from_raw_parts(label_ptr.as_ptr(), self.label_len() as usize) }
    }

    /// Returns a mutable reference to the data associated with this node, if any.
    #[inline]
    pub fn data_mut(&mut self) -> &mut Option<Data> {
        // SAFETY:
        // - The layout satisfies the requirements thanks to invariant 1. in [`Self::ptr`]'s documentation.
        let mut data_ptr = unsafe { self.metadata().value_ptr(self.ptr) };
        // SAFETY:
        // - The data field is dereferenceable thanks to invariant 2. in [`Self::ptr`]'s documentation.
        // - We have exclusive access to the data field since this method takes a mutable reference to `self`.
        unsafe { data_ptr.as_mut() }
    }

    /// Returns a reference to the data associated with this node, if any.
    #[inline]
    pub fn data(&self) -> Option<&Data> {
        // SAFETY:
        // - The layout satisfies the requirements thanks to invariant 1. in [`Self::ptr`]'s documentation.
        let data_ptr = unsafe { self.metadata().value_ptr(self.ptr) };
        // SAFETY:
        // - The data field is dereferenceable thanks to invariant 2. in [`Self::ptr`]'s documentation.
        let data: &Option<Data> = unsafe { data_ptr.as_ref() };
        data.as_ref()
    }

    /// Returns a reference to the children of this node.
    ///
    /// # Invariants
    ///
    /// The index of a child in this array matches the index of its first byte
    /// in the array returned by [`Self::children_first_bytes`].
    #[inline]
    pub fn children(&self) -> &[Node<Data>] {
        // SAFETY:
        // - The layout satisfies the requirements thanks to invariant 1. in [`Self::ptr`]'s documentation.
        let children_ptr = unsafe { self.metadata().children_ptr(self.ptr) };
        // SAFETY:
        // - The children field is dereferenceable thanks to invariant 2. in [`Self::ptr`]'s documentation.
        // - The length is correct thanks to invariant 1. in [`Self::ptr`]'s documentation.
        unsafe { std::slice::from_raw_parts(children_ptr.as_ptr(), self.n_children() as usize) }
    }

    /// Returns a mutable reference to the children of this node.
    ///
    /// # Invariants
    ///
    /// The index of a child in this array matches the index of its first byte
    /// in the array returned by [`Self::children_first_bytes`].
    #[inline]
    pub fn children_mut(&mut self) -> &mut [Node<Data>] {
        // SAFETY:
        // - The layout satisfies the requirements thanks to invariant 1. in [`Self::ptr`]'s documentation.
        let children_ptr = unsafe { self.metadata().children_ptr(self.ptr) };
        // SAFETY:
        // - The children field is dereferenceable thanks to invariant 2. in [`Self::ptr`]'s documentation.
        // - The length is correct thanks to invariant 1. in [`Self::ptr`]'s documentation.
        // - We have exclusive access to the children field since this method takes a mutable reference to `self`.
        unsafe { std::slice::from_raw_parts_mut(children_ptr.as_ptr(), self.n_children() as usize) }
    }

    /// Returns a reference to the array containing the first byte of
    /// each child of this node.
    ///
    /// # Invariants
    ///
    /// The index of a byte in this array matches the index of the child
    /// it belongs to in the array returned by [`Self::children`].
    #[inline]
    pub fn children_first_bytes(&self) -> &[u8] {
        // SAFETY:
        // - The layout satisfies the requirements thanks to invariant 1. in [`Self::ptr`]'s documentation.
        let ptr = unsafe { self.metadata().child_first_bytes_ptr(self.ptr) };
        // SAFETY:
        // - The field is dereferenceable thanks to invariant 2. in [`Self::ptr`]'s documentation.
        // - The length is correct thanks to invariant 1. in [`Self::ptr`]'s documentation.
        unsafe { std::slice::from_raw_parts(ptr.as_ptr(), self.n_children() as usize) }
    }
}
