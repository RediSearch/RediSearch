/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Ports part of the RediSearch RSValue type to Rust. This is a temporary solution until we have a proper
//! Rust port of the RSValue type.

/// A trait that defines the behavior of a RediSearch RSValue.
///
/// The trait is temporary until we have a proper Rust port of the RSValue type.
///
/// This trait is used to create, manipulate, and free RSValue instances. It can be implemented
/// as a mock type for testing purposes, and by a new-type in the ffi layer to interact with the
/// C API.
pub trait RSValueTrait: Clone
where
    Self: Sized,
{
    /// Creates a new RSValue instance which is null
    fn create_null() -> Self;

    /// Creates a new RSValue instance with a string value.
    fn create_string(s: String) -> Self;

    /// Creates a new RSValue instance with a number (double) value.
    fn create_num(num: f64) -> Self;

    /// Creates a new RSValue instance that is a reference to the given value.
    fn create_ref(value: Self) -> Self;

    /// Checks if the RSValue is null
    fn is_null(&self) -> bool;

    /// gets a reference to the RSValue instance, if it is a reference type or None.
    fn get_ref(&self) -> Option<&Self>;

    /// gets a mutable reference to the RSValue instance, if it is a reference type or None.
    fn get_ref_mut(&mut self) -> Option<&mut Self>;

    /// gets the string slice of the RSValue instance, if it is a string type or None otherwise.
    fn as_str(&self) -> Option<&str>;

    /// gets the number (double) value of the RSValue instance, if it is a number type or None otherwise.
    fn as_num(&self) -> Option<f64>;

    /// gets the type of the RSValue instance, it either null, string, number, or reference.
    fn get_type(&self) -> ffi::RSValueType;

    /// returns true if the RSValue is stored as a pointer on the heap (the C implementation)
    fn is_ptr_type() -> bool;

    /// Increments the reference count of the RSValue instance.
    fn increment(&mut self);

    /// Decrements the reference count of the RSValue instance.
    fn decrement(&mut self);

    /// returns the approximate memory size of the RSValue instance.
    fn mem_size() -> usize {
        std::mem::size_of::<Self>()
    }
}
