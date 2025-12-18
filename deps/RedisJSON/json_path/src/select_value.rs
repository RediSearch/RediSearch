/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of (a) the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

use serde::Serialize;
use std::fmt::Debug;

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum SelectValueType {
    Null,
    Bool,
    Long,
    Double,
    String,
    Array,
    Object,
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum ValueRef<'a, T: SelectValue> {
    Borrowed(&'a T),
    Owned(T),
}

impl<'a, T: SelectValue> Serialize for ValueRef<'a, T> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.as_ref().serialize(serializer)
    }
}

impl<'a, T: SelectValue> ValueRef<'a, T> {
    pub fn inner_cloned(&self) -> T {
        match self {
            ValueRef::Borrowed(t) => (*t).clone(),
            ValueRef::Owned(t) => t.clone(),
        }
    }
}

impl<'a, T: SelectValue> AsRef<T> for ValueRef<'a, T> {
    fn as_ref(&self) -> &T {
        match self {
            ValueRef::Borrowed(t) => t,
            ValueRef::Owned(t) => t,
        }
    }
}

impl<'a, T: SelectValue> std::ops::Deref for ValueRef<'a, T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        self.as_ref()
    }
}

impl<'a, T: SelectValue> PartialEq<&T> for ValueRef<'a, T> {
    fn eq(&self, other: &&T) -> bool {
        is_equal(self.as_ref(), *other)
    }
}

#[allow(unused)]
pub trait SelectValue: Debug + Eq + PartialEq + Default + Clone + Serialize {
    fn get_type(&self) -> SelectValueType;
    fn contains_key(&self, key: &str) -> bool;
    fn values<'a>(&'a self) -> Option<Box<dyn Iterator<Item = ValueRef<'a, Self>> + 'a>>;
    fn keys<'a>(&'a self) -> Option<Box<dyn Iterator<Item = &'a str> + 'a>>;
    fn items<'a>(&'a self) -> Option<Box<dyn Iterator<Item = (&'a str, ValueRef<'a, Self>)> + 'a>>;
    fn len(&self) -> Option<usize>;
    fn is_empty(&self) -> Option<bool>;
    fn get_key<'a>(&'a self, key: &str) -> Option<ValueRef<'a, Self>>;
    fn get_index<'a>(&'a self, index: usize) -> Option<ValueRef<'a, Self>>;
    fn is_array(&self) -> bool;
    fn is_double(&self) -> Option<bool>;

    fn get_str(&self) -> String;
    fn as_str(&self) -> &str;
    fn get_bool(&self) -> bool;
    fn get_long(&self) -> i64;
    fn get_double(&self) -> f64;
}

pub fn is_equal<T1: SelectValue, T2: SelectValue>(a: &T1, b: &T2) -> bool {
    a.get_type() == b.get_type()
        && match a.get_type() {
            SelectValueType::Null => true,
            SelectValueType::Bool => a.get_bool() == b.get_bool(),
            SelectValueType::Long => a.get_long() == b.get_long(),
            SelectValueType::Double => a.get_double() == b.get_double(),
            SelectValueType::String => a.get_str() == b.get_str(),
            SelectValueType::Array => {
                a.len().unwrap() == b.len().unwrap()
                    && a.values()
                        .unwrap()
                        .zip(b.values().unwrap())
                        .all(|(a, b)| is_equal(a.as_ref(), b.as_ref()))
            }
            SelectValueType::Object => {
                a.len().unwrap() == b.len().unwrap()
                    && a.keys()
                        .unwrap()
                        .all(|k| match (a.get_key(k), b.get_key(k)) {
                            (Some(a), Some(b)) => is_equal(a.as_ref(), b.as_ref()),
                            _ => false,
                        })
            }
        }
}
