/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of (a) the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

/// Use `SelectValue`
use crate::select_value::{SelectValue, SelectValueType, ValueRef};
use ijson::{array::ArrayIterItem, DestructuredRef, IString, IValue, ValueType};
use serde_json::Value;

impl SelectValue for Value {
    fn get_type(&self) -> SelectValueType {
        match self {
            Self::Bool(_) => SelectValueType::Bool,
            Self::String(_) => SelectValueType::String,
            Self::Null => SelectValueType::Null,
            Self::Array(_) => SelectValueType::Array,
            Self::Object(_) => SelectValueType::Object,
            Self::Number(n) if n.is_i64() => SelectValueType::Long,
            Self::Number(n) if n.is_f64() | n.is_u64() => SelectValueType::Double,
            _ => panic!("bad type for Number value"),
        }
    }

    fn contains_key(&self, key: &str) -> bool {
        match self {
            Self::Object(o) => o.contains_key(key),
            _ => false,
        }
    }

    fn values<'a>(&'a self) -> Option<Box<dyn Iterator<Item = ValueRef<'a, Self>> + 'a>> {
        match self {
            Self::Array(arr) => Some(Box::new(arr.iter().map(ValueRef::Borrowed))),
            Self::Object(o) => Some(Box::new(o.values().map(ValueRef::Borrowed))),
            _ => None,
        }
    }

    fn keys<'a>(&'a self) -> Option<Box<dyn Iterator<Item = &'a str> + 'a>> {
        match self {
            Self::Object(o) => Some(Box::new(o.keys().map(|k| &k[..]))),
            _ => None,
        }
    }

    fn items<'a>(&'a self) -> Option<Box<dyn Iterator<Item = (&'a str, ValueRef<'a, Self>)> + 'a>> {
        match self {
            Self::Object(o) => Some(Box::new(
                o.iter().map(|(k, v)| (&k[..], ValueRef::Borrowed(v))),
            )),
            _ => None,
        }
    }

    fn len(&self) -> Option<usize> {
        match self {
            Self::Array(arr) => Some(arr.len()),
            Self::Object(obj) => Some(obj.len()),
            _ => None,
        }
    }

    fn is_empty(&self) -> Option<bool> {
        match self {
            Self::Array(arr) => Some(arr.is_empty()),
            Self::Object(obj) => Some(obj.is_empty()),
            _ => None,
        }
    }

    fn get_key<'a>(&'a self, key: &str) -> Option<ValueRef<'a, Self>> {
        match self {
            Self::Object(o) => o.get(key).map(ValueRef::Borrowed),
            _ => None,
        }
    }

    fn get_index<'a>(&'a self, index: usize) -> Option<ValueRef<'a, Self>> {
        match self {
            Self::Array(arr) => arr.get(index).map(ValueRef::Borrowed),
            _ => None,
        }
    }

    fn is_array(&self) -> bool {
        matches!(self, Self::Array(_))
    }

    fn is_double(&self) -> Option<bool> {
        match self {
            Self::Number(num) => Some(num.is_f64()),
            _ => None,
        }
    }

    fn get_str(&self) -> String {
        match self {
            Self::String(s) => s.to_string(),
            _ => panic!("not a string"),
        }
    }

    fn as_str(&self) -> &str {
        match self {
            Self::String(s) => s.as_str(),
            _ => panic!("not a string"),
        }
    }

    fn get_bool(&self) -> bool {
        match self {
            Self::Bool(b) => *b,
            _ => panic!("not a bool"),
        }
    }

    fn get_long(&self) -> i64 {
        match self {
            Self::Number(n) if n.is_i64() => n.as_i64().unwrap(),
            _ => panic!("not a long"),
        }
    }

    fn get_double(&self) -> f64 {
        match self {
            Self::Number(n) if n.is_f64() => n.as_f64().unwrap(),
            Self::Number(n) if n.is_u64() => n.as_u64().unwrap() as _,
            _ => panic!("not a double"),
        }
    }
}

impl<'a> From<ArrayIterItem<'a>> for ValueRef<'a, IValue> {
    fn from(item: ArrayIterItem<'a>) -> Self {
        match item {
            ArrayIterItem::Borrowed(val) => ValueRef::Borrowed(val),
            ArrayIterItem::Owned(val) => ValueRef::Owned(val),
        }
    }
}

impl SelectValue for IValue {
    fn get_type(&self) -> SelectValueType {
        match self.type_() {
            ValueType::Bool => SelectValueType::Bool,
            ValueType::String => SelectValueType::String,
            ValueType::Null => SelectValueType::Null,
            ValueType::Array => SelectValueType::Array,
            ValueType::Object => SelectValueType::Object,
            ValueType::Number => {
                let num = self.as_number().unwrap();
                if num.has_decimal_point() | num.to_i64().is_none() {
                    SelectValueType::Double
                } else {
                    SelectValueType::Long
                }
            }
        }
    }

    fn contains_key(&self, key: &str) -> bool {
        self.as_object().map_or(false, |o| o.contains_key(key))
    }

    fn values<'a>(&'a self) -> Option<Box<dyn Iterator<Item = ValueRef<'a, Self>> + 'a>> {
        match self.destructure_ref() {
            DestructuredRef::Array(arr) => Some(Box::new(arr.iter().map(Into::into))),
            DestructuredRef::Object(o) => Some(Box::new(o.values().map(ValueRef::Borrowed))),
            _ => None,
        }
    }

    fn keys<'a>(&'a self) -> Option<Box<dyn Iterator<Item = &'a str> + 'a>> {
        match self.destructure_ref() {
            DestructuredRef::Object(o) => Some(Box::new(o.keys().map(IString::as_str))),
            _ => None,
        }
    }

    fn items<'a>(&'a self) -> Option<Box<dyn Iterator<Item = (&'a str, ValueRef<'a, Self>)> + 'a>> {
        match self.destructure_ref() {
            DestructuredRef::Object(o) => Some(Box::new(
                o.iter().map(|(k, v)| (k.as_str(), ValueRef::Borrowed(v))),
            )),
            _ => None,
        }
    }

    fn len(&self) -> Option<usize> {
        match self.destructure_ref() {
            DestructuredRef::Array(arr) => Some(arr.len()),
            DestructuredRef::Object(o) => Some(o.len()),
            _ => None,
        }
    }

    fn is_empty(&self) -> Option<bool> {
        self.is_empty()
    }

    fn get_key<'a>(&'a self, key: &str) -> Option<ValueRef<'a, Self>> {
        self.as_object()
            .and_then(|o| o.get(key).map(ValueRef::Borrowed))
    }

    fn get_index<'a>(&'a self, index: usize) -> Option<ValueRef<'a, Self>> {
        self.as_array()
            .and_then(|arr| arr.iter().nth(index).map(Into::into))
    }

    fn is_array(&self) -> bool {
        self.is_array()
    }

    fn is_double(&self) -> Option<bool> {
        Some(self.as_number()?.has_decimal_point())
    }

    fn get_str(&self) -> String {
        self.as_string().expect("not a string").to_string()
    }

    fn as_str(&self) -> &str {
        self.as_string().expect("not a string").as_str()
    }

    fn get_bool(&self) -> bool {
        self.to_bool().expect("not a bool")
    }

    fn get_long(&self) -> i64 {
        self.as_number()
            .expect("not a number")
            .to_i64()
            .expect("not a long")
    }

    fn get_double(&self) -> f64 {
        self.as_number().expect("not a number").to_f64_lossy()
    }
}
