#[repr(C)]
/// cbindgen:prefix-with-name
pub enum RsValueType {
    Undefined,
    Null,
    Number,
    SharedString,
    OwnedString,
    SharedRedisString,
    RedisString,
    BorrowedRedisString,
    SharedSds,
    Sds,
    Ref,
    Duo,
}

pub trait AsRsValueType {
    fn as_value_type(&self) -> RsValueType;
}

impl AsRsValueType for value::RsValueInternal {
    fn as_value_type(&self) -> RsValueType {
        match self {
            value::RsValueInternal::Null => RsValueType::Null,
            value::RsValueInternal::Number(_) => RsValueType::Number,
            value::RsValueInternal::SharedString(_) => RsValueType::SharedString,
            value::RsValueInternal::OwnedString(_) => RsValueType::OwnedString,
            value::RsValueInternal::SharedRedisString(_) => RsValueType::SharedRedisString,
            value::RsValueInternal::RedisString(_) => RsValueType::RedisString,
            value::RsValueInternal::BorrowedRedisString(_) => RsValueType::BorrowedRedisString,
            value::RsValueInternal::SharedSds(_) => RsValueType::SharedSds,
            value::RsValueInternal::Sds(_) => RsValueType::Sds,
            value::RsValueInternal::Ref(_) => RsValueType::Ref,
            value::RsValueInternal::Duo(_, _) => RsValueType::Duo,
        }
    }
}

impl AsRsValueType for value::RsValue {
    fn as_value_type(&self) -> RsValueType {
        match self {
            value::RsValue::Undefined => RsValueType::Undefined,
            value::RsValue::Defined(i) => i.as_value_type(),
        }
    }
}

impl AsRsValueType for value::SharedRsValue {
    fn as_value_type(&self) -> RsValueType {
        match self {
            value::SharedRsValue::Undefined => RsValueType::Undefined,
            value::SharedRsValue::Defined(i) => i.as_value_type(),
        }
    }
}

impl AsRsValueType for crate::RSValue {
    fn as_value_type(&self) -> RsValueType {
        match self {
            crate::RSValue::Heap(v) => v.as_value_type(),
            crate::RSValue::Stack(v) => v.as_value_type(),
        }
    }
}
