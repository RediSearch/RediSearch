use fnv::Fnv64;
use std::hash::Hasher;
use std::mem::ManuallyDrop;
use value::{RsValue, shared::SharedRsValue};

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_Hash(value: *const RsValue, hval: u64) -> u64 {
    let shared_value = unsafe { SharedRsValue::from_raw(value) };
    let shared_value = ManuallyDrop::new(shared_value);
    let value = shared_value.value();

    hash(value, hval)
}

fn hash(value: &RsValue, hval: u64) -> u64 {
    match value {
        RsValue::Undefined => 0,
        RsValue::Null => hval + 1,
        RsValue::Number(num) => {
            // C is doing something like:
            // let slice = unsafe {
            //     std::slice::from_raw_parts(
            //         num as *const f64 as *const u8,
            //         std::mem::size_of::<f64>(),
            //     )
            // };
            // fnv_hash(slice, hval)
            fnv_hash(&num.to_ne_bytes(), hval)
        }
        RsValue::String(string) => fnv_hash(string.as_bytes(), hval),
        RsValue::RedisString(string) => fnv_hash(string.as_bytes(), hval),
        RsValue::Array(array) => array.iter().fold(hval, |acc, item| hash(item.value(), acc)),
        RsValue::Ref(val) => hash(val.value(), hval),
        RsValue::Trio(trio) => hash(trio.left().value(), hval),
        RsValue::Map(map) => map.iter().fold(hval, |acc, (key, val)| {
            hash(val.value(), hash(key.value(), acc))
        }),
    }
}

fn fnv_hash(bytes: &[u8], hval: u64) -> u64 {
    let mut fnv = Fnv64::with_offset_basis(hval);
    fnv.write(bytes);
    fnv.finish()
}
