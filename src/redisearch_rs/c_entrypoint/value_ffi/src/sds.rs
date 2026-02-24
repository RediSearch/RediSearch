use ffi::{Obfuscate_Number, Obfuscate_Text};
use ffi::{sds, sdscatlen};
use std::ffi::{CStr, c_void};
use value::RsValue;

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_DumpSds(value: *const RsValue, sds: sds, obfuscate: bool) -> sds {
    let mut sds = unsafe { SdsWriter::new(sds) };

    match unsafe { value.as_ref() } {
        None => sds.write_str("nil"),
        Some(value) => dump_sds(value, &mut sds, obfuscate),
    }

    sds.extract_sds()
}

fn dump_sds(value: &RsValue, sds: &mut SdsWriter, obfuscate: bool) {
    fn write_text(sds: &mut SdsWriter, text: &[u8], obfuscate: bool) {
        sds.write_str("\"");
        if obfuscate {
            sds.write_bytes(obfuscate_text(text));
        } else {
            sds.write_bytes(text);
        }
        sds.write_str("\"");
    }

    match value {
        RsValue::Undefined => sds.write_str("<Undefined>"),
        RsValue::Null => sds.write_str("NULL"),
        RsValue::Number(num) => {
            if obfuscate {
                sds.write_bytes(obfuscate_number(*num));
            } else {
                let mut buf = [0; 32];
                let n = value::util::num_to_str(*num, &mut buf).unwrap();
                sds.write_bytes(&buf[0..n]);
            }
        }
        RsValue::String(str) => write_text(sds, str.as_bytes(), obfuscate),
        RsValue::RedisString(str) => write_text(sds, str.as_bytes(), obfuscate),
        RsValue::Array(arr) => {
            sds.write_str("[");
            for (i, elem) in arr.iter().enumerate() {
                if i > 0 {
                    sds.write_str(", ");
                }
                dump_sds(elem.value(), sds, obfuscate);
            }
            sds.write_str("]");
        }
        RsValue::Map(map) => {
            sds.write_str("{");
            for (i, (key, val)) in map.iter().enumerate() {
                if i > 0 {
                    sds.write_str(", ");
                }
                dump_sds(key.value(), sds, obfuscate);
                sds.write_str(": ");
                dump_sds(val.value(), sds, obfuscate);
            }
            sds.write_str("}");
        }
        RsValue::Ref(ref_value) => dump_sds(ref_value.value(), sds, obfuscate),
        RsValue::Trio(trio) => dump_sds(trio.left().value(), sds, obfuscate),
    }
}

fn obfuscate_number<'a>(number: f64) -> &'a [u8] {
    let obfuscated = unsafe { Obfuscate_Number(number) };
    unsafe { CStr::from_ptr(obfuscated) }.to_bytes()
}

fn obfuscate_text<'a, 'b>(text: &'a [u8]) -> &'b [u8] {
    let obfuscated = unsafe { Obfuscate_Text(text.as_ptr().cast()) };
    unsafe { CStr::from_ptr(obfuscated) }.to_bytes()
}

struct SdsWriter {
    sds: sds,
}

impl SdsWriter {
    unsafe fn new(sds: sds) -> Self {
        Self { sds }
    }

    fn extract_sds(self) -> sds {
        self.sds
    }

    unsafe fn sdscatlen(&mut self, ptr: *const c_void, len: usize) {
        self.sds = unsafe { sdscatlen(self.sds, ptr, len) };
    }

    fn write_bytes(&mut self, bytes: &[u8]) {
        unsafe { self.sdscatlen(bytes.as_ptr().cast(), bytes.len()) };
    }

    fn write_str(&mut self, str: &str) {
        self.write_bytes(str.as_bytes());
    }
}
