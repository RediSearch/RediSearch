use crate::RsValue;
use ffi::{Obfuscate_Number, Obfuscate_Text};
use std::{
    ffi::CStr,
    fmt::{self, Debug},
};

pub struct DebugFormatter<'a> {
    pub(crate) value: &'a RsValue,
    pub(crate) obfuscate: bool,
}

impl<'a> Debug for DebugFormatter<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fn fmt_text(f: &mut fmt::Formatter<'_>, text: &[u8], obfuscate: bool) -> fmt::Result {
            if obfuscate {
                write!(f, "\"{}\"", obfuscate_text(text))
            } else {
                if let Ok(s) = std::str::from_utf8(text) {
                    write!(f, "\"{s}\"")
                } else {
                    f.write_str("<non-utf8-data>")
                }
            }
        }

        match self.value {
            RsValue::Undefined => f.write_str("<Undefined>"),
            RsValue::Null => f.write_str("NULL"),
            RsValue::Number(num) => {
                if self.obfuscate {
                    f.write_str(obfuscate_number(*num))
                } else {
                    let mut buf = [0; 32];
                    let n = crate::util::num_to_str(*num, &mut buf).unwrap();
                    let s = std::str::from_utf8(&buf[0..n]).unwrap();
                    f.write_str(s)
                }
            }
            RsValue::String(str) => fmt_text(f, str.as_bytes(), self.obfuscate),
            RsValue::RedisString(str) => fmt_text(f, str.as_bytes(), self.obfuscate),
            RsValue::Array(array) => {
                let entries = array
                    .iter()
                    .map(|item| item.value().debug_formatter(self.obfuscate));
                f.debug_list().entries(entries).finish()
            }
            RsValue::Map(map) => {
                let entries = map.iter().map(|(key, value)| {
                    (
                        key.value().debug_formatter(self.obfuscate),
                        value.value().debug_formatter(self.obfuscate),
                    )
                });
                f.debug_map().entries(entries).finish()
            }
            RsValue::Ref(ref_value) => ref_value.value().debug_formatter(self.obfuscate).fmt(f),
            RsValue::Trio(trio) => trio.left().value().debug_formatter(self.obfuscate).fmt(f),
        }
    }
}

fn obfuscate_number(number: f64) -> &'static str {
    let obfuscated = unsafe { Obfuscate_Number(number) };
    unsafe { CStr::from_ptr(obfuscated) }.to_str().unwrap()
}

fn obfuscate_text(text: &[u8]) -> &'static str {
    let obfuscated = unsafe { Obfuscate_Text(text.as_ptr().cast()) };
    unsafe { CStr::from_ptr(obfuscated) }.to_str().unwrap()
}
