use crate::RsValue;
use ffi::{Obfuscate_Number, Obfuscate_Text};
use std::ffi::CStr;
use std::io::Write;

pub fn debug(value: &RsValue, writer: &mut impl Write, obfuscate: bool) -> std::io::Result<()> {
    fn write_text(writer: &mut impl Write, text: &[u8], obfuscate: bool) -> std::io::Result<()> {
        writer.write_all(b"\"")?;
        if obfuscate {
            writer.write_all(obfuscate_text(text))?;
        } else {
            writer.write_all(text)?;
        }
        writer.write_all(b"\"")
    }

    match value {
        RsValue::Undefined => writer.write_all(b"<Undefined>"),
        RsValue::Null => writer.write_all(b"NULL"),
        RsValue::Number(num) => {
            if obfuscate {
                writer.write_all(obfuscate_number(*num))
            } else {
                let mut buf = [0; 32];
                let n = crate::util::num_to_str(*num, &mut buf).unwrap();
                writer.write_all(&buf[0..n])
            }
        }
        RsValue::String(str) => write_text(writer, str.as_bytes(), obfuscate),
        RsValue::RedisString(str) => write_text(writer, str.as_bytes(), obfuscate),
        RsValue::Array(arr) => {
            writer.write_all(b"[")?;
            for (i, elem) in arr.iter().enumerate() {
                if i > 0 {
                    writer.write_all(b", ")?;
                }
                debug(elem.value(), writer, obfuscate)?;
            }
            writer.write_all(b"]")
        }
        RsValue::Map(map) => {
            writer.write_all(b"{")?;
            for (i, (key, val)) in map.iter().enumerate() {
                if i > 0 {
                    writer.write_all(b", ")?;
                }
                debug(key.value(), writer, obfuscate)?;
                writer.write_all(b": ")?;
                debug(val.value(), writer, obfuscate)?;
            }
            writer.write_all(b"}")
        }
        RsValue::Ref(ref_value) => debug(ref_value.value(), writer, obfuscate),
        RsValue::Trio(trio) => debug(trio.left().value(), writer, obfuscate),
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
