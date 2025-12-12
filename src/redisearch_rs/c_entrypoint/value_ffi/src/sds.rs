use ffi::sds;
use std::io::Write;
use value::RsValue;
use value::sds_writer::SdsWriter;

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_DumpSds(value: *const RsValue, sds: sds, obfuscate: bool) -> sds {
    let mut writer = unsafe { SdsWriter::new(sds) };

    match unsafe { value.as_ref() } {
        None => write!(writer, "nil").unwrap(),
        Some(value) => write!(writer, "{:?}", value.debug_formatter(obfuscate)).unwrap(),
    }

    writer.extract_sds()
}
