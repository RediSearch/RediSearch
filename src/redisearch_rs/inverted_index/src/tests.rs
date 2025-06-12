use std::io::Cursor;

use crate::{Encoder, Numeric, RSIndexResult};

#[test]
fn numeric_tiny_int() {
    let mut buf = Cursor::new(Vec::new());
    let delta = crate::Delta(0);
    let record = RSIndexResult::numeric(2.0);

    let bytes_written =
        Numeric::encode(&mut buf, delta, &record).expect("to encode numeric record");

    assert_eq!(bytes_written, 1, "tiny int should only fill the header");
    assert_eq!(buf.into_inner(), &[0b010_00_000]);
}
