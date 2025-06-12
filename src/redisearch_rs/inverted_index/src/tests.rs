use std::io::Cursor;

use crate::{Encoder, Numeric, RSIndexResult};

#[test]
fn numeric_tiny_int() {
    let inputs = [
        (
            crate::Delta(0),
            RSIndexResult::numeric(2.0),
            1,
            vec![0b010_00_000],
        ),
        (
            crate::Delta(2),
            RSIndexResult::numeric(7.0),
            2,
            vec![0b111_00_001, 0b0000_0010],
        ),
    ];

    for input in inputs {
        let mut buf = Cursor::new(Vec::new());

        let (delta, record, expected_bytes_written, expected_buffer) = input;

        let bytes_written =
            Numeric::encode(&mut buf, delta, &record).expect("to encode numeric record");

        assert_eq!(bytes_written, expected_bytes_written);
        assert_eq!(buf.into_inner(), expected_buffer);
    }
}
