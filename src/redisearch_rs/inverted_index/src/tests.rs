use crate::{Encoder, InvertedIndex, RSIndexResult};

struct Dummy;

impl Encoder for Dummy {
    fn encode<W: std::io::Write + std::io::Seek>(
        mut writer: W,
        delta: crate::Delta,
        _record: &RSIndexResult,
    ) -> std::io::Result<usize> {
        let delta: usize = delta.into();

        writer.write_all(&delta.to_be_bytes())?;

        Ok(8)
    }
}

#[test]
fn add_records() {
    let mut ii = InvertedIndex::<Dummy>::new();
    let record = RSIndexResult::numeric(10, 5.0);

    ii.add_record(&record).unwrap();

    assert_eq!(ii.buffer, [0, 0, 0, 0, 0, 0, 0, 10]);
    assert_eq!(ii.num_docs, 1);

    let record = RSIndexResult::numeric(11, 5.0);

    ii.add_record(&record).unwrap();

    assert_eq!(
        ii.buffer,
        [0, 0, 0, 0, 0, 0, 0, 10, 0, 0, 0, 0, 0, 0, 0, 11]
    );
    assert_eq!(ii.num_docs, 2);
}

#[test]
fn writting_same_record_twice() {
    let mut ii = InvertedIndex::<Dummy>::new();
    let record = RSIndexResult::numeric(10, 5.0);

    ii.add_record(&record).unwrap();
    assert_eq!(ii.buffer, [0, 0, 0, 0, 0, 0, 0, 10]);

    let bytes_written = ii.add_record(&record).unwrap();

    assert_eq!(
        bytes_written, 0,
        "duplicate record should not be written by default"
    );
    assert_eq!(
        ii.buffer,
        [0, 0, 0, 0, 0, 0, 0, 10],
        "buffer should remain unchanged"
    );
    assert_eq!(ii.num_docs, 1, "this second doc was not added");

    struct AllowDupsDummy;

    impl Encoder for AllowDupsDummy {
        const ALLOW_DUPLICATES: bool = true;

        fn encode<W: std::io::Write + std::io::Seek>(
            mut writer: W,
            _delta: crate::Delta,
            _record: &RSIndexResult,
        ) -> std::io::Result<usize> {
            writer.write_all(&[255])?;

            Ok(1)
        }
    }

    let mut ii = InvertedIndex::<AllowDupsDummy>::new();

    ii.add_record(&record).unwrap();
    assert_eq!(ii.buffer, [255]);

    let bytes_written = ii.add_record(&record).unwrap();

    assert_eq!(
        bytes_written, 1,
        "duplicate record should be written when allowed"
    );
    assert_eq!(ii.buffer, [255, 255], "buffer should contain two entries");
    assert_eq!(
        ii.num_docs, 1,
        "this doc was added but should not affect the count"
    );
}
