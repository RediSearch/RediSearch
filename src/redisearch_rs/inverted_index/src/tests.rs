use crate::{Encoder, InvertedIndex, RSIndexResult};

struct Dummy;

impl Encoder for Dummy {
    fn encode<W: std::io::Write + std::io::Seek>(
        mut writer: W,
        delta: crate::Delta,
        record: &RSIndexResult,
    ) -> std::io::Result<usize> {
        let delta: usize = delta.into();

        writer.write_all(&delta.to_be_bytes())?;

        Ok(8)
    }
}

#[test]
fn add_record() {
    let mut ii = InvertedIndex::<Dummy>::new();
    let record = RSIndexResult::numeric(10, 5.0);

    ii.add_record(&record).unwrap();

    assert_eq!(ii.buffer, [0, 0, 0, 0, 0, 0, 0, 10])
}
