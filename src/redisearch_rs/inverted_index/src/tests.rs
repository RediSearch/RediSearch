use crate::{Encoder, InvertedIndex, RSIndexResult};

struct Dummy;

impl Encoder for Dummy {
    type DeltaType = u32;

    fn encode<W: std::io::Write + std::io::Seek>(
        mut writer: W,
        delta: Self::DeltaType,
        _record: &RSIndexResult,
    ) -> std::io::Result<usize> {
        writer.write_all(&delta.to_be_bytes())?;

        Ok(8)
    }
}

#[test]
fn add_records() {
    let mut ii = InvertedIndex::<Dummy>::new();
    let record = RSIndexResult::numeric(10, 5.0);

    ii.add_record(&record).unwrap();

    assert_eq!(ii.blocks.len(), 1);
    assert_eq!(ii.blocks[0].buffer, [0, 0, 0, 0]);
    assert_eq!(ii.blocks[0].num_entries, 1);
    assert_eq!(ii.blocks[0].first_doc_id, 10);
    assert_eq!(ii.blocks[0].last_doc_id, 10);
    assert_eq!(ii.num_docs, 1);

    let record = RSIndexResult::numeric(11, 5.0);

    ii.add_record(&record).unwrap();

    assert_eq!(ii.blocks.len(), 1);
    assert_eq!(ii.blocks[0].buffer, [0, 0, 0, 0, 0, 0, 0, 1]);
    assert_eq!(ii.blocks[0].num_entries, 2);
    assert_eq!(ii.blocks[0].first_doc_id, 10);
    assert_eq!(ii.blocks[0].last_doc_id, 11);
    assert_eq!(ii.num_docs, 2);
}

#[test]
fn writting_same_record_twice() {
    let mut ii = InvertedIndex::<Dummy>::new();
    let record = RSIndexResult::numeric(10, 5.0);

    ii.add_record(&record).unwrap();
    assert_eq!(ii.blocks.len(), 1);
    assert_eq!(ii.blocks[0].buffer, [0, 0, 0, 0]);
    assert_eq!(ii.blocks[0].num_entries, 1);

    let bytes_written = ii.add_record(&record).unwrap();

    assert_eq!(
        bytes_written, 0,
        "duplicate record should not be written by default"
    );
    assert_eq!(ii.blocks.len(), 1);
    assert_eq!(
        ii.blocks[0].buffer,
        [0, 0, 0, 0],
        "buffer should remain unchanged"
    );
    assert_eq!(ii.blocks[0].num_entries, 1);
    assert_eq!(ii.blocks[0].first_doc_id, 10);
    assert_eq!(ii.blocks[0].last_doc_id, 10);
    assert_eq!(ii.num_docs, 1, "this second doc was not added");

    struct AllowDupsDummy;

    impl Encoder for AllowDupsDummy {
        type DeltaType = u32;

        const ALLOW_DUPLICATES: bool = true;

        fn encode<W: std::io::Write + std::io::Seek>(
            mut writer: W,
            _delta: Self::DeltaType,
            _record: &RSIndexResult,
        ) -> std::io::Result<usize> {
            writer.write_all(&[255])?;

            Ok(1)
        }
    }

    let mut ii = InvertedIndex::<AllowDupsDummy>::new();

    ii.add_record(&record).unwrap();
    assert_eq!(ii.blocks.len(), 1);
    assert_eq!(ii.blocks[0].buffer, [255]);

    let bytes_written = ii.add_record(&record).unwrap();

    assert_eq!(
        bytes_written, 1,
        "duplicate record should be written when allowed"
    );
    assert_eq!(ii.blocks.len(), 1);
    assert_eq!(
        ii.blocks[0].buffer,
        [255, 255],
        "buffer should contain two entries"
    );
    assert_eq!(ii.blocks[0].num_entries, 2);
    assert_eq!(ii.blocks[0].first_doc_id, 10);
    assert_eq!(ii.blocks[0].last_doc_id, 10);
    assert_eq!(
        ii.num_docs, 1,
        "this doc was added but should not affect the count"
    );
}

#[test]
fn writing_creates_new_blocks() {
    struct SmallBlocksDummy;

    impl Encoder for SmallBlocksDummy {
        type DeltaType = u32;

        const ALLOW_DUPLICATES: bool = true;
        const BLOCK_ENTRIES: usize = 2;

        fn encode<W: std::io::Write + std::io::Seek>(
            mut writer: W,
            _delta: Self::DeltaType,
            _record: &RSIndexResult,
        ) -> std::io::Result<usize> {
            writer.write_all(&[1])?;

            Ok(1)
        }
    }

    let mut ii = InvertedIndex::<SmallBlocksDummy>::new();

    ii.add_record(&RSIndexResult::numeric(10, 5.0)).unwrap();
    assert_eq!(ii.blocks.len(), 1);
    ii.add_record(&RSIndexResult::numeric(11, 6.0)).unwrap();
    assert_eq!(ii.blocks.len(), 1);

    ii.add_record(&RSIndexResult::numeric(12, 4.0)).unwrap();
    assert_eq!(
        ii.blocks.len(),
        2,
        "should create a new block after reaching the limit"
    );
    ii.add_record(&RSIndexResult::numeric(13, 2.0)).unwrap();
    assert_eq!(ii.blocks.len(), 2);

    ii.add_record(&RSIndexResult::numeric(13, 1.0)).unwrap();
    assert_eq!(
        ii.blocks.len(),
        2,
        "duplicates should stay on the same block"
    );
}

#[test]
fn writting_big_delta_makes_new_block() {
    let mut ii = InvertedIndex::<Dummy>::new();
    let record = RSIndexResult::numeric(10, 5.0);

    ii.add_record(&record).unwrap();

    assert_eq!(ii.blocks.len(), 1);
    assert_eq!(ii.blocks[0].buffer, [0, 0, 0, 0]);
    assert_eq!(ii.blocks[0].num_entries, 1);
    assert_eq!(ii.blocks[0].first_doc_id, 10);
    assert_eq!(ii.blocks[0].last_doc_id, 10);
    assert_eq!(ii.num_docs, 1);

    let doc_id = (u32::MAX as u64) + 11;
    let record = RSIndexResult::numeric(doc_id, 5.0);

    ii.add_record(&record).unwrap();

    assert_eq!(ii.blocks.len(), 2);
    assert_eq!(ii.blocks[1].buffer, [0, 0, 0, 0]);
    assert_eq!(ii.blocks[1].num_entries, 1);
    assert_eq!(ii.blocks[1].first_doc_id, doc_id);
    assert_eq!(ii.blocks[1].last_doc_id, doc_id);
    assert_eq!(ii.num_docs, 2);
}
