use redisearch_disk::index_spec::doc_table::{
    DocumentFlag, DocumentFlags, flags_from_oss, flags_to_oss,
};

#[test]
fn roundtrip_rust_to_oss_to_rust() {
    let test_cases = [
        DocumentFlags::empty(),
        DocumentFlag::Deleted.into(),
        DocumentFlag::HasPayload.into(),
        DocumentFlag::HasSortVector.into(),
        DocumentFlag::HasOffsetVector.into(),
        DocumentFlag::HasExpiration.into(),
        DocumentFlag::FailedToOpen.into(),
        DocumentFlags::all(),
    ];

    for original in test_cases {
        let oss = flags_to_oss(original);
        let roundtripped = flags_from_oss(oss);
        assert_eq!(
            original, roundtripped,
            "Rust->OSS->Rust failed for {:?}",
            original
        );
    }
}

#[test]
fn roundtrip_oss_to_rust_to_oss() {
    use ffi::{
        RSDocumentFlags_Document_Deleted, RSDocumentFlags_Document_FailedToOpen,
        RSDocumentFlags_Document_HasExpiration, RSDocumentFlags_Document_HasOffsetVector,
        RSDocumentFlags_Document_HasPayload, RSDocumentFlags_Document_HasSortVector,
    };

    let test_cases = [
        0u32,
        RSDocumentFlags_Document_Deleted,
        RSDocumentFlags_Document_HasPayload,
        RSDocumentFlags_Document_HasSortVector,
        RSDocumentFlags_Document_HasOffsetVector,
        RSDocumentFlags_Document_HasExpiration,
        RSDocumentFlags_Document_FailedToOpen,
        RSDocumentFlags_Document_Deleted
            | RSDocumentFlags_Document_HasPayload
            | RSDocumentFlags_Document_HasSortVector
            | RSDocumentFlags_Document_HasOffsetVector
            | RSDocumentFlags_Document_HasExpiration
            | RSDocumentFlags_Document_FailedToOpen,
    ];

    for original in test_cases {
        let rust_flags = flags_from_oss(original);
        let roundtripped = flags_to_oss(rust_flags);
        assert_eq!(
            original, roundtripped,
            "OSS->Rust->OSS failed for {:#x}",
            original
        );
    }
}

#[test]
#[should_panic(expected = "Unknown OSS document flags detected")]
#[cfg(debug_assertions)]
fn unknown_flags_panics_in_debug() {
    // Pass u32::MAX which has many unknown bits set
    flags_from_oss(u32::MAX);
}
