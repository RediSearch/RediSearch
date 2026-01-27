use enumflags2::bitflags;

/// Document metadata flags (based on `RSDocumentFlags` from `redisearch.h`).
///
/// Maintained as a separate type to decouple from OSS—refactors or restructuring
/// We don't want for meaning of flags that are stored in disk to change due to OSS enum refactoring in production environments
#[bitflags]
#[repr(u32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DocumentFlag {
    Deleted = 0x01,
    HasPayload = 0x02,
    HasSortVector = 0x04,
    HasOffsetVector = 0x08,
    HasExpiration = 0x10,
    FailedToOpen = 0x20,
}

pub type DocumentFlags = enumflags2::BitFlags<DocumentFlag>;

/// Converts OSS flags (u32) to Search Disk DocumentFlags.
///
/// This function serves as the translation layer between OSS and disk code.
/// If the OSS flag representation changes, only this function needs updating.
#[inline]
pub fn flags_from_oss(oss_flags: u32) -> DocumentFlags {
    #[cfg(debug_assertions)]
    {
        const KNOWN_OSS_FLAGS: u32 = ffi::RSDocumentFlags_Document_Deleted
            | ffi::RSDocumentFlags_Document_HasPayload
            | ffi::RSDocumentFlags_Document_HasSortVector
            | ffi::RSDocumentFlags_Document_HasOffsetVector
            | ffi::RSDocumentFlags_Document_HasExpiration
            | ffi::RSDocumentFlags_Document_FailedToOpen;

        assert!(
            oss_flags & !KNOWN_OSS_FLAGS == 0,
            "Unknown OSS document flags detected: {:#010x} (known: {:#010x})",
            oss_flags & !KNOWN_OSS_FLAGS,
            KNOWN_OSS_FLAGS
        );
    }

    let mut flags = DocumentFlags::empty();
    if oss_flags & ffi::RSDocumentFlags_Document_Deleted != 0 {
        flags |= DocumentFlag::Deleted;
    }
    if oss_flags & ffi::RSDocumentFlags_Document_HasPayload != 0 {
        flags |= DocumentFlag::HasPayload;
    }
    if oss_flags & ffi::RSDocumentFlags_Document_HasSortVector != 0 {
        flags |= DocumentFlag::HasSortVector;
    }
    if oss_flags & ffi::RSDocumentFlags_Document_HasOffsetVector != 0 {
        flags |= DocumentFlag::HasOffsetVector;
    }
    if oss_flags & ffi::RSDocumentFlags_Document_HasExpiration != 0 {
        flags |= DocumentFlag::HasExpiration;
    }
    if oss_flags & ffi::RSDocumentFlags_Document_FailedToOpen != 0 {
        flags |= DocumentFlag::FailedToOpen;
    }
    flags
}

/// Converts Search Disk DocumentFlags to OSS flags (ffi::RSDocumentFlags).
#[inline]
pub fn flags_to_oss(flags: DocumentFlags) -> ffi::RSDocumentFlags {
    let mut oss_flags: ffi::RSDocumentFlags = 0;
    if flags.contains(DocumentFlag::Deleted) {
        oss_flags |= ffi::RSDocumentFlags_Document_Deleted;
    }
    if flags.contains(DocumentFlag::HasPayload) {
        oss_flags |= ffi::RSDocumentFlags_Document_HasPayload;
    }
    if flags.contains(DocumentFlag::HasSortVector) {
        oss_flags |= ffi::RSDocumentFlags_Document_HasSortVector;
    }
    if flags.contains(DocumentFlag::HasOffsetVector) {
        oss_flags |= ffi::RSDocumentFlags_Document_HasOffsetVector;
    }
    if flags.contains(DocumentFlag::HasExpiration) {
        oss_flags |= ffi::RSDocumentFlags_Document_HasExpiration;
    }
    if flags.contains(DocumentFlag::FailedToOpen) {
        oss_flags |= ffi::RSDocumentFlags_Document_FailedToOpen;
    }
    oss_flags
}
