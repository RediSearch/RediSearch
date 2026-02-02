/// Trait extension to convert various types from/into byte slices for use as values.
pub trait ValueExt: Sized {
    type ArchivedType<'a>: Into<Self>;

    /// Get this value as a Speedb value.
    fn as_speedb_value(&self) -> Vec<u8>;

    /// Construct this value from a Speedb value.
    fn from_speedb_value(value: &[u8]) -> Self {
        Self::archive_from_speedb_value(value).into()
    }

    /// Construct the archived type from a Speedb value. An archived type allows for
    /// zero-copy deserialization.
    fn archive_from_speedb_value(value: &[u8]) -> Self::ArchivedType<'_>;
}
