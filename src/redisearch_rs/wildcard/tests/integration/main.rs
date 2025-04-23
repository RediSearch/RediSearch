mod fixed_length;
mod matches;
mod parse;
// Disable the proptests when testing with Miri,
// as proptest accesses the file system, which is not supported by Miri
#[cfg(not(miri))]
mod properties;
mod utils;
