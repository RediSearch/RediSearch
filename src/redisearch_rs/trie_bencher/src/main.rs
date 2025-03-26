use std::{
    collections::BTreeSet,
    ffi::{CString, c_void},
    ptr::NonNull,
};
use trie_bencher::str2c_char;
use trie_rs::trie::TrieMap;

fn main() -> std::io::Result<()> {
    let mut map = TrieMap::new();
    let cmap = unsafe { trie_bencher::ffi::NewTrieMap() };
    let contents = trie_bencher::download_or_read_corpus();
    let mut raw_size = 0;
    let mut n_words = 0;
    let mut unique_words = BTreeSet::new();
    for line in contents.lines() {
        for word in line.split_whitespace() {
            let converted = str2c_char(word);
            raw_size += converted.len();
            n_words += 1;
            unique_words.insert(word.to_owned());

            // Use a zero-sized type by passing a null pointer for `value`
            let value = NonNull::dangling();

            // Rust insertion
            map.insert(&converted, value);

            // C insertion
            let converted = CString::new(word).expect("CString conversion failed");
            let len: u16 = converted.as_bytes_with_nul().len().try_into().unwrap();
            let converted = converted.into_raw();
            unsafe {
                trie_bencher::ffi::TrieMap_Add(
                    cmap,
                    converted,
                    len,
                    value.as_ptr(),
                    Some(do_nothing),
                )
            };
        }
    }
    let n_unique_words = unique_words.len();
    println!(
        r#"Statistics:
- Raw text size: {:.3} MBs
- Number of words (with duplicates): {n_words}
- Number of unique words: {n_unique_words}
- Rust -> {:.3} MBs
          {} nodes
- C    -> {:.3} MBs
          {} nodes"#,
        raw_size as f64 / 1024. / 1024.,
        map.mem_usage() as f64 / 1024. / 1024.,
        map.num_nodes(),
        unsafe { trie_bencher::ffi::TrieMap_MemUsage(cmap) as f64 / 1024. / 1024. },
        unsafe { (*cmap).size }
    );

    Ok(())
}

unsafe extern "C" fn do_nothing(oldval: *mut c_void, _newval: *mut c_void) -> *mut c_void {
    // Just return the old value, since it's a zero-sized type and we don't want
    // the C map implementation to try to free it.
    oldval
}
