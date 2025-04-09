use std::{collections::BTreeSet, path::PathBuf};

use crate::RustTrieMap;

/// If the corpus has already been downloaded, read it from disk.
/// Otherwise, download it from the internet and save it to disk.
///
/// We also compute and save to disk a smaller corpus for microbenchmarks.
pub fn download_or_read_corpus() -> String {
    let data_dir = PathBuf::from("data");
    let corpus_path = data_dir.join("1984.txt");
    let corpus = if std::fs::exists(&corpus_path).ok() != Some(true) {
        let corpus = download_corpus();
        let checksum = crc32fast::hash(corpus.as_bytes());
        assert_eq!(
            checksum, 3817457071,
            "The checksum of the downloaded corpus does not match the expected value. \
            This may impact the accuracy and relevance of the operations being benchmarked. \
            Review the diff before updating the expected checksum."
        );
        fs_err::write(&corpus_path, corpus.as_bytes()).expect("Failed to write corpus to disk");
        corpus
    } else {
        fs_err::read_to_string(&corpus_path).expect("Failed to read corpus")
    };

    // Build a smaller corpus for microbenchmarks, limited to <100 unique words.
    let bench_words = {
        let mut unique = BTreeSet::new();
        'outer: for line in corpus.lines().skip(36) {
            for word in line.split_whitespace() {
                if unique.insert(word.to_string()) && unique.len() > 82 {
                    break 'outer;
                }
            }
        }
        unique
    };

    // Serialize the benchmark words to a file for easy reference.
    {
        let bench_words_path = data_dir.join("bench.txt");
        let bench_words_str = bench_words
            .clone()
            .into_iter()
            .collect::<Vec<_>>()
            .join("\n");
        fs_err::write(&bench_words_path, bench_words_str.as_bytes())
            .expect("Failed to write bench words to disk");
    }

    // Serialize a debug representation of the benchmark words to a file for easy reference.
    {
        let mut trie = RustTrieMap::new();
        for word in &bench_words {
            trie.insert(
                crate::str2c_char(word).as_ref(),
                std::ptr::NonNull::dangling(),
            );
        }
        fs_err::write(
            &data_dir.join("bench_trie.txt"),
            format!("{trie:?}").as_bytes(),
        )
        .expect("Failed to write bench words debug to disk");
    }

    corpus
}

fn download_corpus() -> String {
    let corpus_url = "https://gutenberg.net.au/ebooks01/0100021.txt";
    let response = ureq::get(corpus_url)
        .call()
        .expect("Failed to download corpus");
    assert!(
        response.status().is_success(),
        "The server responded with an error: {}",
        response.status()
    );
    let text = response
        .into_body()
        .read_to_string()
        .expect("Failed to response body");
    text
}
