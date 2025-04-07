use std::{collections::BTreeSet, io::Cursor, path::PathBuf};

use crate::RustTrieMap;
use crate::str2c_char;

/// This enum allows easy switching between different corpora for benchmarking
/// Information for remote files can be found in the folder: git_root/tests/benchmarks/
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, strum::EnumIter)]
pub enum CorpusType {
    /// uses a corpus from the redis benchmarks that contains 1k rows in a csv file describing wikipedia articles as a line
    RedisBench1kWiki,

    /// uses a corpus from the redis benchmarks that contains 10k rows in a csv file describing wikipedia articles as a line
    RedisBench10kNumerics,

    /// legacy corpus from gutenberg.net called 1984.txt
    GutenbergEbook,
}

impl CorpusType {
    /// If the corpus has already been downloaded, read it from disk.
    /// Otherwise, download it from the internet and save it to disk.
    pub fn download_or_read_corpus(&self) -> String {
        let path = self.get_cached_path();
        if std::fs::exists(&path).ok() != Some(true) {
            let corpus = download_corpus(self.get_url());
            if let Some(stored_checksum) = self.get_checksum() {
                let checksum = crc32fast::hash(corpus.as_bytes());
                assert_eq!(
                    checksum, stored_checksum,
                    "The checksum of the downloaded corpus does not match the expected value. \
                    This may impact the accuracy and relevance of the operations being benchmarked. \
                    Review the diff before updating the expected checksum."
                );
            }
            fs_err::write(&path, corpus.as_bytes()).expect("Failed to write corpus to disk");
            corpus
        } else {
            fs_err::read_to_string(&path).expect("Failed to read corpus")
        }
    }

    /// creates a vector of null terminated cstrs and a vector of "non-terminated" Clike strings
    ///
    /// contents a string slice containing the file content, may be CSV or txt depending on [CorpusType].
    pub fn create_keys(&self, output_pretty_print_trie: bool) -> Vec<String> {
        let corpus = self.download_or_read_corpus();
        let reval = match self {
            CorpusType::RedisBench1kWiki => self.create_keys_redis_wiki1k(&corpus),
            CorpusType::RedisBench10kNumerics => todo!(),
            CorpusType::GutenbergEbook => self.create_keys_gutenberg(&corpus),
        };
        if output_pretty_print_trie {
            write_pretty_printed_file_of_trie(&reval, &self.get_pretty_print_path());
        }
        reval
    }

    fn create_keys_gutenberg(&self, contents: &str) -> Vec<String> {
        let unique_words = {
            let mut unique = BTreeSet::new();
            'outer: for line in contents.lines().skip(36) {
                for word in line.split_whitespace() {
                    if unique.insert(word.to_string()) && unique.len() > 82 {
                        break 'outer;
                    }
                }
            }
            unique.into_iter().collect::<Vec<_>>()
        };

        unique_words
    }

    fn create_keys_redis_wiki1k(&self, contents: &str) -> Vec<String> {
        let reader = Cursor::new(contents);

        // we generate a trie based on the title
        let title_offset = 6;

        // further column offsets for documentation
        let _id_offset = 4;
        let _url_offset = 8;
        let _abstract_offset = 10;

        // Prefix used for each title:
        const _PREFIX: &str = "Wikipedia\\: ";
        let mut rdr = csv::Reader::from_reader(reader);

        // generate strings without prefix:
        let strings = rdr
            .records()
            .into_iter()
            .map(|e| e.unwrap().get(title_offset).unwrap()[_PREFIX.len()..].to_owned())
            .collect::<Vec<_>>();

        strings
    }

    /// returns the url of the corpus
    fn get_url(&self) -> &str {
        match self {
            CorpusType::RedisBench1kWiki => {
                "https://s3.amazonaws.com/benchmarks.redislabs/redisearch/datasets/enwiki_abstract-hashes-contains/enwiki_abstract-hashes-contains.redisearch.commands.SETUP.csv"
            }
            CorpusType::RedisBench10kNumerics => {
                "https://s3.amazonaws.com/benchmarks.redislabs/redisearch/datasets/10K-singlevalue-numeric-json/10K-singlevalue-numeric-json.redisjson.commands.SETUP.csv"
            }
            CorpusType::GutenbergEbook => "https://gutenberg.net.au/ebooks01/0100021.txt",
        }
    }

    fn get_checksum(&self) -> Option<u32> {
        match self {
            CorpusType::RedisBench1kWiki => Some(0x65ed64eb),
            CorpusType::RedisBench10kNumerics => None,
            CorpusType::GutenbergEbook => Some(3817457071),
        }
    }

    /// returns the filesystem cache path of the corpus
    fn get_cached_path(&self) -> PathBuf {
        match self {
            CorpusType::RedisBench1kWiki => PathBuf::from("data")
                .join("enwiki_abstract-hashes-contains.redisearch.commands.SETUP.csv"),
            CorpusType::RedisBench10kNumerics => PathBuf::from("data")
                .join("10K-singlevalue-numeric-json.redisjson.commands.SETUP.csv"),
            CorpusType::GutenbergEbook => PathBuf::from("data").join("1984.txt"),
        }
    }

    fn get_pretty_print_path(&self) -> PathBuf {
        let path = PathBuf::from("data");
        let filename = match self {
            CorpusType::RedisBench1kWiki => "redis_wiki1k_titles_bench.txt",
            CorpusType::RedisBench10kNumerics => "redis_wiki10k_guids_bench.txt",
            CorpusType::GutenbergEbook => "gutenberg_bench.txt",
        };
        path.join(filename)
    }
}

/// writes the output of the debug pretty print of a trie to a given file
///
/// - keys: vector containing all keys of the trie
/// - path: path to the file that shall be written with the pretty printed trie
fn write_pretty_printed_file_of_trie(keys: &Vec<String>, path: &PathBuf) {
    let mut trie = RustTrieMap::new();
    for word in keys {
        trie.insert(&str2c_char(word.as_str()), std::ptr::NonNull::dangling());
    }
    fs_err::write(path, format!("{trie:?}").as_bytes())
        .expect("Failed to write bench words debug to disk");
}

/// downloads a corpus from the specified URL returns its contents as a string
fn download_corpus(corpus_url: &str) -> String {
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
