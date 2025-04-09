use std::{collections::BTreeSet, io::Cursor, path::PathBuf};

use crate::bencher::rust_load_from_keys;

/// This enum allows easy switching between different corpora for benchmarking.
///
/// Users may call [CorpusType::download_or_read_corpus] to get the full content of the source files of a corpus or
/// use the [CorpusType::create_keys] method that generates a [Vec<String>] containing the unique keys for trie construction. These
/// must be further converted to the "legacy" string types: see [crate::strvec2_raw_words].
///
/// Information for remote files can be found in the folder: git_root/tests/benchmarks/
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
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
    /// In any case perform a crc32 check to notice changes in the corpus data.
    pub fn download_or_read_corpus(&self) -> String {
        let path = self.get_cached_path();
        let corpus = if std::fs::exists(&path).ok() != Some(true) {
            let corpus = download_corpus(self.get_url());
            fs_err::write(&path, corpus.as_bytes()).expect("Failed to write corpus to disk");
            corpus
        } else {
            fs_err::read_to_string(&path).expect("Failed to read corpus")
        };

        // check that the corpus hasn't been altered.
        if let Some(stored_checksum) = self.get_checksum() {
            let checksum = crc32fast::hash(corpus.as_bytes());
            assert_eq!(
                checksum, stored_checksum,
                "The checksum of the downloaded corpus does not match the expected value. \
                This may impact the accuracy and relevance of the operations being benchmarked. \
                Review the diff before updating the expected checksum."
            );
        }
        corpus
    }

    /// Creates a vector of keys for insertion into a trie,
    /// may output the trie to a file using a pretty printer.
    pub fn create_keys(&self, output_pretty_print_trie: bool) -> Vec<String> {
        let corpus = self.download_or_read_corpus();
        let reval = match self {
            CorpusType::RedisBench1kWiki => self.create_keys_redis_wiki1k(&corpus),
            CorpusType::RedisBench10kNumerics => self.create_keys_redis_wiki10k(&corpus),
            CorpusType::GutenbergEbook => self.create_keys_gutenberg(&corpus),
        };
        if output_pretty_print_trie {
            let trie = rust_load_from_keys(&reval);
            fs_err::write(self.get_pretty_print_path(), format!("{trie:?}").as_bytes())
                .expect("Failed to write bench words debug to disk");
        }
        reval
    }

    fn create_keys_redis_wiki10k(&self, contents: &str) -> Vec<String> {
        // we find the guid like doc id in column 5
        let idx = 4;

        // Prefix used for each title:
        const _PREFIX: &str = "doc:single:";

        let reader = Cursor::new(contents);
        let mut rdr = csv::Reader::from_reader(reader);

        // generate strings without prefix:
        let strings = rdr
            .records()
            .into_iter()
            .map(|e| e.unwrap().get(idx).unwrap()[_PREFIX.len()..].to_owned())
            .collect::<Vec<_>>();

        strings
    }

    fn create_keys_gutenberg(&self, contents: &str) -> Vec<String> {
        // use words in the text file as keys and ensure uniqueness of keys
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
        // we generate a trie based on the title
        let title_offset = 6;

        // Prefix used for each title:
        let prefix_len = "Wikipedia\\: ".len();

        let reader = Cursor::new(contents);
        let mut rdr = csv::Reader::from_reader(reader);

        // generate strings without prefix:
        let strings = rdr
            .records()
            .into_iter()
            .map(|e| e.unwrap().get(title_offset).unwrap()[prefix_len..].to_owned())
            .collect::<Vec<_>>();

        strings
    }

    /// Returns the url of the corpus.
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

    /// Returns the checksum for the downloaded files.
    fn get_checksum(&self) -> Option<u32> {
        match self {
            CorpusType::RedisBench1kWiki => Some(0x65ed64eb),
            CorpusType::RedisBench10kNumerics => Some(0x3c18690f),
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

    /// returns a path in the data folder that shall be used to store the pretty printed version of the trie
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
