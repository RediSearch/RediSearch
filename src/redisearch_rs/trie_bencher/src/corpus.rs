/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::{collections::BTreeSet, io::Cursor, path::PathBuf};

use crate::bencher::rust_load_from_terms;

/// This enum defines different corpora for benchmarking.
///
/// Users may call [CorpusType::download_or_read_corpus] to get the full content of the source files of a corpus or
/// use the [CorpusType::create_terms] method that generates a [Vec<String>] containing the unique terms for trie construction.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CorpusType {
    /// Uses a corpus from the redisearch benchmarks that contains 1k rows in a csv file describing wikipedia articles as a line.
    ///
    /// Uses the title of the wikipedia article as trie input.
    ///
    /// Used in benchmarks:
    ///
    /// - [search-ftsb-1K-enwiki_abstract-hashes-term-contains.yml](https://github.com/RediSearch/RediSearch/blob/master/tests/benchmarks/search-ftsb-1K-enwiki_abstract-hashes-term-contains.yml)
    /// - [search-ftsb-1K-enwiki_abstract-hashes-term-suffix-witfhsuffixtrie.yml](https://github.com/RediSearch/RediSearch/blob/master/tests/benchmarks/search-ftsb-1K-enwiki_abstract-hashes-term-suffix-withsuffixtrie.yml)
    /// - [search-ftsb-1K-enwiki_abstract-hashes-term-suffix.yml](https://github.com/RediSearch/RediSearch/blob/master/tests/benchmarks/search-ftsb-1K-enwiki_abstract-hashes-term-suffix.yml)
    RedisBench1kWiki,

    /// Uses a corpus from the redisearch benchmarks that contains 10k rows in a csv file describing document ids mapped to abriatary json values as a line.
    ///
    /// Uses doc id (document name)s from the redisearch benchmark as input for the trie instead of a `value`.
    ///
    /// Used in benchmark: [search-ftsb-10K-singlevalue-numeric-json.yml](https://github.com/RediSearch/RediSearch/blob/master/tests/benchmarks/search-ftsb-10K-singlevalue-numeric-json.yml)
    RedisBench10kNumerics,

    /// Small corpus of a book text from gutenberg.net.
    ///
    /// See https://gutenberg.net.au/ebooks01/0100021.txt
    GutenbergEbook(bool),
}

impl CorpusType {
    /// If the corpus has already been downloaded, read it from disk.
    /// Otherwise, download it from the internet and save it to disk.
    /// In any case perform a crc32 check to notice changes in the corpus data.
    pub fn download_or_read_corpus(&self) -> String {
        let path = self.get_cached_path();
        let corpus = if std::fs::exists(&path).ok() != Some(true) {
            let corpus = download_corpus(self.get_url());
            // ensure data folder exists
            let cache_folder = PathBuf::from("data");
            if !std::fs::exists(cache_folder)
                .expect("Cannot check for existience of data folder (cache), check permissions")
            {
                std::fs::create_dir(PathBuf::from("data"))
                    .expect("Failed to create data folder (cache)");
            }
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

    /// Creates a vector of terms for insertion into a trie,
    /// may output the trie to a file using a pretty printer.
    ///
    /// The pretty printed version of the trie helps a developer to
    /// explore the data, i.e. check what node would be inserted at
    /// what depth.
    pub fn create_terms(&self, output_pretty_print_trie: bool) -> Vec<String> {
        let corpus = self.download_or_read_corpus();
        let reval = match self {
            CorpusType::RedisBench1kWiki => self.create_terms_redis_wiki1k(&corpus),
            CorpusType::RedisBench10kNumerics => self.create_terms_redis_wiki10k(&corpus),
            CorpusType::GutenbergEbook(full) => self.create_terms_gutenberg(&corpus, *full),
        };
        if output_pretty_print_trie {
            let trie = rust_load_from_terms(&reval);
            fs_err::write(self.get_pretty_print_path(), format!("{trie:?}").as_bytes())
                .expect("Failed to write bench words debug to disk");
        }
        reval
    }

    /// Creates a vector of terms for insertion into a trie.
    ///
    /// Uses doc id (document name)s from the redis search benchmark as input for the trie instead of a `value`.
    fn create_terms_redis_wiki10k(&self, contents: &str) -> Vec<String> {
        // we find the guid like doc id in column 5
        let idx = 4;

        // Prefix used for each title:
        let prefix = "doc:single:";

        let reader = Cursor::new(contents);
        let mut rdr = csv::Reader::from_reader(reader);

        // generate strings without prefix:
        let strings = rdr
            .records()
            .map(|e| {
                e.unwrap()
                    .get(idx)
                    .unwrap()
                    .strip_prefix(prefix)
                    .unwrap_or_else(|| panic!("prefix in csv isn't {} anymore.", prefix))
                    .to_owned()
            })
            .collect::<Vec<_>>();

        strings
    }

    fn create_terms_gutenberg(&self, contents: &str, full: bool) -> Vec<String> {
        // use words in the text file as keys and ensure uniqueness of keys
        let mut unique = BTreeSet::new();
        // we skip the first 36 lines of the text file, which are not part of the book but metadata
        'outer: for line in contents.lines().skip(36) {
            for word in line.split_whitespace() {
                unique.insert(word.to_string());
                // we only use the first 82 unique words with creates 108 nodes (micro benchmark)
                if !full && unique.len() >= 82 {
                    break 'outer;
                }
            }
        }
        unique.into_iter().collect::<Vec<_>>()
    }

    fn create_terms_redis_wiki1k(&self, contents: &str) -> Vec<String> {
        // we generate a trie based on the title field
        let title_offset = 6;

        // Prefix used for each title:
        let prefix = "Wikipedia\\: ";

        let reader = Cursor::new(contents);
        let mut rdr = csv::Reader::from_reader(reader);

        // generate strings without prefix:
        let strings = rdr
            .records()
            .map(|e| {
                e.unwrap()
                    .get(title_offset)
                    .unwrap()
                    .strip_prefix(prefix)
                    .unwrap_or_else(|| panic!("prefix in csv isn't {} anymore.", prefix))
                    .to_owned()
            })
            .collect::<Vec<_>>();

        strings
    }

    /// Returns the url of the corpus.
    ///
    /// Information for remote files can be found in the folder: git_root/tests/benchmarks/
    fn get_url(&self) -> &str {
        match self {
            CorpusType::RedisBench1kWiki => {
                "https://s3.amazonaws.com/benchmarks.redislabs/redisearch/datasets/enwiki_abstract-hashes-contains/enwiki_abstract-hashes-contains.redisearch.commands.SETUP.csv"
            }
            CorpusType::RedisBench10kNumerics => {
                "https://s3.amazonaws.com/benchmarks.redislabs/redisearch/datasets/10K-singlevalue-numeric-json/10K-singlevalue-numeric-json.redisjson.commands.SETUP.csv"
            }
            CorpusType::GutenbergEbook(_) => "https://gutenberg.net.au/ebooks01/0100021.txt",
        }
    }

    /// Returns the checksum for the downloaded files.
    fn get_checksum(&self) -> Option<u32> {
        match self {
            CorpusType::RedisBench1kWiki => Some(0x65ed64eb),
            CorpusType::RedisBench10kNumerics => Some(0x3c18690f),
            CorpusType::GutenbergEbook(_) => Some(3817457071),
        }
    }

    /// returns the filesystem cache path of the corpus
    fn get_cached_path(&self) -> PathBuf {
        match self {
            CorpusType::RedisBench1kWiki => PathBuf::from("data")
                .join("enwiki_abstract-hashes-contains.redisearch.commands.SETUP.csv"),
            CorpusType::RedisBench10kNumerics => PathBuf::from("data")
                .join("10K-singlevalue-numeric-json.redisjson.commands.SETUP.csv"),
            CorpusType::GutenbergEbook(_) => PathBuf::from("data").join("1984.txt"),
        }
    }

    /// returns a path in the data folder that shall be used to store the pretty printed version of the trie
    fn get_pretty_print_path(&self) -> PathBuf {
        let path = PathBuf::from("data");
        let filename = match self {
            CorpusType::RedisBench1kWiki => "redis_wiki1k_titles_bench.txt".to_owned(),
            CorpusType::RedisBench10kNumerics => "redis_wiki10k_guids_bench.txt".to_owned(),
            CorpusType::GutenbergEbook(full) => {
                let suffix = if *full { "full" } else { "sample" };
                format!("gutenberg_bench_{suffix}.txt")
            }
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
    response
        .into_body()
        .read_to_string()
        .expect("Failed to response body")
}
