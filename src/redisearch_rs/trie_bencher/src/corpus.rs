use std::path::PathBuf;

/// If the corpus has already been downloaded, read it from disk.
/// Otherwise, download it from the internet and save it to disk.
pub fn download_or_read_corpus() -> String {
    let path = PathBuf::from("data").join("1984.txt");
    if std::fs::exists(&path).ok() != Some(true) {
        let corpus = download_corpus();
        fs_err::write(&path, corpus.as_bytes()).expect("Failed to write corpus to disk");
        corpus
    } else {
        fs_err::read_to_string(&path).expect("Failed to read corpus")
    }
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
