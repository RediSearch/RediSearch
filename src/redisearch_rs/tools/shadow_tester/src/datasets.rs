use std::path::PathBuf;

use redis::{cmd, Cmd};

/// Construct queries to create a dataset for the "1984" book from Project Gutenberg
///
/// This creates a part entry for every paragraph (separated by a blank line) in the book. These
/// parts are then stored in the following structure:
///
/// ```
/// 1984:part:<part_num> → {
///   text: "paragraph content",
///   part_num: M,      // Sequential numbering
///   index_pos: 12345, // Character position in the full text
///   part_len: 323     // Paragraph length
/// }
/// ```
pub fn get_1984_index() -> Result<Vec<Cmd>, ureq::Error> {
    let text = get_1984_text()?;

    let mut part_num = 0;
    let mut part_len = 0;
    let mut index_pos = 0;
    let mut part_text = vec![];

    let mut setup_queries = vec![
        // Drop the index and all the entries associated with it. This ensures we are starting with
        // a clean slate.
        cmd("FT.DROPINDEX").arg("idx:1984").arg("DD").to_owned(),
        // Create the index first so that it is updated as new entries are added. If the index is
        // created after the entries, then it will update in the background and the test queries
        // will return inconsistent results based on the partial index.
        cmd("FT.CREATE")
            .arg(vec![
                "idx:1984",
                "ON",
                "HASH",
                "PREFIX",
                "1",
                "1984:part:",
                "SCHEMA",
                "text",
                "TEXT",
                "WEIGHT",
                "1.0",
                "part_num",
                "NUMERIC",
                "SORTABLE",
                "index_pos",
                "NUMERIC",
                "SORTABLE",
                "length",
                "NUMERIC",
                "SORTABLE",
            ])
            .to_owned(),
    ];

    for line in text.lines() {
        if line.is_empty() {
            let t = part_text.join(" ");
            setup_queries.push(
                cmd("HSET")
                    .arg(format!("1984:part:{part_num}"))
                    .arg("text")
                    .arg(t)
                    .arg("part_num")
                    .arg(part_num)
                    .arg("index_pos")
                    .arg(index_pos)
                    .arg("part_len")
                    .arg(part_len)
                    .to_owned(),
            );

            part_num += 1;
            index_pos += part_len + 1;
            part_text.clear();
            part_len = 0;
        } else {
            part_text.push(line);
            part_len += line.len() + 1;
        }
    }

    let t = part_text.join(" ");
    setup_queries.push(
        cmd("HSET")
            .arg(format!("1984:part:{part_num}"))
            .arg("text")
            .arg(t)
            .arg("part_num")
            .arg(part_num)
            .arg("index_pos")
            .arg(index_pos)
            .arg("part_len")
            .arg(part_len)
            .to_owned(),
    );

    Ok(setup_queries)
}

/// Gets the 1984 book text from the cache if it exists, otherwise fetches it from the Gutenberg
/// Project and stores it in the cache.
fn get_1984_text() -> Result<String, ureq::Error> {
    let cache_path = PathBuf::from("cache/1984.txt");
    if cache_path.exists() {
        let text = std::fs::read_to_string(&cache_path).expect("To read 1984 text from cache");
        return Ok(text);
    }

    let text = ureq::get("https://gutenberg.net.au/ebooks01/0100021.txt")
        .call()?
        .into_body()
        .read_to_string()?;

    let cache_dir = cache_path
        .parent()
        .expect("To get the parent directory for the cache file");
    if !cache_dir.exists() {
        std::fs::create_dir_all(cache_dir).expect("To create the cache directory");
    }

    std::fs::write(&cache_path, &text).expect("To write the 1984 text to cache");

    Ok(text)
}
