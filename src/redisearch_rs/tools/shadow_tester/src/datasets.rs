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
pub fn get_1984_index() -> Result<Vec<String>, ureq::Error> {
    let text = ureq::get("https://gutenberg.net.au/ebooks01/0100021.txt")
        .call()?
        .into_body()
        .read_to_string()?;

    let mut part_num = 0;
    let mut part_len = 0;
    let mut index_pos = 0;
    let mut part_text = vec![];

    let mut queries = vec!["flushall".to_string()];

    for line in text.lines() {
        if line.is_empty() {
            let t = part_text.join(" ");
            queries.push(format!(
                "HSET 1984:part:{part_num} text '{t}' part_num {part_num} index_pos {index_pos} part_len {part_len}"
            ).to_string());

            part_num += 1;
            index_pos += part_len + 1;
            part_text.clear();
            part_len = 0;
        } else {
            part_text.push(line.replace("'", "\\'")); // Be sure to escape the single quote since it is used in the query
            part_len += line.len() + 1;
        }
    }

    let t = part_text.join(" ");
    queries.push(format!(
        "HSET 1984:part:{part_num} text '{t}' part_num {part_num} index_pos {index_pos} part_len {part_len}"
    ).to_string());

    queries.push("FT.CREATE idx:1984 ON HASH PREFIX 1 '1984:part:' SCHEMA text TEXT WEIGHT 1.0 part_num NUMERIC SORTABLE index_pos NUMERIC SORTABLE length NUMERIC SORTABLE".to_string());

    Ok(queries)
}
