---
title: "Tokenization"
linkTitle: "Tokenization"
weight: 4
description: Controlling text tokenization and escaping
aliases: 
    - /docs/stack/search/reference/escaping/
---

# Controlling text tokenization and escaping

Redis Stack uses a very simple tokenizer for documents and a slightly more sophisticated tokenizer for queries. Both allow a degree of control over string escaping and tokenization. 

Note: There is a different mechanism for tokenizing text and tag fields, this document refers only to text fields. For tag fields please refer to the [tag fields](/docs/interact/search-and-query/advanced-concepts/tags/) documentation. 

## The rules of text field tokenization

1. All punctuation marks and whitespace (besides underscores) separate the document and queries into tokens. For example, any character of `,.<>{}[]"':;!@#$%^&*()-+=~` will break the text into terms, so the text `foo-bar.baz...bag` will be tokenized into `[foo, bar, baz, bag]`

2. Escaping separators in both queries and documents is done by prepending a backslash to any separator. For example, the text `hello\-world hello-world` will be tokenized as `[hello-world, hello, world]`. In most languages you will need an extra backslash to signify an actual backslash when formatting the document or query, so the actual text entered into redis-cli will be `hello\\-world`. 

3. Underscores (`_`) are not used as separators in either document or query, so the text `hello_world` will remain as is after tokenization. 

4. Repeating spaces or punctuation marks are stripped. 

5. Latin characters are converted to lowercase. 

6. A backslash before the first digit will tokenize it as a term. This will translate the `-` sign as NOT, which otherwise would make the number negative. Add a backslash before `.` if you are searching for a float. For example, `-20 -> {-20} vs -\20 -> {NOT{20}}`.
