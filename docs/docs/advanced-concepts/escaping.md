---
title: "Tokenization"
linkTitle: "Tokenization"
weight: 4
description: Controlling Text Tokenization and Escaping
aliases: 
    - /docs/stack/search/reference/escaping/
---

# Controlling Text Tokenization and Escaping

At the moment, Redis Stack uses a very simple tokenizer for documents and a slightly more sophisticated tokenizer for queries. Both allow a degree of control over string escaping and tokenization. 

Note: There is a different mechanism for tokenizing text and tag fields, this document refers only to text fields. For tag fields please refer to the [Tag Fields](/redisearch/reference/tags) documentation. 

## The rules of text field tokenization

1. All punctuation marks and whitespace (besides underscores) separate the document and queries into tokens. e.g. any character of `,.<>{}[]"':;!@#$%^&*()-+=~` will break the text into terms.  So the text `foo-bar.baz...bag` will be tokenized into `[foo, bar, baz, bag]`

2. Escaping separators in both queries and documents is done by prepending a backslash to any separator. e.g. the text `hello\-world hello-world` will be tokenized as `[hello-world, hello, world]`. **NOTE** that in most languages you will need an extra backslash when formatting the document or query, to signify an actual backslash, so the actual text in redis-cli for example, will be entered as `hello\\-world`. 

3. Underscores (`_`) are not used as separators in either document or query. So the text `hello_world` will remain as is after tokenization. 

4. Repeating spaces or punctuation marks are stripped. 

5. In Latin characters, everything gets converted to lowercase. 

6. A backslash before the first digit will tokenize it as a term. This will translate `-` sign as NOT which otherwise will make the number negative. Add a backslash before `.` if you are searching for a float. (ex. -20 -> {-20} vs -\20 -> {NOT{20}})
