---
title: "Stemming"
linkTitle: "Stemming"
weight: 10
description: Stemming support
aliases: 
    - /docs/stack/search/reference/stemming/
    - /redisearch/reference/stemming
---

# Stemming support

Redis Stack supports stemming, that is adding the base form of a word to the index. For example, querying for "going" will also return results for "go" and "gone".

The current stemming support is based on the [Snowball stemmer library](http://snowballstem.org/), which supports most European languages, as well as Arabic and others. Other languages will be included soon. If you need a specific language support, please open an issue.

## Supported languages

The following languages are supported and can be passed to the engine when indexing or querying using lowercase:

* arabic
* armenian
* danish
* dutch
* english
* finnish
* french
* german
* hungarian
* italian
* norwegian
* portuguese
* romanian
* russian
* serbian
* spanish
* swedish
* tamil
* turkish
* yiddish
* chinese (see below)

## Chinese support

Indexing a Chinese document is different than indexing a document in most other languages because of how tokens are extracted. While most languages can have their tokens distinguished by separation characters and whitespace, this is not common in Chinese.

Chinese tokenization is done by scanning the input text and checking every character or sequence of characters against a dictionary of predefined terms and determining the most likely match based on the surrounding terms and characters.

Redis Stack makes use of the [Friso](https://github.com/lionsoul2014/friso) chinese tokenization library for this purpose. This is largely transparent to the user and often no additional configuration is required.

## Using custom dictionaries

If you wish to use a custom dictionary, you can do so at the module level when loading the module. The `FRISOINI` setting can point to the location of a `friso.ini` file which contains the relevant settings and paths to the dictionary files.

Note that there is no default `friso.ini` file location. RedisSearch comes with its own `friso.ini` and dictionary files that are compiled into the module binary at build-time.
