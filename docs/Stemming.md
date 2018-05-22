# Stemming Support

RediSearch supports stemming - that is adding the base form of a word to the index. This allows the query for "going" to also return results for "go" and "gone", for example.

The current stemming support is based on the Snowball stemmer library, which supports most European languages, as well as Arabic and other. We hope to include more languages soon (if you need a specific language support, please open an issue).

For further details see the [Snowball Stemmer website](http://snowballstem.org/).

## Supported languages

The following languages are supported and can be passed to the engine when indexing or querying, with lowercase letters:

* arabic
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
* spanish
* swedish
* tamil
* turkish
* chinese (see below)

## Chinese support

Indexing a Chinese document is different than indexing a document in most other languages because of how tokens are extracted. While most languages can have their tokens distinguished by separation characters and whitespace, this is not common in Chinese.

Chinese tokenization is done by scanning the input text and checking every character or sequence of characters against a dictionary of predefined terms and determining the most likely (based on the surrounding terms and characters) match.

RediSearch makes use of the [Friso](https://github.com/lionsoul2014/friso) chinese tokenization library for this purpose. This is largely transparent to the user and often no additional configuration is required.

## Using custom dictionaries

If you wish to use a custom dictionary, you can do so at the module level when loading the module. The `FRISOINI` setting can point to the location of a `friso.ini` file which contains the relevant settings and paths to the dictionary files.

Note that there is no "default" friso.ini file location. RedisSearch comes with its own `friso.ini` and dictionary files which are compiled into the module binary at build-time.
