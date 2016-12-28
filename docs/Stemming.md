# Stemming Support

RediSearch supports stemming - that is adding the base form of a word to the index. This allows 
the query for "going" to also return results for "go" and "gone", for example. 

The current stemming support is based on the Snowball stemmer library, which supports most European
languages, as well as Arabic and other. We hope to include more languages soon (if you need a specicif
langauge support, please open an issue). 

For further details see the [Snowball Stemmer website](http://snowballstem.org/).

## Supported languages:

The following languages are supported, and can be passed to the engine 
when indexing or querying, with lowercase letters:

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
