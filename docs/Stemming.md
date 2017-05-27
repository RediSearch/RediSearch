# Stemming Support

RediSearch supports stemming: associating the base form of a word with its index. This allows 
the query for "going" to also return results for "go" and "gone", for example. 

The current stemming capabilities utilize the Snowball Stemmer library, which supports most European
languages, as well as Arabic and others. We hope to include more languages soon (if you need a specific
langauge support, please open an issue). 

For further details see the [Snowball Stemmer website](http://snowballstem.org/).

## Supported languages:

The following languages are supported, and can be passed to the engine (using all lowercase letters)
when indexing or querying:

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
