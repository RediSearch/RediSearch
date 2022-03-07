# Query Spelling Correction

Query spelling correction, a.k.a "did you mean", provides suggestions for misspelled search terms. For example, the term 'reids' may be a misspelled 'redis'.

In such cases and as of v1.4 RediSearch can be used for generating alternatives to misspelled query terms. A misspelled term is a full text term (i.e., a word) that is:

  1. Not a stop word
  2. Not in the index
  3. At least 3 characters long

The alternatives for a misspelled term are generated from the corpus of already-indexed terms and, optionally, one or more custom dictionaries. Alternatives become spelling suggestions based on their respective Levenshtein distances (LD) from the misspelled term. Each spelling suggestion is given a normalized score based on its occurances in the index.

To obtain the spelling corrections for a query, refer to the documentation of the [`FT.SPELLCHECK`](Commands.md#ftspellcheck) command.

## Custom dictionaries

A dictionary is a set of terms. Dictionaries can be added with terms, have terms deleted from them and have their entire contents dumped using the [`FT.DICTADD`](Commands.md#ftdictadd), [`FT.DICTDEL`](Commands.md#ftdictdel) and [`FT.DICTDUMP`](Commands.md#ftdictdump) commands, respectively.

Dictionaries can be used to modify the behavior of RediSearch's query spelling correction, by including or excluding their contents from potential spelling correction suggestions.

When used for term inclusion, the terms in a dictionary can be provided as spelling suggestions regardless their occurances (or lack of) in the index. Scores of suggestions from inclusion dictionaries are always 0. 

Conversely, terms in an exlusion dictionary will never be returned as spelling alternatives.
