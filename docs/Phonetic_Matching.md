# Phonetic Matching

Phonetic matching, a.k.a "Jon or John", allows searching for terms based on their pronounciation. This capability can be a useful tool when searching for names of people.

Phonetic matching is based on the use of a phonetic algorithm. A phonetic algorithm transforms the input term to an approximate representation of its pronounciation. This allows indexing terms, and consequently searching, by their pronounciation.

As of v1.4 RediSearch provides phonetic matching via the definition of text fields with the `PHONETIC` attribute. This causes the terms in such fields to be indexed both by their textual value as well as their phonetic approximation.

Performing a search on `PHONETIC` fields will, by default, also return results for phonetically similar terms. This behavior can be controlled with the [`$phonetic` query attribute](Query_Syntax.md#query_attributes).

## Phonetic algorithms support

RediSearch currently supports a single phonetic algorithm, the [Double Metaphone](https://en.wikipedia.org/wiki/Metaphone#Double_Metaphone) (DM). It uses the implementation at [slacy/double-metaphone](https://github.com/slacy/double-metaphone), which provides general support for Latin languages.
