Performs spelling correction on a query, returning suggestions for misspelled terms.

See [Query Spelling Correction](Spellcheck.md) for more details.

#### Parameters

* **index**: the index with the indexed terms.

* **query**: the search query.

* **TERMS**: specifies an inclusion (`INCLUDE`) or exclusion (`EXCLUDE`) custom dictionary named `{dict}`. Refer to [`FT.DICTADD`](Commands.md#ftdictadd), [`FT.DICTDEL`](Commands.md#ftdictdel) and [`FT.DICTDUMP`](Commands.md#ftdictdump) for managing custom dictionaries.

* **DISTANCE**: the maximal Levenshtein distance for spelling suggestions (default: 1, max: 4).

@return

@array-reply, in which each element represents a misspelled term from the query. The misspelled terms are ordered by their order of appearance in the query.

Each misspelled term, in turn, is a 3-element array consisting of the constant string "TERM", the term itself and an array of suggestions for spelling corrections.

Each element in the spelling corrections array consists of the score of the suggestion and the suggestion itself. The suggestions array, per misspelled term, is ordered in descending order by score.

The score is calculated by dividing the number of documents in which the suggested term exists, by the total number of documents in the index. Results can be normalized by dividing scores by the highest score.

@examples

```
redis> FT.SPELLCHECK idx held DISTANCE 2
1) 1) "TERM"
   2) "held"
   3) 1) 1) "0.66666666666666663"
         2) "hello"
      2) 1) "0.33333333333333331"
         2) "help"
```