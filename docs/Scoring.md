# Scoring in RediSearch

RediSearch comes with a few very basic scoring functions to evaluate document relevance. They are all based on document scores and term frequency. This is regardless of the ability to use [sortable fields](Sorting.md). Scoring functions are specified by adding the `SCORER {scorer_name}` argument to a search query.

If you prefer a custom scoring function, it is possible to add more functions using the [Extension API](Extensions.md).

These are the pre-bundled scoring functions available in RediSearch and how they work. Each function is mentioned by registered name, that can be passed as a `SCORER` argument in `FT.SEARCH`.

## TFIDF (default)

Basic [TF-IDF scoring](https://en.wikipedia.org/wiki/Tf%E2%80%93idf) with a few extra features thrown inside:

1. For each term in each result, we calculate the TF-IDF score of that term to that document. Frequencies are weighted based on field weights that are pre-determined, and each term's frequency is **normalized by the highest term frequency in each document**.

2. We multiply the total TF-IDF for the query term by the a priory document score given on `FT.ADD`.

3. We give a penalty to each result based on "slop" or cumulative distance between the search terms: exact matches will get no penalty, but matches where the search terms are distant see their score reduced significantly. For each 2-gram of consecutive terms, we find the minimal distance between them. The penalty is the square root of the sum of the distances, squared - `1/sqrt(d(t2-t1)^2 + d(t3-t2)^2 + ...)`.

So for N terms in document D, `T1...Tn`, the resulting score could be described with this python function:

```py
def get_score(terms, doc):
    # the sum of tf-idf
    score = 0

    # the distance penalty for all terms
    dist_penalty = 0

    for i, term in enumerate(terms):
        # tf normalized by maximum frequency
        tf = doc.freq(term) / doc.max_freq

        # idf is global for the index, and not calculated each time in real life
        idf = log2(1 + total_docs / docs_with_term(term))

        score += tf*idf

        # sum up the distance penalty
        if i > 0:
            dist_penalty += min_distance(term, terms[i-1])**2

    # multiply the score by the document score
    score *= doc.score

    # divide the score by the root of the cumulative distance
    if len(terms) > 1:
        score /= sqrt(dist_penalty)

    return score
```

## TFIDF.DOCNORM

Identical to the default TFIDF scorer, with one important distinction:

Term frequencies are normalized by the length of the document (expressed as the total number of terms). The length is weighted, so that if a document contains two terms, one in a field that has a weight 1 and one in a field with a weight of 5, the total frequency is 6, not 2.

```
FT.SEARCH myIndex "foo" SCORER TFIDF.DOCNORM
```

## BM25

A variation on the basic TF-IDF scorer, see [this Wikipedia article for more info](https://en.wikipedia.org/wiki/Okapi_BM25).

We also multiply the relevance score for each document by the a priory document score and apply a penalty based on slop as in TFIDF.

```
FT.SEARCH myIndex "foo" SCORER BM25
```

## DISMAX

A simple scorer that sums up the frequencies of the matched terms; in the case of union clauses, it will give the maximum value of those matches. No other penalties or factors are applied.

It is not a 1 to 1 implementation of [Solr's DISMAX algorithm](https://wiki.apache.org/solr/DisMax) but follows it in broad terms.

```
FT.SEARCH myIndex "foo" SCORER DISMAX
```

## DOCSCORE

A scoring function that just returns the a priory score of the document without applying any calculations to it. Since document scores can be updated, this can be useful if you'd like to use an external score and nothing further.

```
FT.SEARCH myIndex "foo" SCORER DOCSCORE
```

## HAMMING

Scoring by the (inverse) Hamming Distance between the documents' payload and the query payload. Since we are interested in the **nearest** neighbors, we inverse the hamming distance (`1/(1+d)`) so that a distance of 0 gives a perfect score of 1 and is the highest rank.

This works only if:

1. The document has a payload.
2. The query has a payload.
3. Both are **exactly the same length**.

Payloads are binary-safe, and having payloads with a length that's a multiple of 64 bits yields slightly faster results.

Example:

```
127.0.0.1:6379> FT.CREATE idx SCHEMA foo TEXT
OK
127.0.0.1:6379> FT.ADD idx 1 1 PAYLOAD "aaaabbbb" FIELDS foo hello
OK
127.0.0.1:6379> FT.ADD idx 2 1 PAYLOAD "aaaacccc" FIELDS foo bar
OK

127.0.0.1:6379> FT.SEARCH idx "*" PAYLOAD "aaaabbbc" SCORER HAMMING WITHSCORES
1) (integer) 2
2) "1"
3) "0.5" // hamming distance of 1 --> 1/(1+1) == 0.5
4) 1) "foo"
   2) "hello"
5) "2"
6) "0.25" // hamming distance of 3 --> 1/(1+3) == 0.25
7) 1) "foo"
   2) "bar"
```
