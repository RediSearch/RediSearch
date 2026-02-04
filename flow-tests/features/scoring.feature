Feature: RediSearchDisk Scoring Functionality
  As a developer
  I want to test BM25 and TFIDF scoring in RediSearchDisk
  So that I can ensure search results have correct relevance scores
  And that scoring remains correct after RDB persistence and reload

  # NOTE: RDB reload test cases are currently commented out in the Examples tables.
  # Uncomment them when RDB persistence for scoring is fully implemented and ready to test.

  # ===========================================================================================
  # TEST SCORER TESTS
  # ===========================================================================================
  # These tests use debug scoring functions that return individual scoring statistics:
  # - TEST_NUM_DOCS: returns the number of documents in the index
  # - TEST_NUM_TERMS: returns the number of unique terms in the index
  # - TEST_AVG_DOC_LEN: returns the average document length
  # - TEST_SUM_IDF: returns the sum of IDF values (TFIDF-style IDF)
  # - TEST_SUM_BM25_IDF: returns the sum of BM25-style IDF values
  #
  # These scorers are registered via a debug command and prove that the engine provides
  # correct values to the scoring functions.
  # ===========================================================================================

  Background:
    Given the RediSearchDisk module is loaded
    And the test scorers are registered
    And I create an index "idx" with schema field "content" as TEXT

  # Simple scorers that return a fixed expected score
  # - TEST_NUM_DOCS: 3 documents -> score 3.0
  # - TEST_NUM_TERMS: unique terms (hello, world, again, more) -> score 4.0
  # - TEST_AVG_DOC_LEN: doc lengths (2, 3, 4) -> average 3.0
  Scenario Outline: <scorer> scorer returns correct value (<reload_policy>)
    Given I add documents to index "idx":
      | id   | field   | value                  |
      | doc1 | content | hello world            |
      | doc2 | content | hello world again      |
      | doc3 | content | hello world again more |
    When I apply reload policy "<reload_policy>"
    And the test scorers are registered
    And I search the index "idx" for "hello" with scorer "<scorer>" and scores
    Then I should get 3 results
    And all documents should have score <expected_score>

    Examples:
      | scorer          | expected_score | reload_policy |
      | TEST_NUM_DOCS   | 3.0            | none          |
      # Skipped: rdb reload not supported for this scorer yet
      # | TEST_NUM_DOCS   | 3.0            | rdb           |
      | TEST_NUM_TERMS  | 4.0            | none          |
      # | TEST_NUM_TERMS  | 4.0            | rdb           |
      | TEST_AVG_DOC_LEN| 3.0            | none          |
      # | TEST_AVG_DOC_LEN| 3.0            | rdb           |

  # IDF scorers with term appearing in all documents
  # IDF values are computed dynamically based on the formula
  Scenario Outline: <scorer> scorer returns correct IDF value (<reload_policy>)
    Given I add documents to index "idx":
      | id   | field   | value                  |
      | doc1 | content | hello world            |
      | doc2 | content | hello world again      |
      | doc3 | content | hello world again more |
    When I apply reload policy "<reload_policy>"
    And the test scorers are registered
    And I search the index "idx" for "hello" with scorer "<scorer>" and scores
    Then I should get 3 results
    And all documents should have the expected <idf_type> IDF score for 3 total documents and 3 containing the term

    Examples:
      | scorer          | idf_type | reload_policy |
      | TEST_SUM_IDF    | TFIDF    | none          |
      # | TEST_SUM_IDF    | TFIDF    | rdb           |
      | TEST_SUM_BM25_IDF| BM25    | none          |
      # | TEST_SUM_BM25_IDF| BM25    | rdb           |

  # IDF scorers with term appearing in subset of documents (1 out of 3)
  Scenario Outline: <scorer> scorer with term in subset of documents (<reload_policy>)
    Given I add documents to index "idx":
      | id   | field   | value            |
      | doc1 | content | hello world      |
      | doc2 | content | world again      |
      | doc3 | content | world again more |
    When I apply reload policy "<reload_policy>"
    And the test scorers are registered
    And I search the index "idx" for "hello" with scorer "<scorer>" and scores
    Then I should get 1 results
    And all documents should have the expected <idf_type> IDF score for 3 total documents and 1 containing the term

    Examples:
      | scorer          | idf_type | reload_policy |
      | TEST_SUM_IDF    | TFIDF    | none          |
      # | TEST_SUM_IDF    | TFIDF    | rdb           |
      | TEST_SUM_BM25_IDF| BM25    | none          |
      # | TEST_SUM_BM25_IDF| BM25    | rdb           |

  # ===========================================================================================
  # BM25STD RELEVANCE ORDERING TEST
  # ===========================================================================================
  # This test validates BM25STD behavior through relative ranking rather than exact scores.
  # BM25 intuition: documents with higher term frequency relative to document length rank higher.
  # ===========================================================================================

  Scenario Outline: BM25STD scorer produces correct relative ranking <case>
    # doc1: Short doc, high term density for "hello" (2 out of 3 words = 67%)
    # doc2: Medium doc, medium term density (2 out of 5 words = 40%)
    # doc3: Long doc, low term density (2 out of 8 words = 25%)
    # doc4: Only 1 "hello" in short doc (1 out of 2 words = 50%)
    Given I add documents to index "idx":
      | id   | field   | value                                  |
      | doc1 | content | hello hello world                      |
      | doc2 | content | hello hello world foo bar              |
      | doc3 | content | hello hello world foo bar baz qux quux |
      | doc4 | content | hello world                            |
    When I apply reload policy "<reload_policy>"
    And I search the index "idx" for "hello" with scorer "BM25STD" and scores
    Then I should get 4 results
    # Expected ranking by BM25STD (higher is better):
    # - doc1 should rank highest (high TF, short doc)
    # - doc2 should rank second (high TF, medium doc)
    # - doc4 should be in middle (low TF, but very short doc)
    # - doc3 should rank lower (high TF, but very long doc)
    And the result "doc1" should have the highest score
    And the result "doc3" should have the lowest score
    And all scores should be positive
    And the results should be ordered by score descending

    Examples:
      | case                       | reload_policy |
      | without RDB reload         | none          |
      # | with RDB reload after add  | rdb           |
