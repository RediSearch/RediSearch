Feature: RediSearchDisk Compaction (GC) and Term Stats
  As a developer
  I want to test that running GC compaction correctly updates term-level statistics
  So that search result scores remain accurate after inverted index cleanup

  # ===========================================================================================
  # COMPACTION TERM STATS TESTS
  # ===========================================================================================
  # Key insight: Document deletion marks the document as deleted, but the inverted index
  # still contains entries for deleted documents. GC/Compaction cleans up the inverted
  # index entries and updates term-level stats (term doc counts in the trie).
  #
  # What GC updates:
  # - numDocs per term in trie: decremented when GC removes posting list entries
  # - numTerms: decremented when terms become completely empty (all docs deleted)
  #
  # Test scorer:
  # - TEST_SUM_BM25_IDF: returns sum of BM25 IDF values for matched terms
  #   BM25 IDF formula: log(1 + (totalDocs - termDocs + 0.5) / (termDocs + 0.5))
  #   This value changes when termDocs (from trie) changes, allowing us to detect GC updates.
  # ===========================================================================================

  Background:
    Given the RediSearchDisk module is loaded
    And the test scorers are registered

  # Test that GC updates term-level stats after compaction (ASCII terms with NOSTEM)
  # - TEST_SUM_BM25_IDF: detects termDocs changes via BM25 IDF formula
  #   Before GC: totalDocs=3, termDocs=3 -> log(1+(3-3+0.5)/(3+0.5)) ≈ 0.134
  #   After GC:  totalDocs=2, termDocs=2 -> log(1+(2-2+0.5)/(2+0.5)) ≈ 0.182
  # - TEST_NUM_TERMS: detects numTerms changes when terms become empty
  #   Setup: doc3 contains unique term "unique"
  #   Initial numTerms = 4 (common, aaa, bbb, unique)
  #   After GC: "unique" removed -> numTerms = 3
  # NOSTEM is used to ensure consistent term handling for both scorers.
  Scenario Outline: GC updates <stat_type> after compaction
    Given I create an index "idx" with schema field "content" as TEXT NOSTEM
    And I add documents to index "idx":
      | id   | field   | value         |
      | doc1 | content | common aaa    |
      | doc2 | content | common bbb    |
      | doc3 | content | common unique |
    # Initial state
    When I search the index "idx" for "common" with scorer "<scorer>" and scores
    Then I should get 3 results
    And all documents should have approximate score <initial_score>
    # After deletion (GC hasn't run yet)
    When I delete "doc3"
    And I search the index "idx" for "common" with scorer "<scorer>" and scores
    Then I should get 2 results
    And all documents should have approximate score <after_delete_score>
    # After GC: stats are updated
    When I run GC on index "idx"
    And I search the index "idx" for "common" with scorer "<scorer>" and scores
    Then I should get 2 results
    And all documents should have approximate score <after_gc_score>

    Examples:
      | stat_type      | scorer            | initial_score | after_delete_score | after_gc_score |
      | term doc count | TEST_SUM_BM25_IDF | 0.134         | 0.134              | 0.182          |
      | numTerms       | TEST_NUM_TERMS    | 4.0           | 4.0                | 3.0            |

  # Test that GC correctly handles UTF-8 terms (naïve, München, 日本語)
  # This verifies that term stats are correctly updated for multi-byte UTF-8 characters
  # after compaction removes documents containing these terms.
  # NOSTEM is required to prevent the stemmer from modifying UTF-8 terms.
  Scenario Outline: GC updates <stat_type> for UTF-8 terms after compaction
    Given I create an index "idx" with schema field "content" as TEXT NOSTEM
    And I add documents to index "idx":
      | id   | field   | value           |
      | doc1 | content | common naïve    |
      | doc2 | content | common München  |
      | doc3 | content | common 日本語   |
    # Initial state
    When I search the index "idx" for "common" with scorer "<scorer>" and scores
    Then I should get 3 results
    And all documents should have approximate score <initial_score>
    # After deletion (GC hasn't run yet)
    When I delete "doc3"
    And I search the index "idx" for "common" with scorer "<scorer>" and scores
    Then I should get 2 results
    And all documents should have approximate score <after_delete_score>
    # After GC: stats are updated
    When I run GC on index "idx"
    And I search the index "idx" for "common" with scorer "<scorer>" and scores
    Then I should get 2 results
    And all documents should have approximate score <after_gc_score>

    Examples:
      | stat_type      | scorer            | initial_score | after_delete_score | after_gc_score |
      | term doc count | TEST_SUM_BM25_IDF | 0.134         | 0.134              | 0.182          |
      | numTerms       | TEST_NUM_TERMS    | 4.0           | 4.0                | 3.0            |
