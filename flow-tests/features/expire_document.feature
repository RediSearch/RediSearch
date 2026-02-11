Feature: Document expiration functionality
  As a developer
  I want to test document expiration in disk-based indexes
  So that I can ensure expired documents are not returned in search results

  Scenario: Document disappears from search after it expires
    When I create an index "idx" with schema field "title" as TEXT
    And I add a document "doc1" with field "title" set to "hello world"
    And I set expiration on "doc1" to 200 milliseconds
    And I search the index "idx" for "hello"
    Then I should get 1 result
    When I wait 500 milliseconds
    And I search the index "idx" for "hello"
    Then I should get 0 results

  # Timeline:
  #   t=0ms      t=100ms    t=400ms    t=500ms    t=1000ms
  #   |----------|----------|----------|----------|
  #   create     reduce     expires    check      (would have expired here)
  #              TTL to 300 (100+300)  = gone
  Scenario: Reducing expiration time causes document to expire sooner
    When I create an index "idx" with schema field "title" as TEXT
    And I add a document "doc1" with field "title" set to "hello world"
    And I set expiration on "doc1" to 1000 milliseconds
    And I wait 100 milliseconds
    And I search the index "idx" for "hello"
    Then I should get 1 result
    When I set expiration on "doc1" to 300 milliseconds
    And I wait 500 milliseconds
    And I search the index "idx" for "hello"
    Then I should get 0 results

  # This test verifies that lazily expired documents are filtered out during search.
  # With lazy expiration enabled, the document still exists in Redis but is marked as expired.
  # The expiration check filters out expired docs before they are returned in results.
  Scenario: Lazily expired documents are filtered out
    When I disable active expiration
    And I create an index "idx" with schema field "title" as TEXT
    And I add a document "doc1" with field "title" set to "hello world"
    And I add a document "doc2" with field "title" set to "hello redis"
    And I set expiration on "doc2" to 100 milliseconds
    And I wait 300 milliseconds
    And I search the index "idx" for "hello"
    Then the results should only contain:
      | doc_id |
      | doc1   |
    And the index should have 1 expired async read
