Feature: Document expiration functionality
  As a developer
  I want to test document expiration in disk-based indexes
  So that I can ensure expired documents are not returned in search results

  Scenario: Document disappears from search after it expires
    When I create an index "idx" with schema field "title" as TEXT
    And I add a document "doc1" with field "title" set to "hello world"
    And I set expiration on "doc1" to 3 seconds
    And I search the index "idx" for "hello"
    Then I should get 1 result
    When I wait 6 seconds
    And I search the index "idx" for "hello"
    Then I should get 0 results

  # Timeline:
  #   t=0        t=2        t=5        t=6        t=10
  #   |----------|----------|----------|----------|
  #   create     reduce     expires    check      (would have expired here)
  #              TTL to 3   (2+3)      = gone
  Scenario: Reducing expiration time causes document to expire sooner
    When I create an index "idx" with schema field "title" as TEXT
    And I add a document "doc1" with field "title" set to "hello world"
    And I set expiration on "doc1" to 10 seconds
    And I wait 2 seconds
    And I search the index "idx" for "hello"
    Then I should get 1 result
    When I set expiration on "doc1" to 3 seconds
    And I wait 4 seconds
    And I search the index "idx" for "hello"
    Then I should get 0 results
