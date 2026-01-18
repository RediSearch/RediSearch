Feature: Delete document functionality
  As a developer
  I want to test the flow of deletion of documents
  So that I can ensure the disk document deletion flow works correctly.

  Scenario: After deleting documents they should not be returned
    When I create an index "idx" with schema field "title" as TEXT
    And I add a document "doc1" with field "title" set to "hello world"
    And I add a document "doc2" with field "title" set to "hello world"
    And I add a document "doc3" with field "title" set to "hello world"
    And I search the index "idx" for "hello"
    Then I should get 3 results
    When I delete "doc1"
    And I search the index "idx" for "hello"
    Then I should get results with keys ["doc2", "doc3"]
    When I delete "doc2"
    And I search the index "idx" for "hello"
    Then I should get results with keys ["doc3"]
    When I delete "doc3"
    And I search the index "idx" for "hello"
    Then I should get 0 results

  Scenario: Deleting documents should update the number of indexed documents
    When I create an index "idx" with schema field "title" as TEXT
    And I add a document "doc1" with field "title" set to "hello world"
    And I add a document "doc2" with field "title" set to "redis search"
    And I add a document "doc3" with field "title" set to "what is up?"
    Then index "idx" should have 3 documents
    When I delete "doc1"
    Then index "idx" should have 2 documents
    When I delete "doc2"
    Then index "idx" should have 1 document
    When I delete "doc3"
    Then index "idx" should have 0 documents
    When I delete "doc4"
    Then index "idx" should have 0 documents
