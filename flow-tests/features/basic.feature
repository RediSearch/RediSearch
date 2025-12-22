Feature: RediSearchDisk Basic Functionality
  As a developer
  I want to test basic RediSearchDisk operations
  So that I can ensure the module works correctly

  Scenario: Module loads successfully
    Given the RediSearchDisk module is loaded
    Then the environment should be available

  Scenario: Create a basic search index
    Given the RediSearchDisk module is loaded
    When I create an index "idx" with schema field "title" as TEXT
    And I add a document "doc1" with field "title" set to "hello world"
    And I search the index "idx" for "hello"
    Then I should get 1 result
    And the first result should be "doc1"

  Scenario: Index and search multiple documents
    Given the RediSearchDisk module is loaded
    When I create an index "idx" with schema field "content" as TEXT
    And I add a document "doc1" with field "content" set to "redis search"
    And I add a document "doc2" with field "content" set to "redis database"
    And I add a document "doc3" with field "content" set to "search engine"
    And I search the index "idx" for "redis"
    Then I should get 2 results
    When I search the index "idx" for "search"
    Then I should get 2 results

  Scenario: Index with tag fields
    Given the RediSearchDisk module is loaded
    When I create an index "idx" with fields:
      | field    | type |
      | title    | TEXT |
      | category | TAG  |
    And I add a document "item1" with fields:
      | field    | value       |
      | title    | item one    |
      | category | electronics |
    And I add a document "item2" with fields:
      | field    | value     |
      | title    | item two  |
      | category | books     |
    And I add a document "item3" with fields:
      | field    | value       |
      | title    | item three  |
      | category | electronics |
    And I search the index "idx" for tag "category" with value "electronics"
    Then I should get 2 results

  @skip
  Scenario: Delete documents from index
    Given the RediSearchDisk module is loaded
    When I create an index "idx" with schema field "content" as TEXT
    And I add a document "doc1" with field "content" set to "test document"
    And I add a document "doc2" with field "content" set to "another test"
    And I search the index "idx" for "test"
    Then I should get 2 results
    When I delete the document "doc1"
    And I search the index "idx" for "test"
    Then I should get 1 result
    And the first result should be "doc2"

  Scenario: Update documents in index
    Given the RediSearchDisk module is loaded
    When I create an index "idx" with schema field "content" as TEXT
    And I add a document "doc1" with field "content" set to "original content"
    And I search the index "idx" for "original"
    Then I should get 1 result
    When I update document "doc1" with field "content" set to "updated content"
    And I search the index "idx" for "updated"
    Then I should get 1 result
    When I search the index "idx" for "original"
    Then I should get 0 results

