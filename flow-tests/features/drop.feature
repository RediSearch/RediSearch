Feature: RediSearchDisk Index Deletion
  As a developer
  I want to test index deletion functionality
  So that I can ensure indexes are properly removed from disk

  Scenario: Drop an empty index
    Given the RediSearchDisk module is loaded
    When I create an index "idx" with schema field "title" as TEXT
    Then the index "idx" should exist
    When I drop the index "idx"
    Then the index "idx" should not exist
    And the index "idx" database files should be deleted

  Scenario: Drop an index with data
    Given the RediSearchDisk module is loaded
    When I create an index "idx" with schema field "content" as TEXT
    And I add a document "doc1" with field "content" set to "test data"
    And I add a document "doc2" with field "content" set to "more data"
    And I search the index "idx" for "data"
    Then I should get 2 results
    When I drop the index "idx"
    Then the index "idx" should not exist
    And the index "idx" database files should be deleted

  @skip
  Scenario: Drop index and recreate with same name
    Given the RediSearchDisk module is loaded
    When I create an index "idx" with schema field "title" as TEXT
    And I add a document "doc1" with field "title" set to "original"
    And I search the index "idx" for "original"
    Then I should get 1 result
    When I drop the index "idx"
    Then the index "idx" should not exist
    When I create an index "idx" with schema field "title" as TEXT
    Then the index "idx" should exist
    When I search the index "idx" for "original"
    Then I should get 1 result

  Scenario: Drop multiple indexes independently
    Given the RediSearchDisk module is loaded
    When I create an index "idx1" with schema field "field1" as TEXT
    And I create an index "idx2" with schema field "field2" as TEXT
    And I add a document "doc1" with field "field1" set to "index one"
    And I add a document "doc2" with field "field2" set to "index two"
    Then the index "idx1" should exist
    And the index "idx2" should exist
    When I drop the index "idx1"
    Then the index "idx1" should not exist
    And the index "idx1" database files should be deleted
    And the index "idx2" should exist
    When I search the index "idx2" for "index"
    Then I should get 1 result

  @skip
  Scenario: Drop index with complex schema
    Given the RediSearchDisk module is loaded
    When I create an index "idx" with fields:
      | field    | type    |
      | title    | TEXT    |
    #   | category | TAG     |
    #   | price    | NUMERIC |
    And I add a document "item1" with fields:
      | field    | value       |
      | title    | laptop      |
      | category | electronics |
      | price    | 999         |
    And I add a document "item2" with fields:
      | field    | value |
      | title    | book  |
      | category | media |
      | price    | 20    |
    When I search the index "idx" for "laptop"
    Then I should get 1 result
    When I drop the index "idx"
    Then the index "idx" should not exist
    And the index "idx" database files should be deleted

