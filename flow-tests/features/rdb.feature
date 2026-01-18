Feature: RDB Save and Load Functionality
  As a developer
  I want to test RDB persistence of RediSearchDisk state
  So that I can ensure disk-related index-data survives Redis restarts

  Scenario: Save and load index with documents
    Given the RediSearchDisk module is loaded
    When I create an index "idx" with schema field "title" as TEXT
    And I add a document "doc1" with field "title" set to "hello world"
    And I add a document "doc2" with field "title" set to "redis search"
    And I add a document "doc3" with field "title" set to "persistence test"
    And I restart Redis and reload the RDB
    Then the index "idx" should exist
    When I search the index "idx" for "hello"
    Then I should get 1 result

  @skip
  Scenario: Disk related data persists across RDB save/load
    Given the RediSearchDisk module is loaded
    When I create an index "idx" with schema field "title" as TEXT
    And I add a document "doc1" with field "title" set to "first"
    And I add a document "doc2" with field "title" set to "second"
    And I add a document "doc3" with field "title" set to "third"
    And I add a document "doc4" with field "title" set to "fourth"
    And I delete the document "doc2"
    And I delete the document "doc3"
    Then the max doc-id for "idx" should be 5
    Then the deleted-ids for "idx" should be [2, 3]
    When I restart Redis and reload the RDB
    Then the index "idx" should exist
    Then the max doc-id for "idx" should be 7
    # 2, 3 are deleted from the original deletion operations prior to the reload.
    # 1, 4 are not read from RDB since we use SST-based reloading.
    Then the deleted-ids for "idx" should be [2, 3]
