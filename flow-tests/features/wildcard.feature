Feature: Wildcard Iterator Functionality
  As a developer
  I want to test wildcard queries that return all documents
  So that I can ensure the wildcard iterator works correctly

  Scenario: Wildcard query returns all documents
    Given the RediSearchDisk module is loaded
    When I create an index "idx" with schema field "content" as TEXT
    And I add a document "doc1" with field "content" set to "first document"
    And I add a document "doc2" with field "content" set to "second document"
    And I add a document "doc3" with field "content" set to "third document"
    And I search the index "idx" for "*"
    Then the results should only contain:
      | doc_id |
      | doc1   |
      | doc2   |
      | doc3   |

  Scenario: Wildcard query with empty index
    Given the RediSearchDisk module is loaded
    When I create an index "idx" with schema field "content" as TEXT
    And I search the index "idx" for "*"
    Then the results should be empty

  Scenario: Wildcard query after adding documents
    Given the RediSearchDisk module is loaded
    When I create an index "idx" with schema field "title" as TEXT
    And I add a document "doc1" with field "title" set to "hello"
    And I search the index "idx" for "*"
    Then the only result should be "doc1"
    When I add a document "doc2" with field "title" set to "world"
    And I search the index "idx" for "*"
    Then the results should only contain:
      | doc_id |
      | doc1   |
      | doc2   |

  @skip
  Scenario: Wildcard query with multiple fields
    Given the RediSearchDisk module is loaded
    When I create an index "idx" with fields:
      | field    | type |
      | title    | TEXT |
      | category | TAG  |
    And I add a document "item1" with fields:
      | field    | value       |
      | title    | laptop      |
      | category | electronics |
    And I add a document "item2" with fields:
      | field    | value |
      | title    | book  |
      | category | media |
    And I search the index "idx" for "*"
    Then the results should only contain:
      | doc_id |
      | item1  |
      | item2  |

  @skip # right now the not iterator doesn't use the disk wildcard iterator
  Scenario: Wildcard query with NOT operator
    Given the RediSearchDisk module is loaded
    When I create an index "idx" with schema field "content" as TEXT
    And I add a document "doc1" with field "content" set to "redis search"
    And I add a document "doc2" with field "content" set to "redis database"
    And I add a document "doc3" with field "content" set to "search engine"
    And I search the index "idx" for "redis -search"
    Then I should get 1 result
    And the only result should be "doc2"

  @skip # right now the optional iterator doesn't use the disk wildcard iterator
  Scenario: Wildcard query with Optional operator
    Given the RediSearchDisk module is loaded
    When I create an index "idx" with schema field "content" as TEXT
    And I add a document "doc1" with field "content" set to "redis search"
    And I add a document "doc2" with field "content" set to "redis database"
    And I add a document "doc3" with field "content" set to "search engine"
    And I search the index "idx" for "redis ~search"
    Then the results should only contain:
      | doc_id |
      | doc1   |
      | doc2   |

