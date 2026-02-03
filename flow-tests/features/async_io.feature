Feature: RediSearchDisk Async IO Control
  As a developer
  I want to test that async IO can be controlled via FT.DEBUG DISK_IO_CONTROL
  So that I can verify both async and sync read paths work correctly

  Scenario: Search works with async IO disabled
    Given the RediSearchDisk module is loaded
    When I disable async disk IO
    And I create an index "idx" with schema field "content" as TEXT
    And I add a document "doc1" with field "content" set to "hello world"
    And I add a document "doc2" with field "content" set to "hello redis"
    And I add a document "doc3" with field "content" set to "goodbye world"
    And I search the index "idx" for "hello"
    Then the results should only contain:
      | doc_id |
      | doc1   |
      | doc2   |
    When I search the index "idx" for "world"
    Then the results should only contain:
      | doc_id |
      | doc1   |
      | doc3   |

  Scenario: Search works with async IO enabled
    Given the RediSearchDisk module is loaded
    When I enable async disk IO
    And I create an index "idx" with schema field "content" as TEXT
    And I add a document "doc1" with field "content" set to "hello world"
    And I add a document "doc2" with field "content" set to "hello redis"
    And I add a document "doc3" with field "content" set to "goodbye world"
    And I search the index "idx" for "hello"
    Then the results should only contain:
      | doc_id |
      | doc1   |
      | doc2   |
    When I search the index "idx" for "world"
    Then the results should only contain:
      | doc_id |
      | doc1   |
      | doc3   |

  Scenario: Async IO status can be checked
    Given the RediSearchDisk module is loaded
    When I disable async disk IO
    Then async disk IO status should be "disabled"
    When I enable async disk IO
    Then async disk IO status should be "enabled"

