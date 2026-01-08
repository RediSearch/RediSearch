Feature: Debug commands on Flex
  As a developer
  I want to test debug commands that behave differently on disk, upon which we
  rely on in tests and debugging operations.

  Scenario: Debug command GET_MAX_DOC_ID on disk index
    Given the RediSearchDisk module is loaded
    When I create an index "idx" with schema field "title" as TEXT
    Then the max doc-id for "idx" should be 1
    When I add a document "doc1" with field "title" set to "first"
    And I add a document "doc2" with field "title" set to "second"
    And I add a document "doc3" with field "title" set to "third"
    Then the max doc-id for "idx" should be 4
    When I delete the document "doc2"
    Then the max doc-id for "idx" should be 4
    When I add a document "doc4" with field "title" set to "fourth"
    And I add a document "doc5" with field "title" set to "fifth"
    Then the max doc-id for "idx" should be 6

  Scenario: Debug command DUMP_DELETED_IDS on disk index
    Given the RediSearchDisk module is loaded
    When I create an index "idx" with schema field "title" as TEXT
    Then the deleted-ids for "idx" should be empty
    When I add a document "doc1" with field "title" set to "first"
    And I add a document "doc2" with field "title" set to "second"
    And I add a document "doc3" with field "title" set to "third"
    And I add a document "doc4" with field "title" set to "fourth"
    And I add a document "doc5" with field "title" set to "fifth"
    Then the deleted-ids for "idx" should be empty
    When I delete the document "doc2"
    Then the deleted-ids for "idx" should be [2]
    When I delete the document "doc4"
    Then the deleted-ids for "idx" should be [2, 4]
    When I delete the document "doc1"
    And I delete the document "doc5"
    Then the deleted-ids for "idx" should be [1, 2, 4, 5]
