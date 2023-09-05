---
title: "Query constructs"
linkTitle: "Query constructs"
weight: 1
description: >
    High-level comparison of FT.SEARCH and FT.AGGREGATE
---

After reading the quick start, you might be wondering why there's no `FT.QUERY` command, when the feature is called "search and query". Redis Stack's query capability is associated with the `FT.AGGREGATE` command, which, unlike the `FT.SEARCH` command, provides for more complex queries along the same lines as SQL's `SELECT` command. Let's look at some key differences between `FT.SEARCH` and `FT.AGGREGATE`.

### Key Differences:

While both commands operate on indexed data and can return results based on user-defined search conditions, `FT.SEARCH` is more about finding specific items, whereas `FT.AGGREGATE` is about analyzing or summarizing the data.

|             | `FT.SEARCH` | `FT.AGGREGATE` |
|:----        | :----       | :----          |
| Purpose     | Designed for direct search and retrieval of documents. | Tailored for processing and aggregating data from search results. |
| Complexity  | Offers a straightforward search. | Provides more advanced operations like grouping and aggregation. |
| Flexibility | Used when you know what you're looking for. | Used when you're trying to derive insights or summaries from your data. |

### FT.SEARCH

|||
|:----|:----|
| **Functionality** | Searches and retrieves documents from indexed fields in Redis. |
| **Return value** | Directly returns matching documents (or their IDs) based on the query. |
| **Filtering** | Can use conditions to narrow down search results. |
| **Scoring** | Has the capability to rank results based on their relevance to the search query. |
| **Highlighting** | Can highlight the terms in the results that match the search query. |
| **Usage example** | Retrieve all books with titles containing the word "Dystopia". |

### FT.AGGREGATE

|||
|:----|:----|
| **Functionality:** | Enables complex aggregations, transformations, and computations on search results. |
| **Return value:** | Does not necessarily return direct matches but rather aggregated or transformed data. |
| **Grouping:** | Can group results by specific fields, similar to the "GROUP BY" clause in SQL. |
| **Aggregation functions:** | Offers multiple functions like `COUNT`, `SUM`, `AVG`, etc., to perform calculations on grouped data. |
| **Filtering:** | Allows post-aggregation filtering to refine aggregated results. |
| **Sorting:** | Has the capability to sort aggregated results. |
| **Usage example:** | Count the number of books published each year by a specific author. |
