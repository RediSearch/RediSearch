---
title: "Use cases"
linkTitle: "Use cases"
weight: 5
description: >
    Search and Query use cases
aliases:
  - /docs/stack/search/reference/query_syntax/  
  - /docs/stack/use-cases/  
---

**Application search and external secondary index** 

RediSearch supports application search whether the source of record is another database or Redis itself. You can use RediSearch as an external secondary index (for example, for indexing numeric or full-text data) and as a full-text search engine.

**Secondary index for Redis data**

You can represent your data model using Redis hashes and RedisJSON documents. You can then declare secondary indexes to support various queries on your data set. RediSearch 2.0 updates indexes automatically whenever a hash/JSON document (that matches the indexes) is updated. 

**Geo-distributed search**

In geo-distributed search, hashes/JSON documents are handled in the usual [Active-Active manner](https://docs.redis.com/latest/rs/databases/active-active/). The index follows whatever is written in the documents in the database. Create an index on each database. Then, add synonyms (if used) to each database. 

**Unified search**

You can use RediSearch to search across several source systems, like file servers, content management systems (CMS), or customer relationship management (CRM) systems. Process source data in batches (for example, using ETL tools) or as live streams (for example, using Kafka or Redis streams). 

**Analytics**

Data often originates from several source systems. RediSearch can provide a materialized view of dimensions and facts. You can slice-and-dice data based on dimensions, group by dimension, and apply aggregations to facts.

{{% alert title="RediSearch for faceted search" color="warning" %}}
 
Facets are multiple explicit dimensions implemented as tags in RediSearch. You can slice-and-dice data based on facets, achievable via RediSearch aggregations (`COUNT`, `TOLIST`, `FIRST_VALUE`, `RANDOM_SAMPLE`).

{{% /alert %}}

**Ephemeral search (retail)**

When the user logs on to the site, the purchase-search history is populated into a RediSearch index from another datastore. This requires lightweight index creation, index expiry, and quick document indexing.

The application/service creates a temporary and user-specific full-text index in RediSearch when a user logs in. The application/service has direct access to the user-specific index and the primary datastore. When the user logs out of the service, the index is explicitly removed. Otherwise, the index expires after a while (for example, after the user's session expires). 

Using RediSearch for this type of application provides these benefits: 

- Search index is only populated when needed. 
- Only a small portion (for example, 2%) of users are actually active. 
- Users are only active for a comparatively short period of time.
- Small number of documents indexed and so very cost effective in comparison to a persistent search index. 

**Real-time inventory (retail)**

In real-time inventory retail, the key question is product availability: "What is available where?" The challenges with such projects are performance and accuracy. RediSearch allows for real-time searching and aggregations over millions of store/SKU combinations.

You can establish real-time event capture from legacy inventory system to RediSearch and then have several inventory services query RediSearch. Then, you can use combined queries, for example, item counts, price ranges, categories, and locations. Take advantage of geo-distributed search (Active-Active) for your remote store locations. 

Using RediSearch for this type of application provides these benefits: 

- Low-latency queries for downstream consumers like marketing, stores/e-commerce, and fulfillment 
- Immediate and higher consistency between stores and datacenters 
- Improved customer experience 
- Real-time/more sensible pricing decisions 
- Less shopping cart abandonment 
- Less remediation (refund, cancellation) 

**Real-time conversation analysis (telecom)**

Collect, access, store, and utilize communication data in real time. Capture network traffic and store it in a full-text index for the purposes of getting insights into the data.

Gather data using connection information gathering (source IPs, DNS) and conversation data gathering (Wireshark/TShark live capture). Then filter, transform, and store the conversation data in RediSearch to perform search queries and create custom dashboards for your analyses.

Using RediSearch for this type of application provides these benefits: 

- Insights into performance issues, security threats, and network faults 
- Improved service uptime and security 

**Research portal (academia)**

Research portals let users search for articles, research, specs, past solutions, and data to answer specific questions and take advantage of existing knowledge and history. 

To build such a system, you can use RediSearch indexes supporting tag queries, numeric range queries, geo-location queries, and full-text search (FTS). 

Using RediSearch for this type of application provides these benefits: 

- Create relevant, personalized search experiences all while enforcing internal and regulatory data governance policies 
- Increased productivity, security, and compliance  