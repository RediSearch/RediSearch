---
title: "Use cases"
linkTitle: "Use cases"
weight: 5
description: >
    Search and query use cases
aliases: 
  - /docs/stack/use-cases/  
---

**Application search and external secondary index** 

Redis Stack supports application search, whether the source of record is Redis or another database. In the latter case, you can use Redis Stack as an external secondary index for numeric or full-text data.

**Secondary index for Redis data**

You can represent your data model using Redis hashes and JSON documents. You can then declare secondary indexes to support various queries on your data set. Redis Stack updates indexes automatically whenever a hash or JSON document that matches the indexes is added or updated. 

**Geo-distributed search**

In geo-distributed search, hashes and JSON documents are handled in the usual [active-active manner](https://docs.redis.com/latest/rs/databases/active-active/). The index follows whatever is written in the documents in the database. Create an index on each database, then add synonyms (if used) to each database. 

**Unified search**

You can use Redis Stack to search across several source systems, like file servers, content management systems (CMS), or customer relationship management (CRM) systems. You can process source data in batches using, for example, ETL tools, or as live streams (e.g., Kafka or Redis streams). 

**Analytics**

Data often originates from several source systems. Redis Stack can provide a unified view of dimensions and facts. You can query data based on dimensions, group by dimension, and apply aggregations to facts.

{{% alert title="Redis Stack for faceted search" color="warning" %}}
 
Facets are multiple explicit dimensions implemented as tags in RediSearch. You can query data based on facets using aggregations (`COUNT`, `TOLIST`, `FIRST_VALUE`, and `RANDOM_SAMPLE`).

{{% /alert %}}

**Ephemeral search (retail)**

When the user logs on to the site, the purchase-search history is populated into an index from another datastore. This requires lightweight index creation, index expiry, and quick document indexing.

The application/service creates a temporary and user-specific, full-text index when a user logs in. The application/service has direct access to the user-specific index and the primary datastore. When the user logs out of the service, the index is explicitly removed. Otherwise, the index expires after a while (for example, after the user's session expires). 

Using Redis Stack for this type of application provides these benefits: 

- Search index is only populated when needed. 
- Only a small portion (for example, 2%) of users are active at the same time. 
- Users are only active for a short period of time.
- A small number of documents are indexed, which is very cost effective in comparison to a persistent search index. 

**Real-time inventory (retail)**

In real-time inventory retail, the key question is product availability: "What is available where?" The challenges with such projects are performance and accuracy. Redis Stack allows for real-time searching and aggregations over millions of store/SKU combinations.

You can establish real-time event capture from a legacy inventory system to Redis Stack and then have several inventory services query it. Then, you can use combined queries such as item counts, price ranges, categories, and locations. Take advantage of geo-distributed search (Active-Active) for your remote store locations. 

Using Redis Stack for this type of application provides these benefits: 

- Low-latency queries for downstream consumers like marketing, stores/e-commerce, and fulfillment 
- Immediate and higher consistency between stores and data-centers 
- Improved customer experience 
- Real-time pricing decisions 
- Less shopping cart abandonment 
- Less remediation (refund, cancellation) 

**Real-time conversation analysis (telecom)**

Collect, access, store, and utilize communication data in real time. Capture network traffic and store it in a full-text index for the purposes of getting insights into the data.

Gather data using connection information gathering (source IPs, DNS) and conversation data gathering (Wireshark/TShark live capture). Then filter, transform, and store the conversation data in Redis to perform search queries and create custom dashboards for your analyses.

Using Redis Stack for this type of application provides these benefits: 

- Insights into performance issues, security threats, and network faults 
- Improved service uptime and security 

**Research portal (academia)**

Research portals let users search for articles, research, specifications, past solutions, and data to answer specific questions and take advantage of existing knowledge and history. 

To build such a system, you can use indexes supporting tag queries, numeric range queries, geo-location queries, and full-text search. 

Using Redis Stack for this type of application provides these benefits: 

- Create relevant, personalized search experiences while enforcing internal and regulatory data governance policies 
- Increased productivity, security, and compliance  