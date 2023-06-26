---
title: "Commands"
linkTitle: "Commands"
weight: 1.5
description: >
    Commands Overview
---

### Search and Query API

Details on module's [commands](/commands/?group=search) can be filtered for a specific module or command, e.g., [`FT`](/commands/?name=ft.).
The details also include the syntax for the commands, where:

*   Command and subcommand names are in uppercase, for example `FT.CREATE`
*   Optional arguments are enclosed in square brackets, for example `[NOCONTENT]`
*   Additional optional arguments are indicated by three period characters, for example `...`

The query commands, i.e. `FT.SEARCH` and `FT.AGGREGATE`, require an index name as their first argument, then a query, i.e. `hello|world`, and finally additional parameters or attributes.

See the [quick start page](/redisearch/quick_start) on creating and indexing and start searching.

See the [reference page](/redisearch/reference) for more details on the various parameters.
