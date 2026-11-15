# Design

Parse one optional selector using existing command conventions. Match row names by exact bytes, case sensitively. Render selected rows in their existing order, with their existing types and complete nested values. Repeated fields appear once.

Share selection parsing and matching between the local renderer and coordinator. Keep full shard replies and existing aggregation rules; filter only the final coordinator reply. Validate every requested field against each shard reply before returning success. Copy request strings into the existing map-reduce request context and free them with its cleanup callback.

Unknown FIELDS names and known but unavailable rows return distinct errors. PREFIX with no matches returns an empty collection; an empty prefix matches all rows. ACL, alias resolution, missing-index errors and shard errors follow the existing FT.INFO path. Selection adds no readiness guarantee, index incarnation or snapshot across shards.

This first version reduces client traffic and decoding. It does not promise reduced statistics collection or shard traffic. No new shard protocol or response version is introduced. Additional server or proxy releases may be needed for Redis Software availability; do not infer availability from the module change alone.

Exact FIELDS selection serves the issue's readiness observations in one call. PREFIX also allows coarse selection without introducing glob syntax or nested paths.
