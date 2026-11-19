# Add NESTED groups to JSON indexes

## Why

A JSON index on an array of objects is a multi-value index. Each field keeps the
values of all array elements together. A query with two conditions matches a
document when each condition holds on some element. The elements can be
different.

Example: a company document holds `metrics: [{k, r, v}, ...]`. The query
`@k:{headcount} @v:[(100 +inf]` also returns a company whose headcount is 90 and
whose revenue is 500. This result is wrong for the user.

Users who index arrays of objects are affected. Today they must split each
element into its own key, or filter the results in the application.

## What Changes

- **`FT.CREATE`**: a new schema item `<path> AS <group> NESTED <n>`. The next
  `n` field definitions are the members of the group. The path of a member
  starts at one element: with the group `$.metrics[*] AS metrics NESTED 3`, the
  member `$.k AS k TAG` is the `k` of each element of `metrics`.
- **Query**: `@group:( ... )`, as in `@metrics:(@k:{headcount} @v:[(100 +inf])`.
  The document matches only when one element satisfies all the conditions.
- **`FT.INFO`**: each member shows `NESTED <group>` and `NESTED_PATH <path>`.
  The group itself is not an attribute.
- **`FT.EXPLAIN`**: the conditions of a group show below `SAME-ELEMENT:@<group>`.
- **RDB**: encoding version 30. A schema with a group saves a group section.
- **Limit**: DIALECT 2 or higher. DIALECT 1 returns
  `Conditions on a NESTED group require DIALECT 2 or higher`.
- **Limit**: members are TAG or NUMERIC only. A NUMERIC member is always
  SORTABLE.
- **Limit**: RAM only. An index on disk rejects `NESTED`.
- **Limit**: JSON only. `FT.ALTER` cannot add a group.
- **Limit**: a query on a member outside `@group:( ... )` behaves as a
  multi-value field does today.
- **Non-goal**: TEXT, GEO, VECTOR or GEOSHAPE members.
- **Non-goal**: a group inside a group.
- **Non-goal**: a change to queries that do not name a group. Their results and
  their error texts stay the same.
