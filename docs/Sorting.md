# Sorting by Indexed Fields

As of RediSearch 0.15, it is possible to bypass the scoring function mechanism, and order search results by the value of different document properties (fields) directly - even if the sorting field is not used by the query. For example, you can search for first name and sort by last name.

## Declaring Sortable Fields

When creating the index with `FT.CREATE`, you can declare `TEXT` and `NUMERIC` properties to be `SORTABLE`. When a property is sortable, we can later decide to order the results by its values. For example, in the following schema:

```
> FT.CREATE users SCHEMA first_name TEXT last_name TEXT SORTABLE age NUMERIC SORTABLE
```

The fields `last_name` and `age` are sortable, but `first_name` isn't. This means we can search by either first and/or last name, and sort by last name or age.

### Note on sortable TEXT fields

In the current implementation, when declaring a sortable field, its content gets copied into a special location in the index, for fast access on sorting. This means that making long text fields sortable is very expensive, and you should be careful with it.

Also, note that text fields get normalized and lowercased in a Unicode-safe way when stored for sorting and currently there is no way to change this behaviour. This means that `America` and `america` are considered equal in terms of sorting.

## Specifying SORTBY

If an index includes sortable fields, you can add the `SORTBY` parameter to the search request (outside the query body), and order the results by it. This overrides the scoring function mechanism, and the two cannot be combined. If `WITHSCORES` is specified along with `SORTBY`, the scores returned are simply the relative position of each result in the result set.

The syntax for `SORTBY` is:

```
SORTBY {field_name} [ASC|DESC]
```

* field_name must be a sortable field defined in the schema.

* `ASC` means the order will be ascending, `DESC` that it will be descending.

* The default ordering is `ASC` if not specified otherwise.

## Quick example

```
> FT.CREATE users SCHEMA first_name TEXT SORTABLE last_name TEXT age NUMERIC SORTABLE

# Add some users
> FT.ADD users user1 1.0 FIELDS first_name "alice" last_name "jones" age 35
> FT.ADD users user2 1.0 FIELDS first_name "bob" last_name "jones" age 36

# Searching while sorting

# Searching by last name and sorting by first name
> FT.SEARCH users "@last_name:jones" SORTBY first_name DESC

# Searching by both first and last name, and sorting by age
> FT.SEARCH users "alice jones" SORTBY age ASC

```
