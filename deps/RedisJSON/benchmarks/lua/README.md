# JSON with Redis server-side Lua

This is an implementation of ReJSON's `JSON.SET` and `JSON.GET` commands in **pure Redis Lua**. The
data is stored using Redis' native String data structure.

Yep, that's right, no modules needed.

The implementation consists of two variants: one uses JSON format for storing the data, while the
other stores it in MessagePack format.

## JSON storage

This variant stores the data in JSON format. It uses (when required) the built-in
[`cjson` library](/docs/manual/programmability/lua-api/#cjson-library) for decoding/encoding from/to JSON to/from
Lua's native table data type. It is made of the following scripts:

*   [`json-set-root.lua`](json-set-root.lua) sets the root to a JSON value. Because this variant
    stores serialized JSON format, this just calls Redis' [`SET`](https://redis.io/commands/set)
    command.
*   [`json-get-root.lua`](json-set-root.lua) gets the JSON value at the root. Similarly, this just
    calls Redis' [`GET`](https://redis.io/commands/set) command to return the serialized JSON value.
*   [`json-set-path.lua`](json-set-path.lua) sets the value at a path. To perform the update, the
    serialized JSON value is decoded, updated and then rendcoded as JSON before being stored.
*   [`json-get-path.lua`](json-get-path.lua) gets the value from a path. To perform the read, the
    serialized JSON value is decoded and then the value at the path is rencoded as JSON before being
    returned.

## MessagePack storage

These scripts use the [`cmsgpack` library](https://redis.io/commands/eval#cmsgpack) to decode/encode
the value before/after storing/fetching it in/from Redis. All scripts in this variant are
functionally identical to their respective above counterparts but include the required conversions
between formats:

*   [`msgpack-set-root.lua`](msgpack-set-root.lua)
*   [`msgpack-get-root.lua`](msgpack-set-root.lua)
*   [`msgpack-set-path.lua`](msgpack-set-path.lua)
*   [`msgpack-get-path.lua`](msgpack-get-path.lua)
