-- Takes a JSON value and sets it to the root using msgpack

local js = ARGV[1]
local v = cjson.decode(js)
local mp = cmsgpack.pack(v)
redis.call('SET', KEYS[1], mp)
return 'OK'
