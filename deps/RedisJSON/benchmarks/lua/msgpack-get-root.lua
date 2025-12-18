-- Gets a msgpacked root and returns it as JSON

local mp = redis.call('GET', KEYS[1])
local v = cmsgpack.unpack(mp)
local js = cjson.encode(v)
return js