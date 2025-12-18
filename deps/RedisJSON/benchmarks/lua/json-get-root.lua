-- Gets a root JSON

local js = redis.call('GET', KEYS[1])
return js