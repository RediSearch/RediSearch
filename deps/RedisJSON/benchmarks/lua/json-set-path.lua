-- Sets a JSON value to a JSON key
-- Path to value is given by ARGV

-- Get the stored value and unpack it
local js = redis.call('GET', KEYS[1])
local v = cjson.decode(js)

-- Parse the path
local p = v
local c = v
local token, n
while #ARGV > 1 do
  p = c
  token = table.remove(ARGV,1)
  n = tonumber(token)
  if n then
    c = p[n+1]
  else
    c = p[token]
  end
end

-- Get the new value
local nv = cjson.decode(ARGV[1])

-- Replace the value in the parent
  if n then
    p[n+1] = nv
  else
    p[token] = nv
  end

-- Encode the value
local njs = cjson.encode(v)

-- Store it
redis.call('SET', KEYS[1], njs)

return 'OK'