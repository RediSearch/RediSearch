-- Gets a value from a msgpacked key and returns it as JSON
-- Path to value is given by ARGV

-- Get the stored value and unpack it
local mp = redis.call('GET', KEYS[1])
local v = cmsgpack.unpack(mp)

-- Parse the path
local r = v
while #ARGV > 0 do
  local token = table.remove(ARGV,1)
  local n = tonumber(token)
  if n then
    r = r[n+1]
  else
    r = r[token]
  end
end

-- Serialize the reply to JSON
local js = cjson.encode(r)
return js