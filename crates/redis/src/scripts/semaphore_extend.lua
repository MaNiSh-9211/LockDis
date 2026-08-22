-- Semaphore extend: refresh only our own expiry score.
-- KEYS[1] = zset, ARGV[1] = token, ARGV[2] = new ttl ms
-- Returns 1 extended, 0 if we hold no permit.

local time = redis.call('TIME')
local now = tonumber(time[1]) * 1000 + math.floor(tonumber(time[2]) / 1000)

if redis.call('ZSCORE', KEYS[1], ARGV[1]) ~= false then
  redis.call('ZADD', KEYS[1], now + tonumber(ARGV[2]), ARGV[1])
  redis.call('PEXPIRE', KEYS[1], ARGV[2])
  return 1
end
return 0
