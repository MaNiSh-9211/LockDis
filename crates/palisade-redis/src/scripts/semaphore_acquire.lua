-- Semaphore acquire with server-side per-holder leases.
-- Expired holders are pruned using Redis TIME — client clocks are never
-- trusted. KEYS[1] = zset, KEYS[2] = fence counter
-- ARGV[1] = token, ARGV[2] = ttl ms, ARGV[3] = max permits, ARGV[4] = fence ttl ms
-- Returns {status, fence}: 1 permit granted, 0 full.

local time = redis.call('TIME')
local now = tonumber(time[1]) * 1000 + math.floor(tonumber(time[2]) / 1000)

redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', now)

if redis.call('ZCARD', KEYS[1]) < tonumber(ARGV[3]) then
  redis.call('ZADD', KEYS[1], now + tonumber(ARGV[2]), ARGV[1])
  redis.call('PEXPIRE', KEYS[1], ARGV[2])
  return {1, redis.call('INCR', KEYS[2])}
end

return {0, 0}
