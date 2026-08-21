-- Atomic grant + fencing-token allocation.
-- KEYS[1] = lock key
-- KEYS[2] = fence counter key (same hash slot as KEYS[1])
-- ARGV[1] = owner token (stored payload)
-- ARGV[2] = lease TTL in ms
-- ARGV[3] = fence counter TTL in ms (kept > lease TTL so the counter
--           always outlives its lock; refreshed on every grant/extend)
--
-- Returns {status, fence}:
--   1 = granted to a new holder
--   2 = re-granted to the SAME token (lease refresh, new fence issued)
--   0 = held elsewhere

if redis.call('SET', KEYS[1], ARGV[1], 'NX', 'PX', ARGV[2]) then
  local fence = redis.call('INCR', KEYS[2])
  redis.call('PEXPIRE', KEYS[2], ARGV[3])
  return {1, fence}
end

if redis.call('GET', KEYS[1]) == ARGV[1] then
  redis.call('PEXPIRE', KEYS[1], ARGV[2])
  local fence = redis.call('INCR', KEYS[2])
  redis.call('PEXPIRE', KEYS[2], ARGV[3])
  return {2, fence}
end

return {0, 0}
