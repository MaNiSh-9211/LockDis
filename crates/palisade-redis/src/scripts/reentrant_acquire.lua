-- Reentrant grant: hash-based hold counting; same token re-enters.
-- KEYS[1] = lock hash, KEYS[2] = fence counter
-- ARGV[1] = token, ARGV[2] = ttl ms, ARGV[3] = fence ttl ms
-- Returns {status, fence}: 1 fresh grant, 2 reentry, 0 held elsewhere.

if redis.call('EXISTS', KEYS[1]) == 0 then
  redis.call('HSET', KEYS[1], 'owner', ARGV[1], 'count', 1)
  redis.call('PEXPIRE', KEYS[1], ARGV[2])
  return {1, redis.call('INCR', KEYS[2])}
end

if redis.call('HGET', KEYS[1], 'owner') == ARGV[1] then
  redis.call('HINCRBY', KEYS[1], 'count', 1)
  redis.call('PEXPIRE', KEYS[1], ARGV[2])
  return {2, redis.call('INCR', KEYS[2])}
end

return {0, 0}
