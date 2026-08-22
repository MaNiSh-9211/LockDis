-- Fair acquire with DEAD-HEAD DISCARD: when the lock is free, stale queue
-- entries whose heartbeat expired are popped and discarded before deciding,
-- so a dead waiter can never stall live ones (EDGE_CASES.md #2).
-- KEYS[1] = lock key, KEYS[2] = queue list, KEYS[3] = fence counter,
-- KEYS[4] = this waiter's heartbeat key
-- ARGV[1] = token, ARGV[2] = ttl ms, ARGV[3] = fence ttl ms, ARGV[4] = hb ttl ms
-- ARGV[5] = hb prefix (e.g. "{key}:hb:")
-- Returns {status, fence}: 2 handed off to us earlier, 1 granted now,
-- 0 still waiting (queued).

if redis.call('GET', KEYS[1]) == ARGV[1] then
  redis.call('PEXPIRE', KEYS[1], ARGV[2])
  return {2, redis.call('INCR', KEYS[3])}
end

if redis.call('EXISTS', KEYS[1]) == 0 then
  -- Discard dead entries from the tail (oldest side) until we hit a
  -- live waiter; a live head-of-line blocks everyone behind it.
  while redis.call('LLEN', KEYS[2]) > 0 do
    local tok = redis.call('LRANGE', KEYS[2], -1, -1)[1]
    if tok and redis.call('EXISTS', ARGV[5] .. tok) == 1 then
      break
    end
    redis.call('RPOP', KEYS[2])
  end

  local oldest = redis.call('LRANGE', KEYS[2], -1, -1)[1]
  if not oldest or oldest == ARGV[1] then
    if oldest then
      redis.call('LREM', KEYS[2], 1, ARGV[1])
    end
    redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2])
    return {1, redis.call('INCR', KEYS[3])}
  end
end

if redis.call('LPOS', KEYS[2], ARGV[1]) == false then
  redis.call('LPUSH', KEYS[2], ARGV[1])
end
redis.call('SET', KEYS[4], '1', 'PX', ARGV[4])
redis.call('PEXPIRE', KEYS[2], tonumber(ARGV[2]) * 10)
return {0, 0}
