-- Fair release: free the lock, then hand off to the oldest LIVE waiter.
-- Dead waiters (heartbeat expired) are skipped and discarded.
-- KEYS[1] = lock key, KEYS[2] = queue list, KEYS[3] = fence counter
-- ARGV[1] = token, ARGV[2] = ttl ms, ARGV[3] = hb prefix (e.g. "{key}:hb:")
-- Returns 1 released (handoff may have occurred), -1 if we didn't own it.

if redis.call('GET', KEYS[1]) ~= ARGV[1] then
  return -1
end

redis.call('DEL', KEYS[1])

for i = 1, redis.call('LLEN', KEYS[2]) do
  local tok = redis.call('RPOP', KEYS[2])
  if tok and redis.call('EXISTS', ARGV[3] .. tok) == 1 then
    redis.call('SET', KEYS[1], tok, 'PX', ARGV[2])
    break
  end
end

return 1
