-- Reentrant release-all: drop every hold at once (ownership-checked).
-- KEYS[1] = lock hash, ARGV[1] = token
-- Returns 1 fully released, -1 if we no longer own it.

if redis.call('EXISTS', KEYS[1]) == 0 then
  return -1
end

if redis.call('HGET', KEYS[1], 'owner') == ARGV[1] then
  redis.call('DEL', KEYS[1])
  return 1
end
return -1
