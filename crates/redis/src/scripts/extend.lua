-- Ownership-checked lease extension (the primitive the watchdog uses).
-- KEYS[1] = lock key
-- KEYS[2] = fence counter key
-- ARGV[1] = owner token
-- ARGV[2] = new lease TTL in ms
-- ARGV[3] = fence counter TTL in ms
--
-- Returns 1 if extended, 0 if we no longer own it.

if redis.call('GET', KEYS[1]) == ARGV[1] then
  redis.call('PEXPIRE', KEYS[1], ARGV[2])
  redis.call('PEXPIRE', KEYS[2], ARGV[3])
  return 1
end
return 0
