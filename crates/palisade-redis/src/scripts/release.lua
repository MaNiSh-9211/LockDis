-- Ownership-checked release. The token comparison is what prevents a
-- caller whose lease already expired from deleting the CURRENT holder's
-- lock.
-- KEYS[1] = lock key
-- ARGV[1] = owner token
--
-- Returns 1 if released, 0 if we no longer own it.

if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
end
return 0
