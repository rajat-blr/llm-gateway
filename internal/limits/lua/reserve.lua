-- Integer milli-tokens; Redis TIME supplies the clock for every instance.
local capacity, rate, amount, ttl = tonumber(ARGV[1])*1000, tonumber(ARGV[2]), tonumber(ARGV[3])*1000, tonumber(ARGV[4])
local clock = redis.call('TIME')
local now = tonumber(clock[1])*1000 + math.floor(tonumber(clock[2])/1000)
local state = redis.call('HMGET', KEYS[1], 'tokens', 'time', 'capacity', 'rate')
local tokens, last = tonumber(state[1]) or capacity, tonumber(state[2]) or now
if state[3] and (tonumber(state[3]) ~= capacity or tonumber(state[4]) ~= rate) then return {-2, 0, 0} end
-- A backwards wall-clock adjustment never creates additional refill.
now = math.max(now, last)
tokens = math.min(capacity, tokens + (now-last)*rate)
local existing = redis.call('HMGET', KEYS[2], 'amount', 'state')
if existing[1] then
 if not state[1] then return {-2, 0, 0} end
 if tonumber(existing[1]) ~= amount or existing[2] ~= 'pending' then return {-2, 0, 0} end
 return {1, math.floor(tokens/1000), 0}
end
if amount > capacity then return {0, math.floor(tokens/1000), 0} end
if tokens < amount then return {0, math.floor(tokens/1000), math.ceil((amount-tokens)/rate)} end
tokens = tokens-amount
-- Debit first. If a later write fails, the caller fails closed and keeps the
-- conservative debit; no upstream attempt is admitted on an ambiguous error.
redis.call('HSET', KEYS[1], 'tokens', tokens, 'time', now, 'capacity', capacity, 'rate', rate)
redis.call('PEXPIRE', KEYS[1], math.max(ttl, math.ceil((capacity-tokens)/rate)+ttl))
redis.call('HSET', KEYS[2], 'amount', amount, 'state', 'pending')
redis.call('PEXPIRE', KEYS[2], ttl)
return {1, math.floor(tokens/1000), 0}
