local actual, capacity, rate, ttl = tonumber(ARGV[1]), tonumber(ARGV[2])*1000, tonumber(ARGV[3]), tonumber(ARGV[4])
local record = redis.call('HMGET', KEYS[2], 'amount', 'state')
if not record[1] or record[2] ~= 'pending' then return 0 end
local state = redis.call('HMGET', KEYS[1], 'tokens', 'time', 'capacity', 'rate')
if not state[1] or tonumber(state[3]) ~= capacity or tonumber(state[4]) ~= rate then return redis.error_reply('bucket missing or policy changed') end
local clock = redis.call('TIME')
local now = tonumber(clock[1])*1000 + math.floor(tonumber(clock[2])/1000)
local last = tonumber(state[2])
now = math.max(now, last)
local tokens = math.min(capacity, tonumber(state[1])+(now-last)*rate)
if actual >= 0 then tokens = math.min(capacity, tokens+tonumber(record[1])-actual*1000) end
-- Mark settled before crediting. Partial Redis command failures can lose a
-- refund but must never permit duplicate refunds. Keep the original TTL.
redis.call('HSET', KEYS[2], 'state', 'settled')
redis.call('HSET', KEYS[1], 'tokens', tokens, 'time', now)
redis.call('PEXPIRE', KEYS[1], math.max(ttl, math.ceil((capacity-tokens)/rate)+ttl))
return 1
