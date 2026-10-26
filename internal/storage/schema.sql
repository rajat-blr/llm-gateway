CREATE TABLE IF NOT EXISTS gateway_keys (
 id TEXT PRIMARY KEY,
 secret_hash BLOB NOT NULL UNIQUE CHECK(length(secret_hash)=32),
 name TEXT NOT NULL,
 prefix TEXT NOT NULL,
 aliases TEXT NOT NULL,
 capacity INTEGER NOT NULL CHECK(capacity BETWEEN 1 AND 1000000000),
 refill INTEGER NOT NULL CHECK(refill BETWEEN 1 AND 1000000),
 max_output_tokens INTEGER NOT NULL CHECK(max_output_tokens BETWEEN 1 AND 32768),
 allow_reuse INTEGER NOT NULL CHECK(allow_reuse IN (0,1)),
 created_ms INTEGER NOT NULL,
 revoked_ms INTEGER
);
CREATE TABLE IF NOT EXISTS requests (
 id TEXT PRIMARY KEY,
 key_id TEXT NOT NULL,
 alias TEXT NOT NULL,
 provider TEXT NOT NULL,
 started_ms INTEGER NOT NULL,
 duration_ms INTEGER NOT NULL,
 outcome TEXT NOT NULL,
 kind TEXT NOT NULL DEFAULT 'client' CHECK(kind IN ('client','execution')),
 execution_id TEXT NOT NULL DEFAULT '',
 cache_state TEXT NOT NULL DEFAULT 'BYPASS',
 config_revision TEXT NOT NULL DEFAULT '',
 client_input INTEGER,
 client_output INTEGER
);
CREATE TABLE IF NOT EXISTS attempts (
 request_id TEXT NOT NULL REFERENCES requests(id),
 number INTEGER NOT NULL CHECK(number BETWEEN 1 AND 2),
 provider TEXT NOT NULL,
 started_ms INTEGER NOT NULL,
 duration_ms INTEGER NOT NULL,
 outcome TEXT NOT NULL,
 reserved_tokens INTEGER NOT NULL,
 input_tokens INTEGER,
 output_tokens INTEGER,
 accounting_error INTEGER NOT NULL,
 PRIMARY KEY(request_id, number)
);
CREATE INDEX IF NOT EXISTS requests_time ON requests(started_ms);
CREATE INDEX IF NOT EXISTS requests_key_time ON requests(key_id, started_ms);
CREATE INDEX IF NOT EXISTS attempts_time ON attempts(started_ms);
