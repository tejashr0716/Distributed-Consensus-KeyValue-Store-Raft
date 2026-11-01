-- Additive migration. Allocators use the same global guard while allocating tokens.
SELECT pg_advisory_xact_lock(7190381);
ALTER TABLE kv_store ADD COLUMN IF NOT EXISTS last_fencing_token BIGINT NOT NULL DEFAULT 0;
CREATE SEQUENCE IF NOT EXISTS distributed_fencing_token_seq START WITH 1000 CACHE 1;
SELECT setval('distributed_fencing_token_seq', GREATEST(
    (SELECT last_value FROM distributed_fencing_token_seq),
    COALESCE((SELECT MAX(fencing_token) FROM distributed_locks), 0),
    COALESCE((SELECT MAX(last_fencing_token) FROM kv_store), 0), 1000), true);
