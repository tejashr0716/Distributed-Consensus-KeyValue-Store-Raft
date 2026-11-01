-- ChandyStore Distributed Consensus Raft Schema

CREATE TABLE IF NOT EXISTS raft_nodes (
    node_id VARCHAR(32) PRIMARY KEY,
    address VARCHAR(64) NOT NULL,
    role VARCHAR(16) NOT NULL, -- LEADER, FOLLOWER, CANDIDATE
    current_term BIGINT DEFAULT 1,
    voted_for VARCHAR(32),
    last_heartbeat TIMESTAMPTZ DEFAULT NOW(),
    is_healthy BOOLEAN DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS raft_logs (
    log_index BIGSERIAL PRIMARY KEY,
    term BIGINT NOT NULL,
    command_type VARCHAR(32) NOT NULL, -- SET, DELETE, LOCK_ACQUIRE, LOCK_RELEASE
    key VARCHAR(128),
    value TEXT,
    client_id VARCHAR(64),
    committed_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS kv_store (
    key VARCHAR(128) PRIMARY KEY,
    value TEXT NOT NULL,
    version BIGINT DEFAULT 1,
    modified_by_log_index BIGINT REFERENCES raft_logs(log_index) ON DELETE SET NULL,
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS distributed_locks (
    lock_name VARCHAR(128) PRIMARY KEY,
    holder_client_id VARCHAR(64) NOT NULL,
    fencing_token BIGINT NOT NULL,
    lease_expires_at TIMESTAMPTZ NOT NULL,
    status VARCHAR(32) DEFAULT 'HELD', -- HELD, RELEASED, EXPIRED
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_raft_logs_term ON raft_logs(term);
CREATE INDEX IF NOT EXISTS idx_locks_holder ON distributed_locks(holder_client_id);
CREATE INDEX IF NOT EXISTS idx_locks_expires ON distributed_locks(lease_expires_at);

-- Seed 3-Node Raft Cluster
INSERT INTO raft_nodes (node_id, address, role, current_term, voted_for, is_healthy) VALUES
('raft-node-1', '10.0.0.1:9001', 'LEADER', 1, 'raft-node-1', TRUE),
('raft-node-2', '10.0.0.2:9002', 'FOLLOWER', 1, 'raft-node-1', TRUE),
('raft-node-3', '10.0.0.3:9003', 'FOLLOWER', 1, 'raft-node-1', TRUE) ON CONFLICT DO NOTHING;

-- Initial Replicated Genesis Logs
INSERT INTO raft_logs (log_index, term, command_type, key, value, client_id) VALUES
(1, 1, 'SET', 'cluster:name', 'production-mesh-alpha', 'SYSTEM'),
(2, 1, 'SET', 'cluster:quorum_size', '2', 'SYSTEM'),
(3, 1, 'SET', 'cluster:status', 'SYNCHRONIZED', 'SYSTEM') ON CONFLICT DO NOTHING;

INSERT INTO kv_store (key, value, version, modified_by_log_index) VALUES
('cluster:name', 'production-mesh-alpha', 1, 1),
('cluster:quorum_size', '2', 1, 2),
('cluster:status', 'SYNCHRONIZED', 1, 3) ON CONFLICT DO NOTHING;

SELECT setval('raft_logs_log_index_seq', GREATEST((SELECT last_value FROM raft_logs_log_index_seq), COALESCE((SELECT MAX(log_index) FROM raft_logs),1)), true);
BEGIN;
-- Additive migration. Allocators use the same global guard while allocating tokens.
SELECT pg_advisory_xact_lock(7190381);
ALTER TABLE kv_store ADD COLUMN IF NOT EXISTS last_fencing_token BIGINT NOT NULL DEFAULT 0;
CREATE SEQUENCE IF NOT EXISTS distributed_fencing_token_seq START WITH 1000 CACHE 1;
SELECT setval('distributed_fencing_token_seq', GREATEST(
    (SELECT last_value FROM distributed_fencing_token_seq),
    COALESCE((SELECT MAX(fencing_token) FROM distributed_locks), 0),
    COALESCE((SELECT MAX(last_fencing_token) FROM kv_store), 0), 1000), true);
COMMIT;