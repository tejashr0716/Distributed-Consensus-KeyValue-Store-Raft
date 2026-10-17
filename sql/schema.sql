-- ChandyStore Distributed Consensus Raft Schema
DROP TABLE IF EXISTS distributed_locks CASCADE;
DROP TABLE IF EXISTS kv_store CASCADE;
DROP TABLE IF EXISTS raft_logs CASCADE;
DROP TABLE IF EXISTS raft_nodes CASCADE;

CREATE TABLE raft_nodes (
    node_id VARCHAR(32) PRIMARY KEY,
    address VARCHAR(64) NOT NULL,
    role VARCHAR(16) NOT NULL, -- LEADER, FOLLOWER, CANDIDATE
    current_term BIGINT DEFAULT 1,
    voted_for VARCHAR(32),
    last_heartbeat TIMESTAMPTZ DEFAULT NOW(),
    is_healthy BOOLEAN DEFAULT TRUE
);

CREATE TABLE raft_logs (
    log_index BIGSERIAL PRIMARY KEY,
    term BIGINT NOT NULL,
    command_type VARCHAR(32) NOT NULL, -- SET, DELETE, LOCK_ACQUIRE, LOCK_RELEASE
    key VARCHAR(128),
    value TEXT,
    client_id VARCHAR(64),
    committed_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE kv_store (
    key VARCHAR(128) PRIMARY KEY,
    value TEXT NOT NULL,
    version BIGINT DEFAULT 1,
    modified_by_log_index BIGINT REFERENCES raft_logs(log_index) ON DELETE SET NULL,
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE distributed_locks (
    lock_name VARCHAR(128) PRIMARY KEY,
    holder_client_id VARCHAR(64) NOT NULL,
    fencing_token BIGINT NOT NULL,
    lease_expires_at TIMESTAMPTZ NOT NULL,
    status VARCHAR(32) DEFAULT 'HELD', -- HELD, RELEASED, EXPIRED
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_raft_logs_term ON raft_logs(term);
CREATE INDEX idx_locks_holder ON distributed_locks(holder_client_id);
CREATE INDEX idx_locks_expires ON distributed_locks(lease_expires_at);

-- Seed 3-Node Raft Cluster
INSERT INTO raft_nodes (node_id, address, role, current_term, voted_for, is_healthy) VALUES
('raft-node-1', '10.0.0.1:9001', 'LEADER', 1, 'raft-node-1', TRUE),
('raft-node-2', '10.0.0.2:9002', 'FOLLOWER', 1, 'raft-node-1', TRUE),
('raft-node-3', '10.0.0.3:9003', 'FOLLOWER', 1, 'raft-node-1', TRUE);

-- Initial Replicated Genesis Logs
INSERT INTO raft_logs (log_index, term, command_type, key, value, client_id) VALUES
(1, 1, 'SET', 'cluster:name', 'production-mesh-alpha', 'SYSTEM'),
(2, 1, 'SET', 'cluster:quorum_size', '2', 'SYSTEM'),
(3, 1, 'SET', 'cluster:status', 'SYNCHRONIZED', 'SYSTEM');

INSERT INTO kv_store (key, value, version, modified_by_log_index) VALUES
('cluster:name', 'production-mesh-alpha', 1, 1),
('cluster:quorum_size', '2', 1, 2),
('cluster:status', 'SYNCHRONIZED', 1, 3);

SELECT setval('raft_logs_log_index_seq', (SELECT MAX(log_index) FROM raft_logs));
