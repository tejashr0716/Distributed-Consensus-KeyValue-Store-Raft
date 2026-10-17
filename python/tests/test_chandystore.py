import json
import os
import pytest
import pytest_asyncio
from httpx import AsyncClient, ASGITransport

from python.api.main import app
from python.services.cpp_bridge import ChandyStoreEngine
from python.database.connection import close_db_pool
from python.services.redis_service import init_redis, set_node_heartbeat, is_node_active, close_redis
from python.spark.spark_consensus_audit import run_consensus_audit

@pytest.fixture(scope="module")
def cpp_engine():
    return ChandyStoreEngine()

@pytest_asyncio.fixture
async def async_client():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        yield client
    await close_db_pool()
    await close_redis()

def test_cpp_crc32_integrity(cpp_engine):
    crc1 = cpp_engine.compute_crc(1, 1, 1, "k1", "v1", "c1")
    crc2 = cpp_engine.compute_crc(1, 1, 1, "k1", "v1", "c1")
    crc3 = cpp_engine.compute_crc(1, 1, 1, "k1", "tampered_value", "c1")

    assert crc1 != 0
    assert crc1 == crc2
    assert crc1 != crc3

def test_cpp_quorum_log_commit(cpp_engine):
    state = {"current_term": 1, "commit_index": 5, "last_applied": 5, "quorum_size": 2}
    entry = {"log_index": 6, "term": 1, "command_type": 1, "key": "k", "value": "v", "client_id": "c"}

    # 1 peer replicated (insufficient quorum for size 3)
    uncommitted = cpp_engine.verify_and_commit(state, entry, match_count=1)
    assert not uncommitted["committed"]

    # 2 peers replicated (quorum achieved)
    committed = cpp_engine.verify_and_commit(state, entry, match_count=2)
    assert committed["committed"]
    assert committed["commit_index"] == 6

def test_cpp_lock_monotonicity(cpp_engine):
    res1 = cpp_engine.acquire_lock("db_migration", "worker-1", 5000, current_fencing_token=1000)
    assert res1["success"]
    assert res1["fencing_token"] == 1001

    res2 = cpp_engine.acquire_lock("db_migration", "worker-2", 5000, current_fencing_token=res1["fencing_token"])
    assert res2["success"]
    assert res2["fencing_token"] == 1002
    assert res2["fencing_token"] > res1["fencing_token"]

def test_cpp_election_quorum(cpp_engine):
    # 2 of 3 votes -> Won
    assert cpp_engine.check_election_quorum(term=2, last_idx=5, last_term=1, votes=2, total_nodes=3)
    # 1 of 3 votes -> Lost
    assert not cpp_engine.check_election_quorum(term=2, last_idx=5, last_term=1, votes=1, total_nodes=3)

@pytest.mark.asyncio
async def test_redis_node_heartbeat():
    await init_redis()
    await set_node_heartbeat("raft-node-test-1", ttl_seconds=10)
    active = await is_node_active("raft-node-test-1")
    assert active is True

    inactive = await is_node_active("raft-node-dead-99")
    assert inactive is False
    await close_redis()

@pytest.mark.asyncio
async def test_api_health(async_client):
    resp = await async_client.get("/health")
    assert resp.status_code == 200
    assert resp.json()["status"] == "HEALTHY"

@pytest.mark.asyncio
async def test_api_cluster_state(async_client):
    resp = await async_client.get("/api/v1/cluster")
    assert resp.status_code == 200
    cluster = resp.json()
    assert cluster["cluster_size"] >= 3
    assert cluster["quorum_size"] == 2
    assert cluster["leader_id"] in ["raft-node-1", "raft-node-2", "raft-node-3"]

@pytest.mark.asyncio
async def test_api_consensus_set_and_get(async_client):
    set_payload = {
        "key": "feature:dark_mode",
        "value": "enabled_globally",
        "client_id": "CLIENT_TEST_SUITE"
    }
    set_resp = await async_client.post("/api/v1/kv/set", json=set_payload)
    assert set_resp.status_code == 201
    set_data = set_resp.json()
    assert set_data["quorum_committed"] is True
    assert set_data["crc32"] != 0

    get_resp = await async_client.get("/api/v1/kv/feature:dark_mode")
    assert get_resp.status_code == 200
    get_data = get_resp.json()
    assert get_data["value"] == "enabled_globally"

@pytest.mark.asyncio
async def test_api_distributed_lock_lifecycle(async_client):
    lock_name = "resource:payment_settlement"
    # Acquire Lock Client 1
    acq_payload = {"lock_name": lock_name, "client_id": "PAYMENT_WORKER_01", "lease_ms": 10000}
    acq_resp = await async_client.post("/api/v1/locks/acquire", json=acq_payload)
    assert acq_resp.status_code == 200
    acq_data = acq_resp.json()
    token = acq_data["fencing_token"]
    assert token > 0

    # Client 2 Conflict
    conflict_payload = {"lock_name": lock_name, "client_id": "PAYMENT_WORKER_02", "lease_ms": 5000}
    conf_resp = await async_client.post("/api/v1/locks/acquire", json=conflict_payload)
    assert conf_resp.status_code == 409

    # Client 1 Release with Fencing Token
    rel_resp = await async_client.post("/api/v1/locks/release", json={"lock_name": lock_name, "fencing_token": token})
    assert rel_resp.status_code == 200

    # Client 2 Re-acquire
    reacq_resp = await async_client.post("/api/v1/locks/acquire", json=conflict_payload)
    assert reacq_resp.status_code == 200
    assert reacq_resp.json()["fencing_token"] > token

@pytest.mark.asyncio
async def test_api_cluster_election(async_client):
    elect_payload = {"candidate_node_id": "raft-node-2"}
    resp = await async_client.post("/api/v1/cluster/election", json=elect_payload)
    assert resp.status_code == 200
    data = resp.json()
    assert data["leader_id"] == "raft-node-2"
    assert data["quorum_votes"] >= 2

    # Verify cluster state reflects new leader
    cluster_resp = await async_client.get("/api/v1/cluster")
    assert cluster_resp.status_code == 200
    assert cluster_resp.json()["leader_id"] == "raft-node-2"

def test_spark_consensus_audit(tmp_path):
    input_file = str(tmp_path / "raft_logs.json")
    output_report = str(tmp_path / "audit_report.json")

    mock_logs = [
        {"log_index": 1, "term": 1, "command_type": "SET", "key": "k1", "value": "v1", "client_id": "C1"},
        {"log_index": 2, "term": 1, "command_type": "SET", "key": "k2", "value": "v2", "client_id": "C1"},
        {"log_index": 3, "term": 2, "command_type": "LOCK_ACQUIRE", "key": "lock1", "value": "held", "client_id": "C2"},
        {"log_index": 4, "term": 2, "command_type": "SET", "key": "k3", "value": "v3", "client_id": "C3"}
    ]

    with open(input_file, "w") as f:
        for entry in mock_logs:
            f.write(json.dumps(entry) + "\n")

    report = run_consensus_audit(input_file, output_report)
    assert report["total_replicated_logs"] == 4
    assert report["is_prefix_consistent"] is True
    assert report["log_consistency_violations"] == 0
    assert os.path.exists(output_report)
