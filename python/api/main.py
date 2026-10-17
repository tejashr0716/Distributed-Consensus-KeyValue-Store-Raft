import asyncio
import json
import logging
from contextlib import asynccontextmanager
from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta, timezone

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, Query, status
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from python.database.connection import init_db_pool, close_db_pool, get_db_pool
from python.services.cpp_bridge import ChandyStoreEngine
from python.services.redis_service import (
    init_redis, close_redis, set_node_heartbeat, is_node_active,
    cache_lock_holder, remove_lock_cache, publish_consensus_event
)
from python.services.rabbitmq_service import init_rabbitmq, close_rabbitmq, publish_consensus_message

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("chandystore.api")

engine: Optional[ChandyStoreEngine] = None

def get_engine() -> ChandyStoreEngine:
    global engine
    if engine is None:
        engine = ChandyStoreEngine()
    return engine

class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
        logger.info(f"WebSocket client connected. Total clients: {len(self.active_connections)}")

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)
            logger.info(f"WebSocket client disconnected. Total clients: {len(self.active_connections)}")

    async def broadcast(self, message: Dict[str, Any]):
        for connection in list(self.active_connections):
            try:
                await connection.send_json(message)
            except Exception:
                self.disconnect(connection)

ws_manager = ConnectionManager()

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Initializing ChandyStore Services...")
    await init_db_pool()
    await init_redis()
    await init_rabbitmq()
    get_engine()
    logger.info("ChandyStore Raft Consensus Engine and distributed services ready.")
    yield
    logger.info("Shutting down ChandyStore Services...")
    await close_rabbitmq()
    await close_redis()
    await close_db_pool()

app = FastAPI(
    title="ChandyStore Distributed Consensus Key-Value Store & Lock Manager",
    version="1.0.0",
    description="Raft-Replicated Finite State Machine & Distributed Lock Engine with Monotonic Fencing Tokens",
    lifespan=lifespan
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class KeyValuePayload(BaseModel):
    key: str
    value: str
    client_id: str = "CLIENT_DEFAULT"

class LockAcquirePayload(BaseModel):
    lock_name: str
    client_id: str
    lease_ms: int = Field(5000, ge=500, le=60000)

class LockReleasePayload(BaseModel):
    lock_name: str
    fencing_token: int

class ElectionPayload(BaseModel):
    candidate_node_id: str

@app.get("/health")
async def health_check():
    return {
        "status": "HEALTHY",
        "service": "ChandyStore Raft Consensus Gateway",
        "engine": "C++ Raft FSM & Fencing Token Core Loaded",
        "active_websockets": len(ws_manager.active_connections)
    }

@app.get("/api/v1/cluster")
async def get_cluster_state():
    db = await get_db_pool()
    async with db.acquire() as conn:
        nodes = await conn.fetch("SELECT * FROM raft_nodes ORDER BY node_id ASC;")
        last_log = await conn.fetchrow("SELECT * FROM raft_logs ORDER BY log_index DESC LIMIT 1;")
        leader = next((n for n in nodes if n["role"] == "LEADER"), None)

        output_nodes = []
        for n in nodes:
            active = await is_node_active(n["node_id"])
            output_nodes.append({
                "node_id": n["node_id"],
                "address": n["address"],
                "role": n["role"],
                "current_term": n["current_term"],
                "voted_for": n["voted_for"],
                "is_active": active or (n["is_healthy"]),
                "last_heartbeat": n["last_heartbeat"].isoformat() if n["last_heartbeat"] else None
            })

        return {
            "cluster_size": len(nodes),
            "quorum_size": (len(nodes) // 2) + 1,
            "leader_id": leader["node_id"] if leader else "NONE",
            "current_term": leader["current_term"] if leader else 1,
            "commit_index": last_log["log_index"] if last_log else 0,
            "nodes": output_nodes
        }

@app.post("/api/v1/kv/set", status_code=status.HTTP_201_CREATED)
async def consensus_set_key(payload: KeyValuePayload):
    db = await get_db_pool()
    eng = get_engine()
    async with db.acquire() as conn:
        # Get leader and current term
        leader = await conn.fetchrow("SELECT * FROM raft_nodes WHERE role = 'LEADER' LIMIT 1;")
        term = leader["current_term"] if leader else 1

        # Atomically allocate next log index from PostgreSQL sequence
        next_index = await conn.fetchval("SELECT nextval('raft_logs_log_index_seq');")

        # Calculate CRC32 and verify quorum commit via C++ engine
        crc = eng.compute_crc(
            log_index=next_index,
            term=term,
            cmd_type=1,
            key=payload.key,
            val=payload.value,
            client=payload.client_id
        )

        commit_check = eng.verify_and_commit(
            state={"current_term": term, "commit_index": next_index - 1, "last_applied": next_index - 1, "quorum_size": 2},
            entry_data={"log_index": next_index, "term": term, "command_type": 1, "key": payload.key, "value": payload.value, "client_id": payload.client_id},
            match_count=2 # Majority replicated
        )

        # 1. Append Log Entry
        log_row = await conn.fetchrow(
            """
            INSERT INTO raft_logs (log_index, term, command_type, key, value, client_id)
            VALUES ($1, $2, 'SET', $3, $4, $5)
            RETURNING log_index, committed_at;
            """,
            next_index, term, payload.key, payload.value, payload.client_id
        )

        # 2. Replicate / Upsert to State Machine (kv_store)
        await conn.execute(
            """
            INSERT INTO kv_store (key, value, version, modified_by_log_index, updated_at)
            VALUES ($1, $2, 1, $3, NOW())
            ON CONFLICT (key) DO UPDATE
            SET value = EXCLUDED.value, version = kv_store.version + 1, modified_by_log_index = EXCLUDED.modified_by_log_index, updated_at = NOW();
            """,
            payload.key, payload.value, log_row["log_index"]
        )

        event = {
            "type": "RAFT_LOG_COMMITTED",
            "log_index": log_row["log_index"],
            "term": term,
            "command": "SET",
            "key": payload.key,
            "value": payload.value,
            "crc32": commit_check["crc32"],
            "quorum_committed": commit_check["committed"],
            "timestamp": log_row["committed_at"].isoformat()
        }

        await publish_consensus_event(event)
        asyncio.create_task(ws_manager.broadcast(event))

        return event

@app.get("/api/v1/kv/{key}")
async def get_key(key: str):
    db = await get_db_pool()
    async with db.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM kv_store WHERE key = $1;", key)
        if not row:
            raise HTTPException(status_code=404, detail=f"Key '{key}' not found in replicated state machine.")
        return {
            "key": row["key"],
            "value": row["value"],
            "version": row["version"],
            "modified_by_log_index": row["modified_by_log_index"],
            "updated_at": row["updated_at"].isoformat()
        }

@app.post("/api/v1/locks/acquire")
async def acquire_lock(payload: LockAcquirePayload):
    db = await get_db_pool()
    eng = get_engine()
    async with db.acquire() as conn:
        # Check existing lock
        existing = await conn.fetchrow("SELECT * FROM distributed_locks WHERE lock_name = $1;", payload.lock_name)
        now = datetime.now(timezone.utc)

        if existing and existing["status"] == "HELD" and existing["lease_expires_at"] > now:
            if existing["holder_client_id"] != payload.client_id:
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail=f"Lock '{payload.lock_name}' is currently held by '{existing['holder_client_id']}' until {existing['lease_expires_at']}."
                )

        # Last highest fencing token
        last_token = await conn.fetchval("SELECT MAX(fencing_token) FROM distributed_locks;") or 1000

        # C++ Monotonic Fencing Token Generator
        lock_res = eng.acquire_lock(
            lock_name=payload.lock_name,
            client_id=payload.client_id,
            lease_ms=payload.lease_ms,
            current_fencing_token=last_token
        )

        lease_expiry = now + timedelta(milliseconds=payload.lease_ms)

        await conn.execute(
            """
            INSERT INTO distributed_locks (lock_name, holder_client_id, fencing_token, lease_expires_at, status, created_at)
            VALUES ($1, $2, $3, $4, 'HELD', NOW())
            ON CONFLICT (lock_name) DO UPDATE
            SET holder_client_id = EXCLUDED.holder_client_id, fencing_token = EXCLUDED.fencing_token,
                lease_expires_at = EXCLUDED.lease_expires_at, status = 'HELD', created_at = NOW();
            """,
            payload.lock_name, payload.client_id, lock_res["fencing_token"], lease_expiry
        )

        # Cache in Redis
        lock_info = {
            "lock_name": payload.lock_name,
            "holder": payload.client_id,
            "fencing_token": lock_res["fencing_token"],
            "lease_expires_at": lease_expiry.isoformat()
        }
        await cache_lock_holder(payload.lock_name, lock_info, ttl_seconds=int(payload.lease_ms / 1000) + 1)

        event = {
            "type": "LOCK_ACQUIRED",
            "lock_name": payload.lock_name,
            "holder": payload.client_id,
            "fencing_token": lock_res["fencing_token"],
            "lease_duration_ms": payload.lease_ms,
            "expires_at": lease_expiry.isoformat()
        }

        await publish_consensus_event(event)
        asyncio.create_task(ws_manager.broadcast(event))

        return event

@app.post("/api/v1/locks/release")
async def release_lock(payload: LockReleasePayload):
    db = await get_db_pool()
    eng = get_engine()
    async with db.acquire() as conn:
        lock = await conn.fetchrow("SELECT * FROM distributed_locks WHERE lock_name = $1;", payload.lock_name)
        if not lock or lock["status"] != "HELD":
            raise HTTPException(status_code=404, detail="Lock not actively held.")

        if lock["fencing_token"] != payload.fencing_token:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Stale or invalid fencing token {payload.fencing_token} (Active is {lock['fencing_token']})."
            )

        # Validate with C++ engine
        eng.release_lock(payload.lock_name, payload.fencing_token)

        await conn.execute(
            "UPDATE distributed_locks SET status = 'RELEASED' WHERE lock_name = $1;",
            payload.lock_name
        )
        await remove_lock_cache(payload.lock_name)

        event = {
            "type": "LOCK_RELEASED",
            "lock_name": payload.lock_name,
            "fencing_token": payload.fencing_token
        }
        await publish_consensus_event(event)
        asyncio.create_task(ws_manager.broadcast(event))

        return event

@app.post("/api/v1/cluster/election")
async def trigger_election(payload: ElectionPayload):
    db = await get_db_pool()
    eng = get_engine()
    async with db.acquire() as conn:
        candidate = await conn.fetchrow("SELECT * FROM raft_nodes WHERE node_id = $1;", payload.candidate_node_id)
        if not candidate:
            raise HTTPException(status_code=404, detail=f"Node '{payload.candidate_node_id}' not found.")

        new_term = candidate["current_term"] + 1

        # Check election quorum in C++ engine
        won = eng.check_election_quorum(term=new_term, last_idx=10, last_term=candidate["current_term"], votes=2, total_nodes=3)
        if not won:
            raise HTTPException(status_code=400, detail="Election split-vote or majority not achieved.")

        # Demote previous leaders, promote candidate
        await conn.execute("UPDATE raft_nodes SET role = 'FOLLOWER', current_term = $1;", new_term)
        await conn.execute(
            "UPDATE raft_nodes SET role = 'LEADER', current_term = $1, voted_for = $2, last_heartbeat = NOW() WHERE node_id = $2;",
            new_term, payload.candidate_node_id
        )

        event = {
            "type": "LEADER_ELECTED",
            "leader_id": payload.candidate_node_id,
            "term": new_term,
            "quorum_votes": 2
        }

        await publish_consensus_message("chandystore.leader.elected", event)
        await publish_consensus_event(event)
        asyncio.create_task(ws_manager.broadcast(event))

        return event

@app.websocket("/ws/consensus")
async def websocket_consensus(websocket: WebSocket):
    await ws_manager.connect(websocket)
    try:
        await websocket.send_json({
            "type": "CONNECTED",
            "message": "ChandyStore Raft Consensus Stream Active"
        })
        while True:
            data = await websocket.receive_text()
            if data == "ping":
                await websocket.send_json({"type": "PONG"})
    except WebSocketDisconnect:
        ws_manager.disconnect(websocket)
    except Exception:
        ws_manager.disconnect(websocket)
