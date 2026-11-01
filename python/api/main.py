import asyncio
import json
import logging
from contextlib import asynccontextmanager
from pathlib import Path
import os
from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta, timezone

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, Query, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field, field_validator

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
                await asyncio.wait_for(connection.send_json(message), timeout=2)
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
    logger.info("ChandyStore educational coordination service started; inspect /health for dependency availability.")
    yield
    logger.info("Shutting down ChandyStore Services...")
    await close_rabbitmq()
    await close_redis()
    await close_db_pool()

app = FastAPI(
    title="ChandyStore PostgreSQL Coordination Prototype",
    version="1.0.0",
    description="Single PostgreSQL authority; local quorum checks are simulated, not autonomous Raft replication.",
    lifespan=lifespan
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=os.getenv("CORS_ORIGINS", "http://localhost:8000,http://127.0.0.1:8000").split(","),
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

class KeyValuePayload(BaseModel):
    key: str = Field(min_length=1, max_length=63)
    value: str = Field(max_length=127)
    client_id: str = Field(default="CLIENT_DEFAULT", min_length=1, max_length=63)
    fencing_token: Optional[int] = Field(default=None, gt=0)

    @field_validator("key", "value", "client_id")
    @classmethod
    def validate_native_bytes(cls, value, info):
        limit = 127 if info.field_name == "value" else 63
        if "\x00" in value or len(value.encode("utf-8")) > limit:
            raise ValueError("Value exceeds native UTF-8 field size or contains NUL")
        return value

class LockAcquirePayload(BaseModel):
    lock_name: str = Field(min_length=1, max_length=128)
    client_id: str = Field(min_length=1, max_length=63)
    lease_ms: int = Field(5000, ge=500, le=60000)

class LockReleasePayload(BaseModel):
    lock_name: str = Field(min_length=1, max_length=128)
    client_id: str = Field(min_length=1, max_length=63)
    fencing_token: int = Field(gt=0)

class ElectionPayload(BaseModel):
    candidate_node_id: str

@app.get("/health")
async def health_check():
    db = await get_db_pool()
    async with db.acquire() as conn:
        await conn.fetchval("SELECT 1")
    get_engine()
    from python.services import rabbitmq_service
    return {"status": "READY" if rabbitmq_service.exchange is not None else "DEGRADED",
            "database": "reachable", "native_engine": "loaded",
            "broker": "connected" if rabbitmq_service.exchange is not None else "unavailable",
            "scope": "local educational prototype; no replication guarantee"}

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

async def resource_guard(conn, resource):
    # Same guard for acquisition, release and writes, including absent rows.
    await conn.execute("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))", "resource:" + resource)


@app.post("/api/v1/kv/set", status_code=201)
async def consensus_set_key(payload: KeyValuePayload):
    db = await get_db_pool()
    eng = get_engine()
    async with db.acquire() as conn:
        async with conn.transaction():
            await resource_guard(conn, payload.key)
            existing = await conn.fetchrow("SELECT last_fencing_token FROM kv_store WHERE key=$1 FOR UPDATE", payload.key)
            previous = existing["last_fencing_token"] if existing else 0
            # Once a resource has been locked it must never be written unfenced.
            lock = await conn.fetchrow("SELECT * FROM distributed_locks WHERE lock_name=$1 FOR UPDATE", payload.key)
            if payload.fencing_token is None:
                if previous > 0 or lock:
                    raise HTTPException(409, "Fencing token required for protected resource")
                token = 0
            else:
                token = payload.fencing_token
                now = await conn.fetchval("SELECT clock_timestamp()")
                if (not lock or lock["status"] != "HELD" or lock["lease_expires_at"] <= now
                    or lock["holder_client_id"] != payload.client_id
                    or lock["fencing_token"] != token or token < previous):
                    raise HTTPException(409, "Stale, expired, wrong-owner or invalid fencing token")
            # A local simulated quorum rule, NOT independent peer replication.
            term = await conn.fetchval("SELECT COALESCE(MAX(current_term),1) FROM raft_nodes")
            idx = await conn.fetchval("SELECT nextval('raft_logs_log_index_seq')")
            crc = eng.compute_crc(idx, term, 1, payload.key, payload.value, payload.client_id)
            row = await conn.fetchrow("""INSERT INTO raft_logs(log_index,term,command_type,key,value,client_id)
                VALUES($1,$2,'SET',$3,$4,$5) RETURNING committed_at""",idx,term,payload.key,payload.value,payload.client_id)
            await conn.execute("""INSERT INTO kv_store(key,value,version,modified_by_log_index,last_fencing_token)
                VALUES($1,$2,1,$3,$4) ON CONFLICT(key) DO UPDATE SET value=EXCLUDED.value,
                version=kv_store.version+1, modified_by_log_index=EXCLUDED.modified_by_log_index,
                last_fencing_token=EXCLUDED.last_fencing_token, updated_at=clock_timestamp()""",
                payload.key,payload.value,idx,token)
            event = {"type":"KV_WRITE_COMMITTED", "log_index":idx,"key":payload.key,
                     "value":payload.value,"crc32":crc,"fencing_token":token,
                     "quorum_simulated":True,"timestamp":row["committed_at"].isoformat()}
    # Notification failures must not turn an already committed write into an apparent failure.
    try:
        await publish_consensus_event(event)
    except Exception:
        logger.exception("Best-effort notification failed after durable write")
    await ws_manager.broadcast(event)
    return event

@app.get("/api/v1/kv/{key}")
async def get_key(key: str):
    db = await get_db_pool()
    async with db.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM kv_store WHERE key = $1;", key)
        if not row:
            raise HTTPException(status_code=404, detail=f"Key '{key}' not found in PostgreSQL-backed key-value state.")
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
    async with db.acquire() as conn:
        async with conn.transaction():
            await resource_guard(conn, payload.lock_name)
            existing = await conn.fetchrow("SELECT * FROM distributed_locks WHERE lock_name=$1 FOR UPDATE",payload.lock_name)
            now = await conn.fetchval("SELECT clock_timestamp()")
            if existing and existing["status"] == "HELD" and existing["lease_expires_at"] > now:
                # Explicitly no implicit renewal, including for the same client.
                raise HTTPException(409,"Resource already held; no implicit renewals")
            await conn.execute("SELECT pg_advisory_xact_lock(7190381)")
            token = await conn.fetchval("SELECT nextval('distributed_fencing_token_seq')")
            now = await conn.fetchval("SELECT clock_timestamp()")
            expiry = now + timedelta(milliseconds=payload.lease_ms)
            await conn.execute("""INSERT INTO distributed_locks(lock_name,holder_client_id,fencing_token,lease_expires_at,status)
                VALUES($1,$2,$3,$4,'HELD') ON CONFLICT(lock_name) DO UPDATE SET
                holder_client_id=EXCLUDED.holder_client_id,fencing_token=EXCLUDED.fencing_token,
                lease_expires_at=EXCLUDED.lease_expires_at,status='HELD',created_at=clock_timestamp()""",
                payload.lock_name,payload.client_id,token,expiry)
    event = {"type":"LOCK_ACQUIRED","lock_name":payload.lock_name,"holder":payload.client_id,
             "fencing_token":token,"expires_at":expiry.isoformat(),"lease_duration_ms":payload.lease_ms}
    await ws_manager.broadcast(event)
    return event


@app.post("/api/v1/locks/release")
async def release_lock(payload: LockReleasePayload):
    db = await get_db_pool()
    async with db.acquire() as conn:
        async with conn.transaction():
            await resource_guard(conn,payload.lock_name)
            lock = await conn.fetchrow("SELECT * FROM distributed_locks WHERE lock_name=$1 FOR UPDATE",payload.lock_name)
            now = await conn.fetchval("SELECT clock_timestamp()")
            if (not lock or lock["status"] != "HELD" or lock["lease_expires_at"] <= now
                or lock["fencing_token"] != payload.fencing_token
                or lock["holder_client_id"] != payload.client_id):
                raise HTTPException(409,"Stale, expired or wrong-owner release")
            await conn.execute("UPDATE distributed_locks SET status='RELEASED' WHERE lock_name=$1",payload.lock_name)
    event={"type":"LOCK_RELEASED","lock_name":payload.lock_name,"fencing_token":payload.fencing_token}
    await ws_manager.broadcast(event)
    return event

@app.post("/api/v1/cluster/election")
async def trigger_election(payload: ElectionPayload):
    db = await get_db_pool()
    eng = get_engine()
    async with db.acquire() as conn, conn.transaction():
        await conn.execute("SELECT pg_advisory_xact_lock(7190382)")
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

@app.get("/demo", include_in_schema=False)
async def backend_demo():
    return FileResponse(Path(__file__).resolve().parents[2] / "web" / "demo.html")
