import json
import os
import redis.asyncio as redis
from typing import Optional, Dict, Any

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB = int(os.getenv("REDIS_DB", "5"))

redis_client: Optional[redis.Redis] = None

async def init_redis() -> redis.Redis:
    global redis_client
    if redis_client is not None:
        try:
            await redis_client.aclose()
        except Exception:
            pass
        redis_client = None

    redis_client = redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        db=REDIS_DB,
        decode_responses=True
    )
    return redis_client

async def get_redis() -> redis.Redis:
    global redis_client
    if redis_client is None:
        await init_redis()
    return redis_client

async def close_redis():
    global redis_client
    if redis_client is not None:
        try:
            await redis_client.aclose()
        except Exception:
            pass
        redis_client = None

async def set_node_heartbeat(node_id: str, ttl_seconds: int = 15):
    client = await get_redis()
    key = f"chandystore:node:{node_id}:hb"
    await client.set(key, "ACTIVE", ex=ttl_seconds)

async def is_node_active(node_id: str) -> bool:
    client = await get_redis()
    key = f"chandystore:node:{node_id}:hb"
    return bool(await client.exists(key))

async def cache_lock_holder(lock_name: str, holder_data: Dict[str, Any], ttl_seconds: int):
    client = await get_redis()
    key = f"chandystore:lock:{lock_name}"
    await client.set(key, json.dumps(holder_data), ex=ttl_seconds)

async def remove_lock_cache(lock_name: str):
    client = await get_redis()
    key = f"chandystore:lock:{lock_name}"
    await client.delete(key)

async def publish_consensus_event(event: Dict[str, Any]):
    client = await get_redis()
    await client.publish("chandystore:consensus", json.dumps(event))
