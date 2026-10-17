import json
import logging
import os
import aio_pika
from typing import Optional, Dict, Any

logger = logging.getLogger(__name__)

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", "5672"))
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "guest")
RABBITMQ_PASSWORD = os.getenv("RABBITMQ_PASSWORD", "guest")

connection: Optional[aio_pika.RobustConnection] = None
channel: Optional[aio_pika.RobustChannel] = None
exchange: Optional[aio_pika.RobustExchange] = None

async def init_rabbitmq():
    global connection, channel, exchange
    try:
        connection = await aio_pika.connect_robust(
            host=RABBITMQ_HOST,
            port=RABBITMQ_PORT,
            login=RABBITMQ_USER,
            password=RABBITMQ_PASSWORD
        )
        channel = await connection.channel()
        exchange = await channel.declare_exchange("chandystore.events", aio_pika.ExchangeType.TOPIC, durable=True)

        locks_dlq = await channel.declare_queue("chandystore.locks.dlq", durable=True)
        consensus_q = await channel.declare_queue(
            "chandystore.consensus.q",
            durable=True,
            arguments={
                "x-dead-letter-exchange": "chandystore.events",
                "x-dead-letter-routing-key": "chandystore.lock.dlq"
            }
        )
        await consensus_q.bind(exchange, "chandystore.#")
        await locks_dlq.bind(exchange, "chandystore.lock.dlq")
        logger.info("RabbitMQ connected and topology declared for ChandyStore.")
    except Exception as e:
        logger.warning(f"RabbitMQ initialization skipped or unavailable: {e}")
        connection, channel, exchange = None, None, None

async def publish_consensus_message(routing_key: str, data: Dict[str, Any]):
    global exchange
    if exchange is not None:
        try:
            message = aio_pika.Message(
                body=json.dumps(data).encode("utf-8"),
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                content_type="application/json"
            )
            await exchange.publish(message, routing_key=routing_key)
        except Exception as e:
            logger.error(f"Failed to publish consensus message to RabbitMQ: {e}")

async def close_rabbitmq():
    global connection
    if connection is not None and not connection.is_closed:
        await connection.close()
        connection = None
